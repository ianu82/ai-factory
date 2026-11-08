import json
import os
import shutil
import subprocess

import pytest

from auto_mindsdb_factory.docker_runner import DockerSandbox


def section(path, *, start=1, lines=100, match=None):
    return {"path": path, "start_line": start, "line_count": lines, "match": match}


def read(root, sections):
    from auto_mindsdb_factory.planning_source_reader import source_read_command
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the source-reader contract")
    command = source_read_command({"sections": sections})
    assert command[0] == "node"
    result = subprocess.run([node, *command[1:]], cwd=root, env={"PATH": os.environ["PATH"]},
                            capture_output=True, timeout=5)
    assert result.returncode == 0 and result.stderr == b"", result.stderr
    assert len(result.stdout) < DockerSandbox.MAX_CAPTURED_OUTPUT_BYTES
    return json.loads(result.stdout)


def test_oversized_section_does_not_discard_other_source_evidence(tmp_path):
    (tmp_path / "large.txt").write_text("x" * 13000)
    (tmp_path / "small.txt").write_text("Useful source evidence\n")
    result = read(tmp_path, [section("large.txt"), section("small.txt")])["sections"]
    assert result[0] == {"index": 0, "status": "omitted_budget"}
    assert result[1]["status"] == "ok" and result[1]["text"] == "Useful source evidence"
    assert result[1]["total_lines"] == 1 and result[1]["start_line"] == 1


def test_literal_match_and_line_windows_report_the_actual_selection(tmp_path):
    (tmp_path / "schema.txt").write_text("preface\nTABLE entries\ndate DATE\nend\nother\n")
    result = read(tmp_path, [section("schema.txt", start=2, lines=2, match="TABLE entries"),
                             section("schema.txt", lines=1, match="absent")])["sections"]
    assert result[0] == {"index": 0, "status": "ok", "start_line": 2, "total_lines": 5,
                         "text": "TABLE entries\ndate DATE", "line_count": 2}
    assert result[1] == {"index": 1, "status": "no_match"}


def test_literal_miss_remains_a_miss_with_labelled_nearby_source(tmp_path):
    (tmp_path / "schema.txt").write_text("preamble\nCREATE TABLE IF NOT EXISTS entries (\ndate DATE\n);\n")
    result = read(tmp_path, [section("schema.txt", lines=3, match="CREATE TABLE entries")])["sections"][0]
    assert result == {"index": 0, "status": "no_match", "nearby": {
        "match_kind": "ordered_tokens", "start_line": 2, "total_lines": 4, "line_count": 3,
        "text": "CREATE TABLE IF NOT EXISTS entries (\ndate DATE\n);"}}


def test_nearby_source_obeys_start_line_token_order_case_and_repetition(tmp_path):
    (tmp_path / "functions.txt").write_text("async function lookup() {}\nlookup function\nFunction lookup\nfunction once\n")
    selections = [section("functions.txt", start=2, match="function lookup"),
                  section("functions.txt", match="function function"),
                  section("functions.txt", match="absent token")]
    assert read(tmp_path, selections)["sections"] == [
        {"index": i, "status": "no_match"} for i in range(3)]
    result = read(tmp_path, [section("functions.txt", lines=1, match="async lookup")])["sections"][0]
    assert result["status"] == "no_match" and result["nearby"]["text"] == "async function lookup() {}"


def test_oversized_nearby_context_preserves_the_miss_and_other_sections(tmp_path):
    (tmp_path / "large.txt").write_text("async function lookup() {" + "x" * 13000 + "}\n")
    (tmp_path / "small.txt").write_text("Other useful evidence\n")
    result = read(tmp_path, [section("large.txt", match="async lookup"), section("small.txt")])["sections"]
    assert result[0] == {"index": 0, "status": "no_match", "nearby_omitted_budget": True}
    assert result[1]["status"] == "ok" and result[1]["text"] == "Other useful evidence"


def test_unicode_and_json_escaping_fit_the_real_utf8_capture_limit(tmp_path):
    text = '\u6f22\U0001f680"\\\t' * 400
    (tmp_path / "unicode.txt").write_text(text)
    result = read(tmp_path, [section("unicode.txt") for _ in range(16)])["sections"]
    assert result[0]["text"] == text
    assert any(item["status"] == "omitted_budget" for item in result)
    assert len(result) == 16


def test_reader_cannot_follow_symlinks_outside_its_workspace(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("synthetic-private-marker")
    (workspace / "link").symlink_to(outside)
    (workspace / "parent").symlink_to(tmp_path, target_is_directory=True)
    result = read(workspace, [section("link"), section("parent/outside.txt")])
    assert [item["status"] for item in result["sections"]] == ["outside_workspace"] * 2
    assert "synthetic-private-marker" not in json.dumps(result)


def test_missing_binary_large_empty_and_out_of_range_are_explicit(tmp_path):
    (tmp_path / "binary").write_bytes(b"\xff\x00")
    (tmp_path / "large").write_bytes(b"a" * (1024 * 1024 + 1))
    (tmp_path / "empty").write_bytes(b"")
    result = read(tmp_path, [section("missing"), section("binary"), section("large"),
                             section("empty"), section("empty", start=2)])["sections"]
    assert [item["status"] for item in result] == [
        "unreadable", "invalid_utf8", "file_too_large", "ok", "out_of_range"]
    assert result[3]["text"] == "" and result[3]["total_lines"] == 0


def test_special_files_do_not_block_and_literal_paths_do_not_execute(tmp_path):
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO contract requires POSIX")
    os.mkfifo(tmp_path / "pipe")
    filename = "literal; $(touch injected).txt"
    source = tmp_path / filename
    source.write_text("source stays unchanged")
    source.chmod(0o444)
    before = source.read_bytes()
    result = read(tmp_path, [section("pipe"), section(filename)])["sections"]
    assert result[0]["status"] == "not_file" and result[1]["text"] == before.decode()
    assert source.read_bytes() == before and not (tmp_path / "injected").exists()


def test_maximum_unicode_request_fits_one_argument_without_losing_identity():
    from auto_mindsdb_factory.planning_source_reader import source_read_command
    request = {"sections": [section("\U0001f680" * 512, match="\U0001f680" * 256) for _ in range(16)]}
    payload = source_read_command(request)[-1]
    assert len(payload.encode()) < 65536
    assert json.loads(payload)["sections"] == request["sections"]
    with pytest.raises(ValueError, match="encoding"):
        source_read_command({"sections": [section("\ud800")]})


@pytest.mark.parametrize("mutation", ["absolute", "traversal", "backslash", "nul", "extra",
                                     "zero", "too_many_lines", "too_many_sections", "empty_match"])
def test_invalid_requests_fail_before_a_job_command_is_constructed(mutation):
    from auto_mindsdb_factory.planning_source_reader import source_read_command
    request = {"sections": [section("file.txt")]}
    item = request["sections"][0]
    if mutation in {"absolute", "traversal", "backslash", "nul"}:
        item["path"] = {"absolute": "/etc/passwd", "traversal": "a/../file.txt",
                        "backslash": "a\\b", "nul": "a\x00b"}[mutation]
    elif mutation == "extra":
        request["limit"] = 999999
    elif mutation == "zero":
        item["start_line"] = 0
    elif mutation == "too_many_lines":
        item["line_count"] = 501
    elif mutation == "too_many_sections":
        request["sections"] *= 17
    else:
        item["match"] = ""
    with pytest.raises(ValueError):
        source_read_command(request)
