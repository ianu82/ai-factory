import json
import os

from auto_mindsdb_factory.__main__ import _load_local_env_files, main
from auto_mindsdb_factory.run_journal import RunJournal


def test_product_commands_do_not_load_target_env_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("OPENAI_API_KEY=candidate-controlled\n")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert _load_local_env_files(["product-deliver"]) == []
    assert "OPENAI_API_KEY" not in os.environ


def test_inspect_reports_terminal_state_and_retains_events(tmp_path, capsys):
    journal = RunJournal(tmp_path, "task")
    journal.create({})
    journal.append("run_finished", status="blocked")
    assert main(["product-inspect", "--store-dir", str(tmp_path), "--run-id", "task", "--events"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "blocked"
    assert output["timeline"][-1]["kind"] == "run_finished"
