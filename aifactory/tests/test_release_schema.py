"""Additive release schema upgrades and database-enforced idempotency."""
import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from aifactory import db
from aifactory.models import Release, ReleaseStep, ReleaseTrainCursor, Repository


def test_upgrade_existing_database_preserves_data(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/legacy", config={"release": {"health_url": "https://app.test"}})
        s.add(repo)
        s.flush()
        rid = repo.id
    engine = db.engine()
    url = engine.url.render_as_string(hide_password=False)
    # Simulate the pre-release schema, leaving all existing product tables/data intact.
    ReleaseTrainCursor.__table__.drop(engine)
    ReleaseStep.__table__.drop(engine)
    Release.__table__.drop(engine)
    for _ in range(2):
        engine = db.init_engine(url)
        assert {"releases", "release_steps", "release_train_cursors"} <= set(inspect(engine).get_table_names())
        with db.session_scope() as s:
            assert s.get(Repository, rid).config == {"release": {"health_url": "https://app.test"}}
    with db.session_scope() as s:
        s.add(ReleaseTrainCursor(workspace_id=ws, repo_id=rid, baseline_sha="a" * 40))
    with db.session_scope() as s:
        assert s.get(ReleaseTrainCursor, rid).baseline_sha == "a" * 40


@pytest.mark.parametrize("duplicate", ["slot", "attempt", "operation"])
def test_release_uniqueness_is_database_enforced(signed_in, duplicate):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/train")
        s.add(repo)
        s.flush()
        rid = repo.id
        release = Release(workspace_id=ws, repo_id=rid, scheduled_slot="2026-11-11T09:00[UTC]", scheduled_at=db.utcnow())
        s.add(release)
        s.flush()
        release_id = release.id
        s.add(ReleaseStep(workspace_id=ws, repo_id=rid, release_id=release_id, step="staging", attempt=1,
                          operation_key="release-staging-1"))
    with pytest.raises(IntegrityError):
        with db.session_scope() as s:
            if duplicate == "slot":
                s.add(Release(workspace_id=ws, repo_id=rid, scheduled_slot="2026-11-11T09:00[UTC]", scheduled_at=db.utcnow()))
            else:
                s.add(ReleaseStep(workspace_id=ws, repo_id=rid, release_id=release_id, step="staging",
                                  attempt=1 if duplicate == "attempt" else 2,
                                  operation_key="different" if duplicate == "attempt" else "release-staging-1"))
    with db.session_scope() as s:
        assert s.query(Release).count() == 1
        assert s.query(ReleaseStep).count() == 1
