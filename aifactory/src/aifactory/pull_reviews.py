"""Review record creation. Callers own the transaction; no planning or delivery events."""

from sqlalchemy import update
from sqlalchemy.orm import Session

from .models import Outcome, PullReview, Repository, ReviewAttempt, Workspace


def get_or_create_review(db: Session, repo: Repository, pr_number: int) -> PullReview:
    # A write lock works on both SQLite and Postgres. Lookup after locking prevents
    # concurrent intake from allocating a second outcome (including its budget).
    db.execute(update(Workspace).where(Workspace.id == repo.workspace_id).values(
        outcome_seq=Workspace.outcome_seq))
    review = db.query(PullReview).filter_by(repo_id=repo.id, pr_number=pr_number).one_or_none()
    if review is not None:
        return review
    number = db.execute(update(Workspace).where(Workspace.id == repo.workspace_id).values(
        outcome_seq=Workspace.outcome_seq + 1).returning(Workspace.outcome_seq)).scalar_one()
    outcome = Outcome(workspace_id=repo.workspace_id, number=number, source="review", status="running",
                      title=f"Review {repo.full_name}#{pr_number}", repo_ids=[repo.id])
    db.add(outcome)
    db.flush()
    review = PullReview(workspace_id=repo.workspace_id, repo_id=repo.id, pr_number=pr_number, outcome_id=outcome.id)
    db.add(review)
    db.flush()
    return review


def get_or_create_attempt(db: Session, review: PullReview, head_sha: str, base_sha: str) -> ReviewAttempt:
    db.execute(update(Workspace).where(Workspace.id == review.workspace_id).values(
        outcome_seq=Workspace.outcome_seq))
    attempt = db.query(ReviewAttempt).filter_by(review_id=review.id, head_sha=head_sha).one_or_none()
    if attempt is None:
        attempt = ReviewAttempt(review_id=review.id, head_sha=head_sha, base_sha=base_sha)
        db.add(attempt)
        db.flush()
    return attempt
