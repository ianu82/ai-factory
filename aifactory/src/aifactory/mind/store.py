"""Private persistence and database-backed per-person paid-request admission.

No process-local lock is authoritative. A conditional account UPDATE is the
serialization point on SQLite and PostgreSQL. Pending charges outlive leases
and processes and fail closed until the provider supplies a cumulative total.
"""
from __future__ import annotations

import asyncio
import math
from datetime import timedelta
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import or_, update
from sqlalchemy.exc import IntegrityError

from ..db import SessionLocal, utcnow
from ..models import Membership, MindAccount, MindCharge, MindMessage, MindThread, Workspace

LEASE_SECONDS = 180
MAX_CALLS = 8


class AccountingError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__({
            "budget_exceeded": "Mind's daily budget is exhausted. Try again after UTC midnight.",
            "accounting_pending": "Mind usage is unresolved. Further paid calls are blocked until accounting reconciles.",
            "accounting_busy": "Another Mind request is being accounted for. Try again shortly.",
            "invalid_budget": "Mind's daily budget must be finite and nonnegative.",
            "turn_limit": "Mind reached the maximum number of calls for this turn.",
        }[code])


def memberships(db, user_id):
    return sorted(r[0] for r in db.query(Membership.workspace_id).filter_by(user_id=user_id).all())


def owned(db, user_id, thread_id):
    thread = db.query(MindThread).filter_by(id=thread_id, owner_id=user_id).populate_existing().one_or_none()
    if thread is None or not set(thread.workspace_ids).issubset(memberships(db, user_id)):
        raise HTTPException(404, "Thread not found")
    return thread


def effective_limit(db, user_id):
    limits = []
    for ws in db.query(Workspace).join(Membership).filter(Membership.user_id == user_id):
        settings = ws.settings.get("mind") or {}
        if not isinstance(settings, dict):
            raise AccountingError("invalid_budget")
        value = settings.get("daily_budget_usd", 10)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise AccountingError("invalid_budget")
        limits.append(float(value))
    return min(limits, default=10.0)


def cost(rows):
    rows = list(rows)
    known = all(r.settled and r.usd is not None for r in rows)
    return {"usd": sum(r.usd or 0 for r in rows) if known else None, "known": known}


def usage(db, user_id, thread_id=None):
    today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    rows = db.query(MindCharge).filter_by(user_id=user_id)
    return {"thread_cost": cost(rows.filter_by(thread_id=thread_id)) if thread_id else {"usd": 0, "known": True},
            "daily_spend": cost(rows.filter(MindCharge.created_at >= today)),
            "daily_limit": effective_limit(db, user_id)}


def wire_usage(db, user_id, thread_id=None):
    """The usage event as the dock reads it (web/src/api.ts MindWireUsage): daily spend is a number, or null while
    any of today's charges is unknown."""
    current = usage(db, user_id, thread_id)
    spend = current["daily_spend"]
    return {**current, "daily_spend": spend["usd"] if spend["known"] else None}


def recover(db, thread):
    if thread.active_turn and thread.lease_until and thread.lease_until <= utcnow():
        old = thread.active_turn
        changed = db.execute(update(MindThread).where(MindThread.id == thread.id,
            MindThread.active_turn == old, MindThread.lease_until <= utcnow()).values(
                active_turn=None, lease_until=None, status="interrupted")).rowcount
        if changed:
            db.query(MindMessage).filter_by(thread_id=thread.id, turn_id=old, status="streaming").update(
                {"status": "interrupted"})
        db.commit()
        db.refresh(thread)


class TurnLedger:
    def __init__(self, user_id, billing_workspace_id, thread_id=None, runtime=None, session_id=None):
        self.user_id, self.billing_workspace_id = user_id, billing_workspace_id
        self.thread_id, self.runtime = thread_id, runtime
        self.session_id = session_id or "mind-" + uuid4().hex
        self.token = uuid4().hex
        self.calls = 0

    def _claim(self, db):
        if db.get(MindAccount, self.user_id) is None:
            try:
                db.add(MindAccount(user_id=self.user_id))
                db.commit()
            except IntegrityError:
                db.rollback()
        now = utcnow()
        changed = db.execute(update(MindAccount).where(MindAccount.user_id == self.user_id,
            or_(MindAccount.claim.is_(None), MindAccount.lease_until <= now,
                MindAccount.claim == self.token)).values(
                    claim=self.token, lease_until=now + timedelta(seconds=LEASE_SECONDS))).rowcount
        db.commit()
        if changed != 1:
            raise AccountingError("accounting_busy")

    def _release(self):
        with SessionLocal() as db:
            db.execute(update(MindAccount).where(MindAccount.user_id == self.user_id,
                MindAccount.claim == self.token).values(claim=None, lease_until=None))
            db.commit()

    @staticmethod
    def _amount(value):
        if isinstance(value, dict):
            value = value.get("cost_usd", value.get("usd"))
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            return float(value)
        return None

    def _settle(self, db, session_id, total):
        total = self._amount(total)
        if total is None:
            return
        rows = db.query(MindCharge).filter_by(user_id=self.user_id, session_id=session_id).order_by(
            MindCharge.created_at, MindCharge.id).all()
        pending = [r for r in rows if not r.settled]
        paid = sum(r.usd or 0 for r in rows if r.settled)
        # A cumulative total below what is already settled is not trustworthy;
        # pending rows stay pending (fail closed) rather than guessing.
        if total < paid or len(pending) > 1:
            return
        if pending:
            pending[0].usd, pending[0].settled = total - paid, True
        elif total > paid and rows:
            # Growth reported after its request settled (e.g. late usage that
            # arrives after UTC midnight) is real spend: count only the delta,
            # on the day it became known, so it is never dropped or doubled.
            db.add(MindCharge(user_id=self.user_id, thread_id=rows[-1].thread_id,
                              billing_workspace_id=rows[-1].billing_workspace_id,
                              session_id=session_id, usd=total - paid, settled=True, created_at=utcnow()))
        else:
            return
        db.commit()

    async def _reconcile(self, db):
        pending = db.query(MindCharge).filter_by(user_id=self.user_id, settled=False).all()
        for row in pending:
            total = None
            if self.runtime is not None and hasattr(self.runtime, "session_cost"):
                try:
                    async with asyncio.timeout(10):
                        total = await self.runtime.session_cost(row.billing_workspace_id, row.session_id)
                except Exception:
                    pass  # Never persist or expose provider exceptions.
            self._settle(db, row.session_id, total)

    async def reconcile(self):
        try:
            with SessionLocal() as db:
                self._claim(db)
                await self._reconcile(db)
        finally:
            self._release()

    async def before_call(self):
        if self.calls >= MAX_CALLS:
            raise AccountingError("turn_limit")
        try:
            with SessionLocal() as db:
                self._claim(db)
                if self.thread_id:
                    owned(db, self.user_id, self.thread_id)
                elif self.billing_workspace_id not in memberships(db, self.user_id):
                    raise HTTPException(404, "Workspace not found")
                await self._reconcile(db)
                if db.query(MindCharge).filter_by(user_id=self.user_id, settled=False).first():
                    raise AccountingError("accounting_pending")
                daily = usage(db, self.user_id, self.thread_id)
                if daily["daily_spend"]["usd"] >= daily["daily_limit"]:
                    raise AccountingError("budget_exceeded")
                db.add(MindCharge(user_id=self.user_id, thread_id=self.thread_id,
                    billing_workspace_id=self.billing_workspace_id, session_id=self.session_id, created_at=utcnow()))
                db.commit()
                self.calls += 1
        except BaseException:
            self._release()
            raise

    async def after_call(self, usage):
        try:
            with SessionLocal() as db:
                # Verify lease ownership before applying a cumulative total.
                self._claim(db)
                self._settle(db, self.session_id, usage)
        finally:
            self._release()

    def close(self):
        self._release()
