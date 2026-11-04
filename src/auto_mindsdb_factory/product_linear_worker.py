"""Controller-side, durable Linear to product-delivery handoff.
A claimed issue is never built twice. Linear comments are at-least-once: the
client only exposes a bounded comment history, so old markers may age out.
Cancellation can race the final Git push after the last guard; Linear and Git
cannot participate in one atomic transaction.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import replace
from pathlib import Path

from .product_delivery import ProductDelivery
from .product_linear_intake import eligibility, issue_fingerprint, task_from_issue
from .product_linear_state import LinearDeliveryState, LinearStateError, _result
from .product_promotion import GitHubPromotion
from .run_journal import RunJournal


class SourceConflict(RuntimeError):
    """The source is no longer owned by this controller."""


class LinearProductWorker:

    def __init__(self, profile, target_root, store, route, client, *,
                 delivery_factory=ProductDelivery, promotion_factory=GitHubPromotion):
        if profile.document.get("auto_merge") is not True:
            raise ValueError("Automatic Linear delivery requires auto_merge")
        if (not profile.document.get("required_status_checks")
                or any(not name.strip() for name in profile.document["required_status_checks"])):
            raise ValueError("Automatic Linear delivery requires named GitHub CI checks")
        self.profile, self.target, self.store = profile, Path(target_root), Path(store)
        self.route, self.client = route, client
        self.state = LinearDeliveryState(self.store, self.target)
        self.delivery_factory, self.promotion_factory = delivery_factory, promotion_factory

    def _owner(self, journal):
        path = journal.root / "owner.json"
        if path.is_symlink():
            raise SourceConflict("Invalid ownership evidence")
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(doc, dict) or set(doc) != {"assignee"}:
                raise ValueError()
            return doc["assignee"]
        except (OSError, ValueError):
            raise SourceConflict("Missing ownership evidence") from None

    def _freeze_owner(self, journal, snapshot):
        # A crash between claim and this file fails closed during reconciliation.
        path = journal.root / "owner.json"
        with path.open("x", encoding="utf-8") as handle:
            json.dump({"assignee": snapshot.assignee}, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        fd = os.open(journal.root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _check(self, journal, binding, states=None):
        if binding["route_digest"] != self.route.digest or binding["profile_digest"] != self.profile.digest:
            raise SourceConflict("Route or profile changed")
        snapshot = self.client.fetch_issue_snapshot(binding["issue_id"])
        try:
            queued = replace(snapshot, state={"id": self.route.document["queued_state_id"]})
            if (snapshot.id.lower() != binding["issue_id"] or not eligibility(queued, self.route)
                    or issue_fingerprint(snapshot) != binding["fingerprint"]
                    or snapshot.identifier != binding["identifier"] or snapshot.url != binding["url"]
                    or snapshot.assignee != self._owner(journal)
                    or not isinstance(snapshot.state, dict) or not isinstance(snapshot.state.get("id"), str)
                    or not snapshot.state["id"].strip()
                    or (states is not None and snapshot.state["id"] not in states)):
                raise SourceConflict("Issue changed or is no longer controller-owned")
        except (ValueError, AttributeError, TypeError):
            raise SourceConflict("Invalid source snapshot") from None
        return snapshot

    def _recover(self, binding):
        blocked = {"status": "blocked", "reason": "Interrupted delivery; inspect product evidence"}
        root = self.store.resolve() / "delivery" / binding["task"]["id"]
        try:
            if root.parent.is_symlink() or root.is_symlink() or not root.is_dir() or (root / "manifest.json").is_symlink():
                return blocked
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            if (type(manifest.get("version")) is not int or manifest["version"] != 1
                    or not isinstance(manifest.get("created_at"), str) or not manifest["created_at"].strip()
                    or manifest.get("task_sha") != binding["task_digest"] or manifest.get("profile_sha") != binding["profile_digest"]
                    or manifest.get("task") != binding["task"] or manifest.get("profile") != self.profile.document
                    or manifest.get("purpose") != "delivery"):
                return blocked
            if any(p.is_symlink() or (p.name.startswith("event-") and not re.fullmatch(r"event-[0-9]{6}\.json", p.name)) for p in root.iterdir()):
                return blocked
            events = RunJournal(self.store, binding["task"]["id"]).events()
            if any(type(e.get("sequence")) is not int or set(e) != {"sequence", "at", "kind", "data"}
                   or not isinstance(e["at"], str) or not e["at"].strip()
                   or not isinstance(e["kind"], str) or not e["kind"].strip()
                   or not (root / f"event-{index:06d}.json").is_file()
                   for index, e in enumerate(events, 1)):
                return blocked
            finished = [(i, e["data"]) for i, e in enumerate(events) if e["kind"] == "run_finished"]
            merges = [(i, e["data"]) for i, e in enumerate(events) if e["kind"] == "merge_confirmed"]
            pubs = [(i, e["data"]) for i, e in enumerate(events) if e["kind"] == "pull_request_published"]
            if len(finished) != 1 or finished[0][1].get("status") != "complete" or len(merges) != 1 or len(pubs) != 1:
                return blocked
            mi, merge = merges[0]
            pi, pub = pubs[0]
            fi = finished[0][0]
            sha = merge.get("commit_sha")
            number = pub.get("number")
            if not (pi < mi < fi == len(events) - 1 and pub.get("commit_sha") == sha and type(number) is int and number > 0
                    and type(merge.get("pr_number")) is int and number == merge["pr_number"]
                    and isinstance(pub.get("url"), str)
                    and pub["url"].lower() == f"https://github.com/{self.profile.document['repository']}/pull/{number}".lower()):
                return blocked
            smoke = [(i, e["data"]) for i, e in enumerate(events) if e["kind"] == "post_merge_smoke"]
            commands = self.profile.document["smoke"]
            if len(smoke) != len(commands) or any(
                not (mi < i < fi and d.get("commit_sha") == sha and type(d.get("index")) is int and d.get("index") == n
                     and d.get("command") == command and type(d.get("exit_code")) is int and d["exit_code"] == 0)
                for n, ((i, d), command) in enumerate(zip(smoke, commands), 1)
            ):
                return blocked
            result = {"status": "complete", "merge_sha": sha, "pull_request_url": pub.get("url")}
            _result(result)
            return result
        except Exception:
            return blocked

    def _sync(self, journal, binding, outcome, lease):
        lease.heartbeat()
        snapshot = self._check(journal, binding)
        marker = f"[ai-factory run:{binding['task']['id']} outcome]"
        if outcome["status"] == "complete":
            comment = f"{marker} Code verified and merged. PR: {outcome['pull_request_url']}\nMerge SHA: {outcome['merge_sha']}"
        else:
            comment = f"{marker} Blocked. {outcome.get('reason', 'Inspect product evidence')}"
        lease.heartbeat()
        if not any(isinstance(c, dict) and c.get("body") == comment for c in snapshot.comments):
            self.client.create_comment(binding["issue_id"], comment)
        lease.heartbeat()
        self.state.mark_synced(journal)

    def _reconcile(self, journal, lease):
        binding = self.state.get_binding(journal)
        if binding["route_digest"] != self.route.digest or binding["profile_digest"] != self.profile.digest:
            raise SourceConflict("Route or profile changed")
        outcome = self.state.outcome(journal)
        if outcome is None:
            outcome = self._recover(binding)
            lease.heartbeat()
        self.state.record_outcome(journal, outcome)
        self._sync(journal, binding, outcome, lease)
        return outcome["status"]

    def _claim(self, issue_id, lease):
        snapshot = self.client.fetch_issue_snapshot(issue_id)
        if snapshot.id.lower() != issue_id.lower() or not eligibility(snapshot, self.route):
            return "ineligible"
        task = task_from_issue(snapshot, self.route)
        path = self.state.delivery / task.document["id"]
        existed = path.exists() or path.is_symlink()
        try:
            journal = self.state.claim(task, snapshot, self.route, self.profile)
        except LinearStateError as exc:
            raise SourceConflict("Conflicting source claim") from exc
        if existed:
            # Even a fully synced claim is immutable and never authorizes a rebuild.
            return "already claimed"
        self._freeze_owner(journal, snapshot)
        binding = self.state.get_binding(journal)
        config = self.route.document
        promoted = []
        try:
            lease.heartbeat()
            marker = f"[ai-factory run:{binding['task']['id']} started]"
            snapshot = self._check(journal, binding, {config["queued_state_id"]})
            if not any(isinstance(c, dict) and c.get("body") == marker + " Delivery started." for c in snapshot.comments):
                self.client.create_comment(issue_id, marker + " Delivery started.")
            lease.heartbeat()
            self._check(journal, binding, {config["queued_state_id"]})
            delivery = self.delivery_factory(self.profile, self.target, self.store)
            def guard():
                lease.heartbeat()
                self._check(journal, binding, {config["queued_state_id"]})
                lease.heartbeat()
            def promote(candidate, product_journal):
                guard()
                promotion = self.promotion_factory(self.profile, task, self.target, self.store,
                    smoke=delivery.smoke, deadline=delivery.deadline, before_merge=guard)
                result = promotion(candidate, product_journal)
                promoted.append(result)
                return result
            result = delivery.run(task, promote=promote)
            if (result.get("status") == "complete" and len(promoted) == 1
                    and promoted[0].get("status") == "complete"
                    and result.get("merge_sha") == promoted[0].get("merge_sha")
                    and result.get("pull_request_url") == promoted[0].get("pull_request_url")):
                outcome = {"status": "complete", "merge_sha": result.get("merge_sha"),
                           "pull_request_url": result.get("pull_request_url")}
                _result(outcome)
                if not re.fullmatch(r"https://github\.com/" + re.escape(self.profile.document["repository"]) + r"/pull/[1-9][0-9]*", outcome["pull_request_url"], re.IGNORECASE):
                    raise ValueError("Publication repository mismatch")
            else:
                outcome = {"status": "blocked", "reason": "Delivery did not complete"}
        except Exception:
            outcome = {"status": "blocked", "reason": "Delivery blocked; inspect product evidence"}
        # Deliberately outside the exception handler: failed persistence is recovered
        # from the product journal, never converted into another result or rerun.
        lease.heartbeat()
        self.state.record_outcome(journal, outcome)
        self._sync(journal, binding, outcome, lease)
        return outcome["status"]

    def run_cycle(self, max_issues=1):
        if type(max_issues) is not int or not 1 <= max_issues <= 100:
            raise ValueError("max_issues must be an integer from 1 to 100")
        try:
            self.client.validate_route(self.route)
        except Exception:
            raise RuntimeError("Linear route validation failed") from None
        results, seen = [], set()
        for journal in self.state.pending():
            binding = self.state.get_binding(journal)
            issue_id = binding["issue_id"]
            seen.add(issue_id)
            try:
                with self.state.lease(issue_id) as lease:
                    if self.state.is_synced(journal):
                        continue
                    status = self._reconcile(journal, lease)
            except SourceConflict:
                status = "conflict"
            except Exception:
                status = "pending"
            results.append({"issue_id": issue_id, "status": status})
        claimed = self.state.claimed_issue_ids()
        try:
            queued = self.client.queued_issue_ids(self.route, limit=max_issues, exclude_ids=claimed)
        except Exception:
            raise RuntimeError("Linear queue unavailable") from None
        for issue_id in queued:
            if issue_id in seen:
                continue
            try:
                with self.state.lease(issue_id) as lease:
                    status = self._claim(issue_id, lease)
            except SourceConflict:
                status = "conflict"
            except Exception:
                status = "pending"
            results.append({"issue_id": issue_id, "status": status})
        return results
