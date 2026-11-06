# Scoped GitHub publication

Direct deliveries use the controller's GitHub authorization by default. When the
operator already has independent authorization for another repository, explicit
`--github-transport operator-handoff --publish` selects a network-only handoff.
This does not grant the controller access to that repository or move credentials.
The controller still plans, builds, independently verifies, checks CI, performs
exact-base promotion and confirms the merge before fresh smoke commands.

`--base-sha` accepts a full commit already present in the trusted target Git store.
It avoids a fetch using an unrelated repository credential. Export/import source
only through an authorized connection; the later remote base check and atomic
compare-and-swap remain mandatory. A changed base makes verification stale.

After candidate validation, the private run directory gains `github-handoff/`.
Its `scope.json` identifies the repository, run, tested base, candidate and branch,
and hashes `candidate.bundle`. The bundle contains only the retained candidate
attempt ref and objects required beyond the base. These files stay outside all
candidate jobs. They contain no authorization tokens.

An operator reads each `*.request.json` and uses `product-github-handoff` with
independently confirmed `--repository`, `--run-id`, `--base-sha`, `--candidate-sha`,
`--base-branch`, `--target-root`, `--request` and a stable `--receipts-dir` outside
the target source. The operator target must have the same GitHub origin/push URL
and exact candidate commit. If needed, `--bundle` and `--bundle-sha256` import the
checked candidate objects without changing the checkout. No candidate code runs
in the operator process, and this command makes no inference requests.

The command retains an exclusive intent before one network operation, then a
digest-bound response. Transfer the resulting `<request-id>.response.json` back
through the authenticated private channel without altering it. Keep the same
receipt directory for the entire run. Requests expire after at most five minutes
and never extend the original run deadline. Cancellation stops admission while
waiting; an already completed external write cannot be undone by cancellation.

The allowlist permits exact ref reads/creation, tested-base CAS, PR/check reads,
PR creation and the controller's actual independent-verification status. It
cannot alter repository access/protections, merge through an API, replace an
unrelated ref, or write `validate`/`sandbox` statuses. Existing GitHub protections
remain in force. Authentic CI results are still read and checked by the controller.

An unavailable, failed, malformed, expired or mismatched response stops delivery.
An existing intent prohibits repeating the same request with the same receipt
directory. Inspect actual remote state after uncertainty; do not erase receipts,
resume the original run, manufacture acknowledgements or blindly repeat a write.
Retain the failed run and its original outcome. Handoff scope is a controller and
operator trust boundary, not protection against an authorized operator rewriting
controller code or private evidence.
