# Devin-Native Software Factory

A reimplementation of the ai-factory pipeline as Devin Playbooks and Knowledge items, replacing the Python orchestration system with Devin's interactive, stateful session model.

## Architecture

The original ai-factory uses a 9-stage Python pipeline (`intake → ticketing → build_review → integration → eval → security → merge → staging → monitoring → feedback`). This Devin-native version distills the core value — **taking requirements through to a solid draft PR** — into 4 composable assets that run within Devin's session model:

```
Original ai-factory                    Devin-Native Factory
─────────────────                      ────────────────────
Stage 1: Intake (intake.py)        →   Knowledge: Spec Template
Stage 2: TicketArchitect           →   Playbook 1: Analyze + Plan
  (ticketing.py)                       (dynamic codebase analysis replaces
                                        static PATH_HINTS)
Stage 3: Builder + BudgetGuardian  →   Playbook 2: Build + Test + Fix Loop
  (build_review.py)                    (3-attempt budget limit preserved)
PR Packet schema                   →   Knowledge: PR Description Template
  (schemas/pr-packet.schema.json)      (reviewer_report.non_blocking_findings
                                        → "Left for You" section)
linear_trigger.py                  →   Knowledge: Trigger Wiring
                                       (GitHub label, Linear state, Slack,
                                        or manual trigger)
```

### Key Design Decisions

1. **Dynamic over static**: The original `PATH_HINTS` dict (build_review.py:88-155) hardcoded file paths per concern area. Playbook 1 instead performs live codebase analysis — grep for imports, read conventions from actual files, discover test/lint commands from CI config.

2. **Budget-guarded fix loop**: The `BudgetGuardian` pattern (build_review.py:63-75) is preserved — Playbook 2 enforces a hard limit of 3 fix attempts before documenting failures and proceeding.

3. **Human gates preserved**: The original pipeline had `autonomy_mode` and approval requirements. The Devin-native version has two explicit gates: spec confirmation and plan approval. Both block the session until the engineer responds.

4. **Honest failure reporting**: Inspired by `reviewer_report.non_blocking_findings` from the pr-packet schema, the PR template includes a mandatory "What I Couldn't Figure Out / Left for You" section.

## Assets

| Deliverable | Type | ID | Purpose |
|---|---|---|---|
| Spec Template | Knowledge Item | `note-396e6348a4a642f9a20fcdfdaf74e05c` | Normalize any input into a structured engineering spec |
| Playbook 1: Analyze + Plan | Playbook | `playbook-f7765a94bc69403ca17ee3b187a71893` | Dynamic codebase analysis → implementation plan |
| PR Description Template | Knowledge Item | `note-dab3b5bdf4304b68af7ed47312e72007` | Consistent, reviewer-friendly PR descriptions |
| Playbook 2: Build + Test + Fix Loop | Playbook | `playbook-b35dfdc9dd6f4805bdf2f9fab98959d9` | Implement → test → fix (3x max) → draft PR |
| Trigger Wiring | Knowledge Item | `note-4f4cb4f121f84abcbcdc141f6ca798bd` | Pipeline flow docs + trigger setup instructions |

## Pipeline Flow

```
Trigger (GitHub issue label / Linear state / Slack / Manual)
  │
  ▼
Phase 1: Spec Normalization (Spec Template knowledge)
  Input → Structured Spec + Clarifying Questions → WAIT for confirmation
  │
  ▼
Phase 2: Analyze + Plan (Playbook 1)
  Codebase analysis → Implementation Plan → WAIT for approval
  │
  ▼
Phase 3: Build + Test + Fix Loop (Playbook 2)
  Implement → Test → Fix (up to 3x) → Draft PR (using PR Description Template)
  │
  ▼
Draft PR Created → Engineer reviews
```

## Quick Start

### Manual trigger (fastest way to try it)

Start a Devin session with this prompt:

```
Normalize the following into a structured engineering spec using the
"Software Factory — Spec Template" knowledge item, then follow the full
factory pipeline (Playbook 1 → Playbook 2 → Draft PR):

[paste ticket content, issue URL, or raw description here]

Target repository: [owner/repo]
```

### GitHub issue trigger

1. Add a `factory-intake` label to your repo
2. Configure Devin's GitHub integration to trigger on that label
3. Label any issue → Devin starts the full pipeline

### Linear trigger

1. Create a "Factory Intake" workflow state in your Linear team
2. Configure Devin's Linear integration to trigger on that state
3. Move any issue to "Factory Intake" → Devin starts the full pipeline

See the [Trigger Wiring knowledge item](https://app.devin.ai) for detailed setup instructions.

## Testing

### Test 1: Spec Template — 3 Input Formats

Feed these three formats and verify specs are complete with relevant clarifying questions:
1. GitHub issue URL
2. Linear ticket URL  
3. Raw text description

### Test 2: Playbook 1 — Plan Quality

Run against your actual codebase with a known change. Verify:
- Correct files identified
- Conventions accurately captured from existing code
- Quality commands discovered from CI/Makefile/package.json
- Risk flags generate "Human Review Required" sections

### Test 3: PR Template — Output Format

Review a real PR created by the pipeline. Verify:
- All sections populated
- Test results are actual (not fabricated)
- "Left for You" section is honest
- Scope check matches the approved plan

### Test 4: Playbook 2 — End-to-End

Run on a small change (e.g., "add a new field to an existing config"). Verify:
- Tests run and fix loop works
- Budget limit (3 attempts) respected
- PR description follows the template
- Draft PR created correctly

### Test 5: Full Pipeline

Trigger → Spec → Confirm → Plan → Approve → Build → Draft PR
- Verify each gate blocks correctly
- Verify state carries through the session

## Comparison with Original ai-factory

| Aspect | Original (Python) | Devin-Native |
|---|---|---|
| Orchestration | Python state machine (`automation.py`) | Devin session flow with human gates |
| File discovery | Static `PATH_HINTS` dict | Dynamic grep + codebase analysis |
| Budget enforcement | `BudgetGuardian` class | Playbook instruction (3 attempts max) |
| PR format | `pr-packet.schema.json` | Knowledge item template |
| Risk handling | `risk_profile` in spec-packet schema | Risk flags checkboxes + Human Review Required |
| Triggers | `linear_trigger.py` webhook server | Devin integrations (GitHub/Linear/Slack) |
| State persistence | File-based run store | Devin session conversation context |
| Agent | OpenAI API + Codex CLI | Devin |
