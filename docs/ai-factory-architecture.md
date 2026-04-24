# AI Factory Architecture

This is the current executable shape of the factory: Anthropic release-note intake, policy assignment, staged build/review/eval/security/merge/release/monitoring/feedback, recurring automation around a persisted run store, and a first real vertical slice that creates GitHub PR evidence while keeping deployment and observability file-backed.

## Workflow

```mermaid
flowchart TD
    A["Anthropic release notes"] --> B["Stage 1: Scout + Clarifier"]
    B --> C["Spec packet"]
    C --> D["Policy Engine"]
    D --> E{"Relevant?"}
    E -->|"watchlist / ignore"| Z["Stop: watchlisted or rejected"]
    E -->|"active build"| F["Stage 2: Ticket Architect + Eval Engineer"]
    F --> G["Ticket bundle + eval manifest"]
    G --> H["Stage 3: Builder + Reviewer"]
    H --> I{"PR reviewable?"}
    I -->|"no"| H
    I -->|"yes"| J["Stage 4: Integration Engineer"]
    J --> K["Prompt contract + tool schema + golden dataset + latency baseline"]
    K --> L["Stage 5: Eval Runner"]
    L --> M{"Merge-gating evals pass?"}
    M -->|"no"| H
    M -->|"yes"| N["Stage 6: Security Sentinel"]
    N --> O{"Security approved?"}
    O -->|"blocked"| H
    O -->|"human signoff needed"| P["Wait: SECURITY_REVIEWING"]
    O -->|"approved"| Q["Merge Conductor"]
    Q --> R{"Merged?"}
    R -->|"blocked"| H
    R -->|"human signoff needed"| S["Wait: MERGE_REVIEWING"]
    R -->|"merged"| T["Stage 7: Release Manager"]
    T --> U{"Promoted?"}
    U -->|"blocked"| H
    U -->|"release signoff needed"| V["Wait: STAGING_SOAK"]
    U -->|"promoted"| W["Stage 8: SRE Sentinel"]
    W --> X{"Production status"}
    X -->|"healthy"| Y["Stage 9: Weekly feedback"]
    X -->|"incident or open follow-up"| AA["Immediate Stage 9 feedback"]
    Y --> AB["Feedback report + backlog candidates"]
    AA --> AB
    AB --> A
```

## Runtime

```mermaid
flowchart LR
    subgraph Workspace["Local workspace: /Users/ian/auto-mindsdb-eng"]
        CLI["auto-mindsdb-factory CLI"]
        Runtime["src/auto_mindsdb_factory runtime"]
        Schemas["schemas/*.schema.json"]
        Policies["factory/policies/*.yaml"]
        Fixtures["fixtures/scenarios + fixtures/intake"]
        Tests["pytest + validate_contracts.py"]
    end

    subgraph Store["Run store: .factory-automation"]
        Bundles["stage1..stage9 + merge result bundles"]
        Signals["file-backed staging, monitoring, rollback signals"]
        State["automation-state.json"]
        Leases["state lease + per-run leases"]
    end

    subgraph External["External seams"]
        GitHub["GitHub repo + PR + checks"]
    end

    subgraph Schedulers["How it is invoked"]
        Manual["One-shot stage commands"]
        Supervisor["automation-supervisor-cycle"]
        Stage1Loop["automation-stage1-cycle"]
        ProgressionLoop["automation-advance-runs"]
        WeeklyLoop["automation-weekly-feedback"]
    end

    Manual --> CLI
    Supervisor --> CLI
    Stage1Loop --> CLI
    ProgressionLoop --> CLI
    WeeklyLoop --> CLI
    CLI --> Runtime
    Runtime --> Schemas
    Runtime --> Policies
    Runtime --> Bundles
    Runtime --> Signals
    Runtime --> State
    Runtime --> Leases
    Runtime --> GitHub
    Tests --> Runtime
    Fixtures --> Tests
```

## Execution Surfaces

- One-shot stages run through `uv run auto-mindsdb-factory stage1-intake`, `stage2-ticketing`, `stage3-build-review`, `stage4-integration`, `stage5-eval`, `stage6-security-review`, `stage-merge`, `stage7-release-staging`, `stage8-production-monitoring`, and `stage9-feedback-synthesis`.
- The autonomous lane runs through `uv run auto-mindsdb-factory automation-supervisor-cycle --store-dir .factory-automation ...`.
- Persisted work lives in `.factory-automation`, with stage result bundles per work item and shared automation state in `automation-state.json`.
- The first real vertical slice runs through `uv run auto-mindsdb-factory factory-vertical-slice --store-dir .factory-automation --repository ianu82/ai-factory`.
- GitHub is the first real connector seam: the slice creates a branch, commits a small evidence file, opens a draft PR, and records PR/check status in the run store.
- Staging, monitoring, and rollback use JSON signal files in `.factory-automation/ops-signals/<work_item_id>/` until real deploy, observability, feature-flag, and rollback providers replace them.
- Operators can inspect the local control-plane view with `uv run auto-mindsdb-factory factory-cockpit --store-dir .factory-automation`.
- Governance lives in `factory/policies/*.yaml`; handoff shape and drift checks live in `schemas/*.schema.json` and `scripts/validate_contracts.py`.
- The current boundary is live rollback execution. Stage 8 can model mitigation and escalation, Stage 9 can synthesize incident feedback, and the vertical slice can require rollback-probe evidence, but no real infrastructure rollback command is executed yet.
