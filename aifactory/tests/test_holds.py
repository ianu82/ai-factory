"""A provider hold says why the provider refused, and what lifts it, wherever it shows."""
from __future__ import annotations

import pytest

from aifactory.models import Decision
from aifactory.pipeline.holds import CAUSES, hold_cause, hold_details


@pytest.mark.parametrize("error, cause", [
    ("MindsHub: no credit remaining (insufficient balance)", "credit"),
    ("Error code: 402 - Payment Required", "credit"),
    ("Provider returned 402: wallet_empty", "credit"),
    ("You exceeded your current quota, please check your plan and billing details.", "credit"),
    ("MindsHub refused the credential (401): invalid key", "credential"),
    ("Error code: 403 - Key disabled", "credential"),
    ("No model provider is configured for this workspace.", "no_provider"),
    ("MindsHub limit reached (429): Monthly limit reached", "limit"),
    ("Rate limit reached for gpt-terra", "rate_limit"),
    ("Error code: 429 - Too many requests", "rate_limit"),
    ("The model provider is momentarily overloaded. It did not recover after 3 attempts.", "overloaded"),
    ("MindsHub error 503: upstream unavailable", "overloaded"),
    ("The provider said no, in words no rule here knows", "unknown"),
    ("Used 1,402,000 tokens before the refusal", "unknown"),  # digits inside a number are not a status
])
def test_a_refusal_names_its_cause(error, cause):
    assert hold_cause(error) == cause
    label, advice = CAUSES[cause]
    assert label and advice.endswith("resume work.")


def test_a_hold_opened_before_causes_were_kept_is_read_from_its_text():
    # The wording of the hold that stopped the AI Factory workspace on 12 Nov 2026.
    hold = Decision(kind="provider_hold", title="Model provider refused: work is on hold", body=(
        "The model provider refused a request (planning outcome #59, the planner):\n\nThe model provider is "
        "momentarily overloaded. It did not recover after 3 attempts.\n\nNothing new starts in this workspace, neither "
        "planning nor tasks, until someone chooses Resume work. Work that is refused the same way waits in the queue; "
        "no task is charged an attempt for it.\n\nWhat to do: add credits or fix the provider credential in Settings, "
        "then choose Resume work. If the provider still refuses, this hold opens again."))
    assert hold_details(hold) == {
        "cause": "overloaded", "label": CAUSES["overloaded"][0], "advice": CAUSES["overloaded"][1],
        "source": "planning outcome #59, the planner",
        "error": "The model provider is momentarily overloaded. It did not recover after 3 attempts."}
    # One opened since keeps its cause: its text is never read for it.
    kept = Decision(kind="provider_hold", title="t", body="anything",
                    metadata_json={"cause": "credential", "source": "T2 of outcome #4, the builder session",
                                   "error": "Bad credentials"})
    assert hold_details(kept)["cause"] == "credential" and hold_details(kept)["source"].startswith("T2")
