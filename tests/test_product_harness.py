import copy

import pytest

from auto_mindsdb_factory import product_delivery
from auto_mindsdb_factory.product_harness import HarnessSelectionError, resolve_harness


def test_resolution_does_not_mutate_trusted_configuration():
    document = {"builder_harness": "native", "reviewer_model": "pinned-model"}
    original = copy.deepcopy(document)
    assert resolve_harness(document, "builder") == "native"
    assert resolve_harness(document, "verifier") == "native"
    assert document == original


@pytest.mark.parametrize("role,document", [
    ("planner", {}),
    ("verifier", {"reviewer_harness": "codex"}),
    ("builder", {"builder_harness": "unknown"}),
])
def test_native_factory_rejects_selection_before_reading_credentials(monkeypatch, role, document):
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid harness reached provider configuration")

    monkeypatch.setattr(product_delivery.OpenAIResponsesAgentConfig, "from_env", forbidden)
    delivery = object.__new__(product_delivery.ProductDelivery)
    delivery.settings = document
    with pytest.raises(HarnessSelectionError):
        delivery._agent(role, None)
