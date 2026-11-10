"""Shared redaction tests run even on deployments without the Anton SDK."""
import pytest

from aifactory.security import redact


def fake_credentials():
    # Assemble every provider-shaped value at runtime. Never commit a complete
    # token-shaped literal, even a fake: provider push protection must stay on.
    return ["AK" + "IA" + "X" * 16, "AS" + "IA" + "Y" * 16,
            "xo" + "xb-" + "0" * 24, "xo" + "xp-" + "1" * 24,
            "gh" + "p_" + "a" * 36, "github" + "_pat_" + "a" * 50,
            "s" + "k-" + "a" * 32, "md" + "b_" + "b" * 32,
            "ey" + "JhbGciOiJIUzI1NiJ9" + "." + "ey" + "JzdWIiOiIxIn0" + "." + "a" * 32,
            "-----BEGIN " + "PRIVATE KEY-----\n" + "a" * 40 + "\n-----END " + "PRIVATE KEY-----",
            "-----BEGIN " + "RSA PRIVATE KEY-----\n" + "a" * 40 + "\n-----END " + "RSA PRIVATE KEY-----"]


@pytest.mark.parametrize("credential", fake_credentials(), ids=lambda value: "credential-format")
def test_common_unlabeled_credentials(credential):
    assert redact("Before " + credential + " after") == "Before [redacted] after"


def test_non_secret_narrative_is_preserved():
    assert redact("Migration summary for Factory run 123") == "Migration summary for Factory run 123"
