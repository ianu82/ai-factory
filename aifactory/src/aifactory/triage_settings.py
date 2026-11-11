"""Bounded, strict workspace triage configuration (independent of agent runtime)."""
from pydantic import BaseModel, ConfigDict, Field


class TriageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = True
    interval_seconds: int = Field(default=1800, gt=0, le=31_536_000)
    run_usd: float = Field(default=1, gt=0, allow_inf_nan=False)
    day_usd: float = Field(default=10, gt=0, allow_inf_nan=False)
    run_tokens: int = Field(default=100_000, gt=0, le=6_000_000)
    max_turns: int = Field(default=20, gt=0, le=120)


DEFAULT_TRIAGE = TriageSettings().model_dump()
