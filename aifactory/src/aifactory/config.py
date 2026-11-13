"""Runtime settings, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path


def _bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    """An integer environment setting; a missing or malformed value falls back to the default."""
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def concurrency_limit(value: object, default: int) -> int:
    """Normalize execution/planning limits, including legacy stored settings."""
    try:
        return max(1, min(int(value), 50))
    except (TypeError, ValueError, OverflowError):
        return default


def _bounded_int(name: str, default: int | None, minimum: int, maximum: int) -> int | None:
    """Reject unsafe pool overrides rather than silently accepting unbounded capacity."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        result = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}") from None
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return result


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    database_url: str
    secret_key: str
    public_url: str
    environment: str
    # Sign-in
    oidc_issuer: str
    oidc_client_id: str
    oidc_client_secret: str
    allow_api_key_sign_in: bool
    allow_dev_sign_in: bool
    # Inference defaults
    mindshub_api_base: str
    mindshub_auth_url: str
    openai_api_base: str
    bootstrap_openai_key: str
    bootstrap_mindshub_key: str
    bootstrap_github_token: str
    # Execution
    jobs_dir: Path
    sandbox_user: str
    global_concurrency: int
    global_planning_concurrency: int
    orchestrator_enabled: bool
    internal_url: str
    allowed_emails: tuple[str, ...] = field(default_factory=tuple)
    allowed_org_ids: tuple[str, ...] = field(default_factory=tuple)

    # None derives the pool from configured execution/planning plus 16 background/HTTP slots.
    db_pool_size: int | None = None
    db_max_overflow: int = 0
    sqlite_busy_timeout_ms: int = 2000

    @property
    def database_pool_size(self) -> int:
        return self.db_pool_size if self.db_pool_size is not None else (
            self.global_concurrency + self.global_planning_concurrency + 16)

    # Positive provider-wide rolling-minute allowances (shared across workspaces).
    mindshub_requests_per_minute: int = 50
    openai_requests_per_minute: int = 50

    github_app_id: str = field(default="", repr=False)
    github_app_private_key: str = field(default="", repr=False)
    github_app_client_id: str = field(default="", repr=False)
    github_app_client_secret: str = field(default="", repr=False)
    github_app_webhook_secret: str = field(default="", repr=False)

    @property
    def github_app_available(self) -> bool:
        return all((self.github_app_id, self.github_app_private_key, self.github_app_client_id,
                    self.github_app_client_secret, self.github_app_webhook_secret))

    @property
    def secure_cookies(self) -> bool:
        return self.public_url.startswith("https://")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    data_dir = Path(os.environ.get("AIFACTORY_DATA_DIR", "./.aifactory-data")).resolve()
    port = os.environ.get("PORT", "8000")
    return Settings(
        data_dir=data_dir,
        database_url=os.environ.get("DATABASE_URL", f"sqlite:///{data_dir / 'factory.db'}"),
        secret_key=os.environ.get("AIFACTORY_SECRET_KEY", ""),
        public_url=os.environ.get("AIFACTORY_PUBLIC_URL", f"http://localhost:{port}").rstrip("/"),
        environment=os.environ.get("AIFACTORY_ENV", "development"),
        oidc_issuer=os.environ.get("MINDSHUB_OIDC_ISSUER", "https://auth.mindshub.ai/auth/realms/mindsdb").rstrip("/"),
        oidc_client_id=os.environ.get("MINDSHUB_OIDC_CLIENT_ID", ""),
        oidc_client_secret=os.environ.get("MINDSHUB_OIDC_CLIENT_SECRET", ""),
        allow_api_key_sign_in=_bool("AIFACTORY_ALLOW_API_KEY_SIGN_IN", True),
        allow_dev_sign_in=_bool("AIFACTORY_ALLOW_DEV_SIGN_IN", False),
        mindshub_api_base=os.environ.get("MINDSHUB_API_BASE", "https://api.mindshub.ai/v1").rstrip("/"),
        mindshub_auth_url=os.environ.get("MINDSHUB_AUTH_URL", "https://auth.mindshub.ai/v1/authenticate/"),
        openai_api_base=os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/"),
        bootstrap_openai_key=os.environ.get("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", ""),
        bootstrap_mindshub_key=os.environ.get("AIFACTORY_BOOTSTRAP_MINDSHUB_API_KEY", ""),
        bootstrap_github_token=os.environ.get("AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", ""),
        github_app_id=os.environ.get("AIFACTORY_GITHUB_APP_ID", "").strip(),
        github_app_private_key=os.environ.get("AIFACTORY_GITHUB_APP_PRIVATE_KEY", "").strip(),
        github_app_client_id=os.environ.get("AIFACTORY_GITHUB_APP_CLIENT_ID", "").strip(),
        github_app_client_secret=os.environ.get("AIFACTORY_GITHUB_APP_CLIENT_SECRET", "").strip(),
        github_app_webhook_secret=os.environ.get("AIFACTORY_GITHUB_APP_WEBHOOK_SECRET", "").strip(),
        jobs_dir=Path(os.environ.get("AIFACTORY_JOBS_DIR", str(data_dir / "jobs"))).resolve(),
        sandbox_user=os.environ.get("AIFACTORY_SANDBOX_USER", ""),
        global_concurrency=concurrency_limit(os.environ.get("AIFACTORY_GLOBAL_CONCURRENCY"), 8),
        global_planning_concurrency=concurrency_limit(os.environ.get("AIFACTORY_GLOBAL_PLANNING_CONCURRENCY"), 4),
        db_pool_size=_bounded_int("AIFACTORY_DB_POOL_SIZE", None, 1, 128),
        db_max_overflow=_bounded_int("AIFACTORY_DB_MAX_OVERFLOW", 0, 0, 32) or 0,
        sqlite_busy_timeout_ms=_bounded_int("AIFACTORY_SQLITE_BUSY_TIMEOUT_MS", 2000, 0, 5000) or 0,
        orchestrator_enabled=_bool("AIFACTORY_ORCHESTRATOR", True),
        internal_url=os.environ.get("AIFACTORY_INTERNAL_URL", f"http://127.0.0.1:{port}").rstrip("/"),
        mindshub_requests_per_minute=max(1, _int("AIFACTORY_MINDSHUB_REQUESTS_PER_MINUTE", 50)),
        openai_requests_per_minute=max(1, _int("AIFACTORY_OPENAI_REQUESTS_PER_MINUTE", 50)),
        allowed_emails=tuple(e.strip().lower() for e in os.environ.get("AIFACTORY_ALLOWED_EMAILS", "").split(",") if e.strip()),
        allowed_org_ids=tuple(o.strip() for o in os.environ.get("AIFACTORY_ALLOWED_ORG_IDS", "").split(",") if o.strip()),
    )


def reset_settings_cache() -> None:
    get_settings.cache_clear()
