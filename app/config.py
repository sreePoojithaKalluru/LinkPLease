"""
app/config.py
─────────────
All configuration is read from environment variables (or a .env file).
Pydantic-settings validates types and provides defaults.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Database ─────────────────────────────────────────────────────────────
    # Use sqlite+aiosqlite:///./linkplease.db for local dev.
    # Swap to postgresql+asyncpg://user:pass@host/db on Render/Fly.
    database_url: str = "sqlite+aiosqlite:///./linkplease.db"

    # ── Mock API ──────────────────────────────────────────────────────────────
    pseudogram_api_url: str = "https://mock-api.example.com"
    pseudogram_api_key: str = "changeme"

    # ── Webhook signature secret ──────────────────────────────────────────────
    # Legacy compatibility value; per assignment spec the HMAC secret is the
    # same as PSEUDOGRAM_API_KEY. Request verification uses that value.
    webhook_secret: str = "changeme"

    # ── Worker tuning ────────────────────────────────────────────────────────
    # Maximum number of send + reconcile retries before a dm_attempt is
    # permanently marked "failed".
    max_dm_retries: int = 5

    # How often (seconds) the reconciler polls queued DMs.
    reconciler_interval_seconds: int = 15

    # How often (seconds) the dm_sender polls for pending rows.
    dm_sender_poll_seconds: float = 2.0

    # Rate limit: 10 requests per 60 seconds (spec value — do not change).
    rate_limit_requests: int = 10
    rate_limit_window_seconds: float = 60.0


# Module-level singleton — import `settings` everywhere.
settings = Settings()
