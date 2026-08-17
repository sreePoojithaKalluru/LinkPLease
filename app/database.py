"""
app/database.py
───────────────
SQLAlchemy async engine + session factory.

Supports both SQLite (local dev) and Postgres (production) via DATABASE_URL.
The only thing that changes between environments is the connection string —
no application code needs to know which backend is in use.
"""
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


def _normalize_db_url(url: str) -> str:
    """
    Render (and Heroku) provide DATABASE_URL as `postgresql://` or `postgres://`.
    SQLAlchemy 2.0 requires the async driver to be specified explicitly.
    Normalise both forms to `postgresql+asyncpg://` so no manual env editing is needed.
    """
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


_db_url = _normalize_db_url(settings.database_url)

# ── Engine ───────────────────────────────────────────────────────────────────
# connect_args is SQLite-specific: enables WAL mode so reads don't block writes.
# For Postgres the dict is empty and pool settings come from the URL.
_connect_args = {}
if _db_url.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}

engine = create_async_engine(
    _db_url,
    echo=False,           # set True for SQL debug logging
    connect_args=_connect_args,
    # For SQLite we want a conservative pool; Postgres handles its own pooling.
    pool_pre_ping=True,
)

# ── Session factory ───────────────────────────────────────────────────────────
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # avoids lazy-load errors after commit
    autoflush=False,
    autocommit=False,
)


# ── Declarative base ─────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────
async def get_db() -> AsyncSession:
    """FastAPI dependency that yields a database session."""
    async with AsyncSessionLocal() as session:
        yield session


async def create_tables() -> None:
    """Create all tables on first run (idempotent)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
