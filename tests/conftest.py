import asyncio
import importlib
import os
import tempfile

import pytest
import pytest_asyncio
import httpx


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def tmp_db_path(tmp_path_factory):
    p = tmp_path_factory.mktemp("data") / "test_linkplease.db"
    return str(p)


@pytest.fixture(autouse=True)
def configure_test_db(tmp_db_path, monkeypatch):
    # Point settings to a temporary sqlite DB file before importing DB modules
    from app import config

    monkeypatch.setattr(config.settings, "database_url", f"sqlite+aiosqlite:///{tmp_db_path}")

    # Ensure no app modules were imported earlier (clean import with test DB)
    import sys

    for m in list(sys.modules.keys()):
        if m == "app" or m.startswith("app."):
            sys.modules.pop(m, None)

    # Import database (will use monkeypatched settings.database_url)
    import app.database as database

    # Do not reload model/worker modules here to avoid duplicate table metadata.
    # Modules will pick up the test DB when they are imported after this fixture runs.

    # Create tables (use asyncio.run to create a loop reliably)
    import asyncio

    asyncio.run(database.create_tables())

    yield


@pytest_asyncio.fixture
async def client(monkeypatch):
    """Provide an httpx AsyncClient against the FastAPI app."""
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
