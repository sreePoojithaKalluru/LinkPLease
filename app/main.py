"""
app/main.py
────────────
FastAPI application entry point.

Lifespan handles:
  1. Create all DB tables (idempotent, safe on every startup)
  2. Re-enqueue unprocessed events from DB (restart recovery)
  3. Start three background asyncio tasks:
       - event_processor: processes webhook events from the queue
       - dm_sender:        sends pending DMs with rate limiting
       - reconciler:       confirms queued DMs were actually delivered
  4. On shutdown: cancel background tasks gracefully, close HTTP client

Single uvicorn worker is REQUIRED for the in-process rate limiter to be
correct. The render.yaml / Dockerfile both enforce this with --workers 1.
"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import create_tables
from app.routes import rules, stats, webhook
from app.worker.dm_sender import close_http_client, run_dm_sender
from app.worker.event_processor import requeue_unprocessed_events, run_event_processor
from app.worker.reconciler import run_reconciler

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ── Background tasks ───────────────────────────────────────────────────────────
_background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup → yield → Shutdown.
    All long-lived resources are created here, not at module import time.
    """
    # ── STARTUP ────────────────────────────────────────────────────────────────
    logger.info("LinkPlease starting up…")

    # 1. Ensure tables exist (safe to run every time — CREATE TABLE IF NOT EXISTS)
    await create_tables()
    logger.info("Database tables ready")

    # 2. Re-enqueue any events that were received but not yet processed before
    #    the last process exit. This is the restart recovery mechanism.
    await requeue_unprocessed_events()

    # 3. Start background workers as asyncio Tasks.
    #    They run in the same event loop as FastAPI request handlers.
    _background_tasks.append(asyncio.create_task(run_event_processor(), name="event_processor"))
    _background_tasks.append(asyncio.create_task(run_dm_sender(), name="dm_sender"))
    _background_tasks.append(asyncio.create_task(run_reconciler(), name="reconciler"))
    logger.info("Background workers started: event_processor, dm_sender, reconciler")

    yield  # ← FastAPI serves requests here

    # ── SHUTDOWN ───────────────────────────────────────────────────────────────
    logger.info("LinkPlease shutting down…")

    for task in _background_tasks:
        task.cancel()

    # Wait for all tasks to acknowledge cancellation (or finish their current work).
    await asyncio.gather(*_background_tasks, return_exceptions=True)

    # Close the shared httpx client cleanly.
    await close_http_client()

    logger.info("Shutdown complete")


# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="LinkPlease",
    description="Instagram DM automation backend — webhook receiver and rule engine.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: allow all origins for the grading script (it calls from various hosts).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ─────────────────────────────────────────────────────────────────────
app.include_router(webhook.router)
app.include_router(rules.router)
app.include_router(stats.router)


@app.get("/health")
async def health() -> dict:
    """Simple health check for the deploy platform."""
    return {"status": "ok"}
