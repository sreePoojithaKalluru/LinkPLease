"""
app/worker/reconciler.py
─────────────────────────
Reconciliation loop: every N seconds, poll GET /v1/dm/{dm_id} for every
dm_attempts row in status='queued' to confirm actual delivery.

Why this matters (Part C of spec):
  ~15% of DMs accepted with 202 later fail at the mock API level.
  Without reconciliation, those would stay as 'queued' forever and never
  show in /stats as failed (and never be retried). This loop catches them.

Reads are NOT counted against the 10 req/60s rate limit per the spec.
We therefore do NOT call rate_limiter.acquire() for GET requests here.

After confirming 'failed' from the mock API:
  - If retry_count < MAX_DM_RETRIES: reset status to 'pending' with a new
    idempotency key (f"{id}-r{retry_count}") so dm_sender will re-send.
    A new idempotency key is used because the old send definitively failed —
    we want a fresh attempt, not a deduplicated one.
  - If retry_count >= MAX_DM_RETRIES: mark 'failed' permanently.
"""
import asyncio
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, update

from app.config import settings
from app.database import AsyncSessionLocal
from app.models import DmAttempt
from app.worker.dm_sender import get_http_client

logger = logging.getLogger(__name__)


async def _check_dm_status(attempt: DmAttempt) -> None:
    """Poll GET /v1/dm/{dm_id} and update the attempt row accordingly."""
    if not attempt.dm_id:
        logger.warning(
            "reconciler: attempt %s is queued but has no dm_id — skipping",
            attempt.id,
        )
        return

    client = get_http_client()
    now = datetime.now(timezone.utc)

    try:
        resp = await client.get(f"/v1/dm/{attempt.dm_id}")
    except httpx.RequestError as exc:
        logger.warning("reconciler: network error polling dm %s: %s", attempt.dm_id, exc)
        return

    if resp.status_code == 404:
        # dm_id not found — treat as a failed send.
        logger.warning("reconciler: dm_id %s returned 404 — treating as failed", attempt.dm_id)
        await _handle_reconcile_failure(attempt, now)
        return

    if resp.status_code != 200:
        logger.warning(
            "reconciler: unexpected status %d for dm_id %s", resp.status_code, attempt.dm_id
        )
        return

    body = resp.json()
    # The mock API returns a "status" field: "delivered" | "failed" | "pending"
    dm_status = body.get("status", "").lower()

    if dm_status == "delivered":
        # ── Confirmed delivered → mark 'sent' ────────────────────────────────
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(status="sent", updated_at=now)
                )
        logger.info("reconciler: dm_id %s delivered → attempt %s marked sent", attempt.dm_id, attempt.id)

    elif dm_status == "failed":
        await _handle_reconcile_failure(attempt, now)

    else:
        # Still pending on the mock API side — check again next cycle.
        logger.debug("reconciler: dm_id %s still pending", attempt.dm_id)


async def _handle_reconcile_failure(attempt: DmAttempt, now: datetime) -> None:
    """
    The mock API confirmed this DM failed. Either retry (new idempotency key)
    or mark permanently failed.
    """
    new_retry_count = attempt.retry_count + 1

    if new_retry_count >= settings.max_dm_retries:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(status="failed", retry_count=new_retry_count, updated_at=now)
                )
        logger.error(
            "reconciler: attempt %s permanently failed after %d retries",
            attempt.id, new_retry_count,
        )
    else:
        # New idempotency key for the retry — the old attempt definitively
        # failed, so we want the mock API to treat this as a fresh request.
        new_ikey = f"{attempt.id}-r{new_retry_count}"
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(
                        status="pending",
                        retry_count=new_retry_count,
                        idempotency_key=new_ikey,
                        dm_id=None,          # clear old dm_id; sender will get a new one
                        next_retry_at=None,  # eligible immediately for re-send
                        updated_at=now,
                    )
                )
        logger.warning(
            "reconciler: attempt %s failed on mock API — reset to pending (retry %d/%d, ikey=%s)",
            attempt.id, new_retry_count, settings.max_dm_retries, new_ikey,
        )


async def run_reconciler() -> None:
    """
    Long-running asyncio task. Runs every RECONCILER_INTERVAL_SECONDS.
    Fetches all 'queued' dm_attempts and checks their actual delivery status.
    """
    logger.info(
        "reconciler: started (interval=%ds)", settings.reconciler_interval_seconds
    )
    while True:
        await asyncio.sleep(settings.reconciler_interval_seconds)
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(DmAttempt).where(DmAttempt.status == "queued")
                )
                queued = result.scalars().all()

            if queued:
                logger.info("reconciler: checking %d queued dm(s)", len(queued))
                for attempt in queued:
                    await _check_dm_status(attempt)
            else:
                logger.debug("reconciler: no queued dms to check")

        except Exception as exc:
            logger.exception("reconciler: unhandled error: %s", exc)
