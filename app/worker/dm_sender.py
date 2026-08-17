"""
app/worker/dm_sender.py
────────────────────────
Background worker: picks up dm_attempts with status='pending' and calls
POST /v1/dm/send on the mock API.

Key design decisions documented here (asked about on calls):

1. Idempotency key = dm_attempt.id (first send), or dm_attempt.id + "-r{n}"
   (nth retry after reconciler confirms failure). A stable key means: if the
   process crashes after the HTTP call but before the DB write, the next
   process will retry with the same key and the mock API will treat it as a
   duplicate — protecting against double-sends IF the mock API honours
   idempotency keys. If it doesn't, that's documented in FAILURES.md.

2. 202 → status='queued' (NOT 'sent'). The reconciler confirms delivery.

3. 429 → set Retry-After on the rate limiter; do not count as failure.
   The row remains 'pending' and will be retried after the back-off window.

4. 500 → exponential back-off with jitter; increment retry_count; if
   retry_count >= MAX_DM_RETRIES, mark 'failed'.

5. 400 → mark 'failed' immediately. A malformed request won't succeed on retry.

6. We query the DB on every poll cycle rather than keeping an in-memory list
   of pending IDs — this makes the worker restart-safe.
"""
import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database import AsyncSessionLocal
from app.models import DmAttempt
from app.worker.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)

# Shared httpx client — created once, reused across all sends.
# Timeout: 30s (generous; the mock API is local/cloud, not flaky internet).
_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            base_url=settings.pseudogram_api_url,
            headers={"Authorization": f"Bearer {settings.pseudogram_api_key}"},
            timeout=30.0,
        )
    return _http_client


async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()


async def _send_dm(attempt: DmAttempt) -> None:
    """
    Call POST /v1/dm/send for one dm_attempt and update the row in DB.
    This function does NOT raise — all outcomes are handled and persisted.
    """
    client = get_http_client()
    now = datetime.now(timezone.utc)

    # ── Acquire a rate-limit token before sending ─────────────────────────────
    # This may sleep until a token is available or the Retry-After window clears.
    await rate_limiter.acquire()

    try:
        resp = await client.post(
            "/v1/dm/send",
            json={
                "recipient_user_id": attempt.recipient_user_id,
                "message": attempt.rule.dm_message if attempt.rule else "",
                "rule_id": attempt.rule_id,
            },
            headers={"Idempotency-Key": attempt.idempotency_key},
        )
    except httpx.RequestError as exc:
        # Network-level failure — treat like a 500 (transient).
        logger.warning("dm_sender: network error for attempt %s: %s", attempt.id, exc)
        await _handle_transient_failure(attempt)
        return

    logger.info(
        "dm_sender: attempt %s → HTTP %d", attempt.id, resp.status_code
    )

    if resp.status_code == 202:
        # ── 202 Accepted ─────────────────────────────────────────────────────
        # DM has been accepted but NOT confirmed delivered. Status → 'queued'.
        # The reconciler will poll GET /v1/dm/{dm_id} to confirm delivery.
        body = resp.json()
        dm_id = body.get("dm_id") or body.get("id") or ""
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(
                        status="queued",
                        dm_id=dm_id,
                        updated_at=now,
                    )
                )
        logger.info("dm_sender: attempt %s queued, dm_id=%s", attempt.id, dm_id)

    elif resp.status_code == 429:
        # ── 429 Too Many Requests ─────────────────────────────────────────────
        # Do NOT count as failure. Tell the rate limiter to back off.
        # Row stays 'pending' — it will be retried automatically.
        retry_after = float(resp.headers.get("Retry-After", "60"))
        await rate_limiter.set_retry_after(retry_after)
        logger.warning(
            "dm_sender: 429 for attempt %s — backing off %.1fs", attempt.id, retry_after
        )
        # Set next_retry_at so we don't immediately re-fetch this row.
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=retry_after))
                )

    elif resp.status_code == 400:
        # ── 400 Bad Request ───────────────────────────────────────────────────
        # Malformed payload — retrying will never succeed. Mark failed now.
        logger.error(
            "dm_sender: 400 for attempt %s — marking failed permanently. Body: %s",
            attempt.id, resp.text[:200],
        )
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(status="failed", updated_at=now)
                )

    elif resp.status_code >= 500:
        await _handle_transient_failure(attempt)

    else:
        # Unexpected status code — treat as transient.
        logger.error("dm_sender: unexpected status %d for attempt %s", resp.status_code, attempt.id)
        await _handle_transient_failure(attempt)


async def _handle_transient_failure(attempt: DmAttempt) -> None:
    """
    Exponential backoff + jitter for 5xx / network errors.
    Cap retries at settings.max_dm_retries.
    """
    new_retry_count = attempt.retry_count + 1
    now = datetime.now(timezone.utc)

    if new_retry_count >= settings.max_dm_retries:
        # Max retries exhausted — mark failed permanently.
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(status="failed", retry_count=new_retry_count, updated_at=now)
                )
        logger.error(
            "dm_sender: attempt %s permanently failed after %d retries",
            attempt.id, new_retry_count,
        )
    else:
        # Exponential backoff: 2^retry * base (5s) + jitter (0–5s).
        base_delay = 5 * (2 ** attempt.retry_count)
        jitter = random.uniform(0, 5)
        delay = min(base_delay + jitter, 300)  # cap at 5 minutes
        next_retry = now + timedelta(seconds=delay)
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(DmAttempt)
                    .where(DmAttempt.id == attempt.id)
                    .values(
                        retry_count=new_retry_count,
                        next_retry_at=next_retry,
                        updated_at=now,
                    )
                )
        logger.warning(
            "dm_sender: attempt %s retry %d/%d in %.1fs",
            attempt.id, new_retry_count, settings.max_dm_retries, delay,
        )


async def run_dm_sender() -> None:
    """
    Long-running asyncio task. Polls the database every DM_SENDER_POLL_SECONDS
    for pending dm_attempts and sends them.

    Uses a DB query rather than an in-memory queue so that pending rows from
    before a restart are automatically picked up.
    """
    logger.info("dm_sender: started (poll interval=%.1fs)", settings.dm_sender_poll_seconds)
    while True:
        try:
            now = datetime.now(timezone.utc)
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(DmAttempt)
                    .where(
                        DmAttempt.status == "pending",
                        # Only pick up rows whose retry window has elapsed.
                        (DmAttempt.next_retry_at.is_(None))
                        | (DmAttempt.next_retry_at <= now),
                    )
                    # Eagerly load the rule so dm_message is available without a
                    # second round-trip per attempt (eliminates N+1 query pattern).
                    .options(selectinload(DmAttempt.rule))
                    .limit(50)  # process in batches to avoid holding a lock too long
                )
                attempts = result.scalars().all()

            for attempt in attempts:
                await _send_dm(attempt)

        except Exception as exc:
            logger.exception("dm_sender: unhandled error in poll cycle: %s", exc)

        await asyncio.sleep(settings.dm_sender_poll_seconds)
