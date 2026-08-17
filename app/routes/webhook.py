"""
app/routes/webhook.py
──────────────────────
POST /webhook

Contract (non-negotiable per spec):
  - Must return 200 within 5 seconds, ALWAYS — even on internal errors.
  - All real work is done AFTER the response is queued in the background worker.
  - Signature verification happens before any DB work (fail fast, fail cheap).
  - Dedup is enforced by the DB constraint on events.event_id, not by Python logic.
"""
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import text

from app.config import settings
from app.database import AsyncSessionLocal
from app.worker.event_processor import event_queue

logger = logging.getLogger(__name__)

router = APIRouter()


def _verify_signature(raw_body: bytes, signature_header: str | None) -> bool:
    """
    Verify HMAC-SHA256 signature.

    The header format is: sha256=<hex_digest>
    We compute HMAC-SHA256 of the raw request bytes using the API key as the
    secret — BEFORE any JSON parsing, as the spec requires.

    hmac.compare_digest is used (not ==) to prevent timing attacks.
    """
    if not signature_header:
        return False

    expected_hmac = hmac.new(
        settings.webhook_secret.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    expected = f"sha256={expected_hmac}"

    # compare_digest requires both args to be the same type (str here).
    return hmac.compare_digest(expected, signature_header)


@router.post("/webhook")
async def receive_webhook(request: Request) -> Response:
    """
    Receive a webhook event from the PseudoGram simulator.

    Steps:
    1. Read raw bytes (signature must be verified against raw bytes, not parsed JSON)
    2. Verify HMAC-SHA256 — return 401 on mismatch
    3. Parse JSON
    4. INSERT into events with ON CONFLICT DO NOTHING on event_id
    5. If rowcount == 0 → duplicate, return 200 immediately
    6. Push event_id to the background asyncio.Queue
    7. Return 200

    All exceptions are caught to honour the "always 200 within 5s" contract.
    """
    # ── 1. Read raw bytes first ───────────────────────────────────────────────
    try:
        raw_body = await request.body()
    except Exception as exc:
        logger.error("webhook: failed to read request body: %s", exc)
        return Response(status_code=200, content='{"status":"error","detail":"body read failed"}')

    # ── 2. Verify signature ───────────────────────────────────────────────────
    sig_header = request.headers.get("X-PseudoGram-Signature")
    if not _verify_signature(raw_body, sig_header):
        logger.warning("webhook: invalid signature — rejecting")
        # 401 is the only case where we don't return 200.
        raise HTTPException(status_code=401, detail="Invalid signature")

    # ── 3. Parse JSON ─────────────────────────────────────────────────────────
    try:
        payload = json.loads(raw_body)
        event_id = payload["event_id"]
        event_type = payload["event_type"]
    except (json.JSONDecodeError, KeyError) as exc:
        logger.error("webhook: malformed payload: %s", exc)
        # Still 200 — we can't crash the caller for bad data.
        return Response(status_code=200, content='{"status":"error","detail":"malformed payload"}')

    # ── 4. INSERT with ON CONFLICT DO NOTHING ─────────────────────────────────
    # This is the ONLY place that writes to the events table.
    # The PRIMARY KEY on event_id makes duplicate rejection atomic.
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    text(
                        """
                        INSERT INTO events (event_id, event_type, raw_payload, received_at)
                        VALUES (:event_id, :event_type, :raw_payload, :received_at)
                        ON CONFLICT (event_id) DO NOTHING
                        """
                    ),
                    {
                        "event_id": event_id,
                        "event_type": event_type,
                        "raw_payload": raw_body.decode("utf-8"),
                        "received_at": datetime.now(timezone.utc),
                    },
                )
                rows_affected = result.rowcount

    except Exception as exc:
        logger.exception("webhook: DB insert failed for event %s: %s", event_id, exc)
        return Response(status_code=200, content='{"status":"error","detail":"db error"}')

    # ── 5. Duplicate check ────────────────────────────────────────────────────
    if rows_affected == 0:
        # The ON CONFLICT fired — this event_id already exists. It's a
        # redelivery (~8% per spec). Do nothing else, return 200.
        logger.info("webhook: duplicate event_id %s — ignored", event_id)
        return Response(status_code=200, content='{"status":"duplicate"}')

    # ── 6. Enqueue for background processing ─────────────────────────────────
    # We enqueue the event_id only (not the payload). The worker re-reads from
    # DB, which means if the queue is lost (restart), the DB is the source of
    # truth and requeue_unprocessed_events() handles recovery.
    try:
        await event_queue.put(event_id)
    except Exception as exc:
        # Queue failure is very unlikely with asyncio.Queue (unbounded).
        # Log it but still return 200 — the event is in DB and will be
        # re-enqueued on next restart.
        logger.error("webhook: failed to enqueue event %s: %s", event_id, exc)

    logger.info("webhook: accepted event_id=%s type=%s", event_id, event_type)
    return Response(status_code=200, content='{"status":"accepted"}')
