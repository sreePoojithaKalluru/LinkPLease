"""
app/worker/event_processor.py
──────────────────────────────
Background worker that consumes events from the in-process asyncio.Queue
and executes business logic (comment matching, DM queuing, cancellations).

Concurrency notes:
  - This is a single asyncio Task running in the same event loop as FastAPI.
  - It is the ONLY writer to dm_attempts for new rows, so the INSERT ... ON
    CONFLICT DO NOTHING is the concurrency barrier, not a Python if-check.
  - comment.deleted uses an UPDATE with a WHERE clause that checks status —
    this is atomic at the SQL level for a single-process setup. See FAILURES.md
    for the race window that exists with the dm_sender loop.
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, text, update

from app.database import AsyncSessionLocal
from app.models import DedupEvent, DmAttempt, Event, Rule
from app.schemas import CommentCreatedData, CommentDeletedData

logger = logging.getLogger(__name__)

# Module-level asyncio.Queue — shared between the webhook route and this worker.
# The queue holds event_id strings (not full payloads — the payload is in DB).
event_queue: asyncio.Queue[str] = asyncio.Queue()


async def _process_comment_created(session, event: Event) -> None:
    """
    Match the comment text against all rules, then for each match attempt to
    insert a dm_attempts row. Use INSERT ... ON CONFLICT DO NOTHING so the
    UNIQUE(recipient_user_id, rule_id) constraint is the only dedup gate.
    """
    payload = json.loads(event.raw_payload)
    try:
        data = CommentCreatedData(**payload["data"])
    except Exception as exc:
        logger.error("Failed to parse comment.created payload for event %s: %s", event.event_id, exc)
        return

    comment_text_lower = data.text.lower()

    # Fetch all rules — typically a small table (tens of rows at most).
    rules = (await session.execute(select(Rule))).scalars().all()

    for rule in rules:
        # Case-insensitive substring match, as spec requires.
        if rule.keyword.lower() not in comment_text_lower:
            continue

        attempt_id = str(uuid.uuid4())
        idempotency_key = attempt_id  # stable key for first send

        # ── INSERT ... ON CONFLICT DO NOTHING ────────────────────────────────
        # CRITICAL: This is the ONLY place we check for duplicates. We do NOT
        # do SELECT first. The UNIQUE(recipient_user_id, rule_id) constraint on
        # dm_attempts makes this atomic — two concurrent workers (if they ever
        # existed) would both attempt to insert; exactly one would succeed.
        #
        # SQLAlchemy's dialect-agnostic way to express this:
        stmt = (
            text(
                """
                INSERT INTO dm_attempts
                    (id, recipient_user_id, rule_id, comment_id,
                     idempotency_key, status, retry_count,
                     created_at, updated_at)
                VALUES
                    (:id, :user_id, :rule_id, :comment_id,
                     :ikey, 'pending', 0,
                     :now, :now)
                ON CONFLICT (recipient_user_id, rule_id) DO NOTHING
                """
            )
        )
        result = await session.execute(
            stmt,
            {
                "id": attempt_id,
                "user_id": data.user_id,
                "rule_id": rule.rule_id,
                "comment_id": data.comment_id,
                "ikey": idempotency_key,
                "now": datetime.now(timezone.utc),
            },
        )

        if result.rowcount == 0:
            # The UNIQUE constraint fired — this user already has a DM
            # queued or sent for this rule. Record it in dedup_events so
            # /stats can count it accurately.
            dedup = DedupEvent(
                recipient_user_id=data.user_id,
                rule_id=rule.rule_id,
                comment_id=data.comment_id,
            )
            session.add(dedup)
            logger.info(
                "Dedup blocked: user=%s rule=%s comment=%s",
                data.user_id, rule.rule_id, data.comment_id,
            )
        else:
            logger.info(
                "DM attempt queued: user=%s rule=%s comment=%s id=%s",
                data.user_id, rule.rule_id, data.comment_id, attempt_id,
            )


async def _process_comment_deleted(session, event: Event) -> None:
    """
    If a comment is deleted and its DM attempt is still pending/queued,
    cancel it. If already sent, leave it — DMs already delivered can't be
    recalled, and this is documented in FAILURES.md.
    """
    payload = json.loads(event.raw_payload)
    try:
        data = CommentDeletedData(**payload["data"])
    except Exception as exc:
        logger.error("Failed to parse comment.deleted payload for event %s: %s", event.event_id, exc)
        return

    # UPDATE with WHERE checks status atomically — no race between SELECT and UPDATE.
    # Race with dm_sender: if dm_sender fetched the row and is mid-send when
    # this UPDATE runs, the DM may still go out. See FAILURES.md bullet 3.
    result = await session.execute(
        update(DmAttempt)
        .where(
            DmAttempt.comment_id == data.comment_id,
            DmAttempt.status.in_(["pending", "queued"]),
        )
        .values(status="cancelled", updated_at=datetime.now(timezone.utc))
    )

    if result.rowcount > 0:
        logger.info(
            "Cancelled %d dm_attempt(s) for deleted comment %s",
            result.rowcount, data.comment_id,
        )
    else:
        logger.info(
            "comment.deleted for %s — no cancellable attempts (may already be sent)",
            data.comment_id,
        )


async def process_event(event_id: str) -> None:
    """Fetch one event from DB and dispatch to the appropriate handler."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            event = await session.get(Event, event_id)
            if event is None:
                logger.error("event_processor: event_id %s not found in DB", event_id)
                return

            if event.processed_at is not None:
                # Already processed (e.g. re-queued after restart but then
                # processed twice from the queue before the UPDATE committed).
                logger.debug("Skipping already-processed event %s", event_id)
                return

            if event.event_type == "comment.created":
                await _process_comment_created(session, event)
            elif event.event_type == "comment.deleted":
                await _process_comment_deleted(session, event)
            else:
                logger.warning("Unknown event_type %s for event %s", event.event_type, event_id)

            event.processed_at = datetime.now(timezone.utc)
            # session.begin() will commit on successful exit


async def run_event_processor() -> None:
    """
    Long-running asyncio task. Consumes event_ids from the queue and processes
    each one. Exceptions per event are caught and logged — a bad event must not
    kill the whole worker loop.
    """
    logger.info("event_processor: started")
    while True:
        event_id = await event_queue.get()
        try:
            await process_event(event_id)
        except Exception as exc:
            logger.exception("event_processor: unhandled error for event %s: %s", event_id, exc)
        finally:
            event_queue.task_done()


async def requeue_unprocessed_events() -> None:
    """
    Called at startup. Finds all events with processed_at IS NULL and pushes
    them onto the queue so the worker picks them up. This makes the worker
    resumable after a process restart.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Event.event_id).where(Event.processed_at.is_(None))
        )
        ids = result.scalars().all()

    for event_id in ids:
        await event_queue.put(event_id)

    if ids:
        logger.info("Requeued %d unprocessed event(s) from DB after startup", len(ids))
