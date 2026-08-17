"""
app/routes/stats.py
────────────────────
GET /stats — return live counts from the database.

CRITICAL: Every number here comes from a COUNT() SQL query against the
database. There are NO in-memory counters anywhere in this codebase.
In-memory counters can drift on restart or concurrency bugs; DB counts
cannot (modulo transaction isolation, which SQLite/Postgres handle correctly).

Stats mapping (matches dm_attempts.status values exactly):
  sent              = COUNT(*) FROM dm_attempts WHERE status = 'sent'
  failed            = COUNT(*) FROM dm_attempts WHERE status = 'failed'
  queued (in stats) = COUNT(*) FROM dm_attempts WHERE status IN ('pending', 'queued')
  duplicates_blocked = COUNT(*) FROM dedup_events

The spec says: inflated numbers are worse than honest low ones.
We never count 'queued' (202-accepted) DMs as 'sent'.
"""
import logging

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import DedupEvent, DmAttempt
from app.schemas import StatsResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/stats", response_model=StatsResponse)
async def get_stats(db: AsyncSession = Depends(get_db)) -> StatsResponse:
    """
    Compute live stats from the database. Four independent COUNT queries.

    These are cheap on the expected data volume (thousands of rows at most).
    If the table grows large, add an index on dm_attempts.status — but for
    this assignment's scale, a full-scan COUNT is fine and simpler to audit.
    """
    # sent: DMs confirmed delivered by the reconciler
    sent_result = await db.execute(
        select(func.count()).where(DmAttempt.status == "sent")
    )
    sent = sent_result.scalar_one()

    # failed: DMs permanently failed (exhausted retries or 400)
    failed_result = await db.execute(
        select(func.count()).where(DmAttempt.status == "failed")
    )
    failed = failed_result.scalar_one()

    # queued: in-flight DMs (pending = not yet sent; queued = accepted, awaiting reconcile)
    # Both count as "queued" in the external stats — they're not yet confirmed delivered.
    queued_result = await db.execute(
        select(func.count()).where(DmAttempt.status.in_(["pending", "queued"]))
    )
    queued = queued_result.scalar_one()

    # duplicates_blocked: from dedup_events table — each row = one blocked duplicate
    dedup_result = await db.execute(select(func.count()).select_from(DedupEvent))
    duplicates_blocked = dedup_result.scalar_one()

    logger.debug(
        "stats: sent=%d failed=%d queued=%d duplicates_blocked=%d",
        sent, failed, queued, duplicates_blocked,
    )

    return StatsResponse(
        sent=sent,
        failed=failed,
        queued=queued,
        duplicates_blocked=duplicates_blocked,
    )
