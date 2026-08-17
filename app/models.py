"""
app/models.py
─────────────
SQLAlchemy ORM models. These define the EXACT schema.

Design rule: every correctness invariant that can be expressed as a DB
constraint IS expressed as a DB constraint — not an application-level
if-statement. See the UNIQUE constraints and the PK on events.event_id.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# rules
# ─────────────────────────────────────────────────────────────────────────────
class Rule(Base):
    __tablename__ = "rules"

    rule_id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    # keyword is stored in its original case; comparisons done case-insensitively
    keyword: Mapped[str] = mapped_column(Text, nullable=False)
    dm_message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    dm_attempts: Mapped[list["DmAttempt"]] = relationship("DmAttempt", back_populates="rule")


# ─────────────────────────────────────────────────────────────────────────────
# events
# ─────────────────────────────────────────────────────────────────────────────
class Event(Base):
    """
    One row per unique webhook delivery.

    event_id is the PRIMARY KEY — attempting to INSERT a duplicate raises
    IntegrityError at the DB level (or returns rowcount=0 with ON CONFLICT
    DO NOTHING). This is the ONLY dedup mechanism; there is no SELECT-then-
    INSERT anywhere in the codebase.
    """
    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(String, primary_key=True)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    raw_payload: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    # NULL means "not yet processed by the event_processor worker".
    # On restart, all rows with processed_at IS NULL are re-enqueued.
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ─────────────────────────────────────────────────────────────────────────────
# dm_attempts
# ─────────────────────────────────────────────────────────────────────────────
class DmAttempt(Base):
    """
    One row per (recipient_user_id, rule_id) pair that should ever be DMed.

    UNIQUE(recipient_user_id, rule_id) is a DB-level guarantee that the same
    user is never DMed twice for the same rule, regardless of how many matching
    comment events arrive — no Python-level if-check required or used.

    Status lifecycle:
        pending  → queued (202 from /v1/dm/send received, dm_id stored)
        queued   → sent   (reconciler confirms delivered)
        queued   → pending (reconciler finds failed; will retry)
        pending  → failed  (max retries exhausted, or 400 from API)
        pending  → cancelled (comment.deleted arrived before send)

    Stats mapping (these are the only sources for /stats — no in-memory counters):
        sent              = COUNT(*) WHERE status = 'sent'
        failed            = COUNT(*) WHERE status = 'failed'
        queued (in stats) = COUNT(*) WHERE status IN ('pending', 'queued')
        duplicates_blocked = COUNT(*) FROM dedup_events  [separate table]
    """
    __tablename__ = "dm_attempts"
    __table_args__ = (
        # This constraint is the single source of truth for "no double DM".
        UniqueConstraint("recipient_user_id", "rule_id", name="uq_user_rule"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    recipient_user_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    rule_id: Mapped[str] = mapped_column(String, ForeignKey("rules.rule_id"), nullable=False)
    comment_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # dm_id is returned by POST /v1/dm/send on 202; used by reconciler.
    dm_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Stable idempotency key for the FIRST send = str(id).
    # For retries after reconciler finds 'failed': f"{id}-r{retry_count}"
    # This means a retry doesn't accidentally double-send if the previous
    # attempt actually went through but we didn't record it.
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)

    # pending / queued / sent / failed / cancelled
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending", index=True)

    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # When non-null, the dm_sender loop will not pick this row up until now() >= next_retry_at.
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )

    rule: Mapped["Rule"] = relationship("Rule", back_populates="dm_attempts")


# ─────────────────────────────────────────────────────────────────────────────
# dedup_events
# ─────────────────────────────────────────────────────────────────────────────
class DedupEvent(Base):
    """
    Written whenever a dm_attempts INSERT fails due to UNIQUE(user, rule)
    — i.e., the same user already had a DM queued/sent for this rule and
    a second matching comment arrived.

    duplicates_blocked in /stats = COUNT(*) FROM dedup_events.

    Storing these separately (vs. trying to derive the number from dm_attempts)
    means the count is never ambiguous as statuses change.
    """
    __tablename__ = "dedup_events"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    recipient_user_id: Mapped[str] = mapped_column(String, nullable=False)
    rule_id: Mapped[str] = mapped_column(String, nullable=False)
    comment_id: Mapped[str] = mapped_column(String, nullable=False)
    blocked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
