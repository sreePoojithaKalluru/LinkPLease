"""
app/schemas.py
──────────────
Pydantic request/response schemas for all API routes.
These are the authoritative shapes for the external API contract.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────────────────────
# POST /rules
# ─────────────────────────────────────────────────────────────────────────────
class RuleCreate(BaseModel):
    keyword: str = Field(..., min_length=1)
    dm_message: str = Field(..., min_length=1)


class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


# ─────────────────────────────────────────────────────────────────────────────
# GET /stats
# ─────────────────────────────────────────────────────────────────────────────
class StatsResponse(BaseModel):
    # DMs confirmed delivered by the reconciler (dm_attempts.status = 'sent')
    sent: int
    # DMs that exhausted retries or got a 400 (dm_attempts.status = 'failed')
    failed: int
    # DMs in-flight: status IN ('pending', 'queued')
    queued: int
    # Blocked by UNIQUE(user, rule) constraint: COUNT(*) FROM dedup_events
    duplicates_blocked: int


# ─────────────────────────────────────────────────────────────────────────────
# Webhook payload shapes (internal — used by the event_processor worker)
# ─────────────────────────────────────────────────────────────────────────────
class CommentCreatedData(BaseModel):
    comment_id: str
    post_id: str
    user_id: str          # the commenter — this is who we DM
    text: str
    timestamp: datetime | None = None


class CommentDeletedData(BaseModel):
    comment_id: str
    post_id: str | None = None
    user_id: str | None = None


class WebhookPayload(BaseModel):
    event_id: str
    event_type: Literal["comment.created", "comment.deleted"] | str
    data: dict             # parsed loosely; workers use typed sub-models
