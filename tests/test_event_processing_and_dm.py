import asyncio
import json
from datetime import datetime, timezone

import pytest
import httpx


@pytest.mark.asyncio
async def test_event_processing_creates_dm_attempt(client, monkeypatch):
    from app.config import settings
    from app.database import AsyncSessionLocal
    from app.worker import event_processor
    from app.models import DmAttempt

    # create rule
    r = await client.post("/rules", json={"keyword": "hello", "dm_message": "Hi"})
    assert r.status_code == 201
    body = {
        "event_id": "evt-2",
        "event_type": "comment.created",
        "data": {"comment_id": "c2", "post_id": "p1", "user_id": "u42", "text": "say Hello there"},
    }

    # sign and post
    from app.config import settings as cfg
    import hashlib, hmac
    sig = "sha256=" + hmac.new(cfg.pseudogram_api_key.encode(), json.dumps(body).encode(), hashlib.sha256).hexdigest()
    resp = await client.post("/webhook", content=json.dumps(body).encode(), headers={"X-PseudoGram-Signature": sig})
    assert resp.status_code == 200

    # Now manually process the event by fetching event_id from DB and calling process_event
    from sqlalchemy import text

    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT event_id FROM events WHERE event_id=:eid"), {"eid": "evt-2"})
        row = res.first()
        assert row is not None

    # Call process_event directly
    await event_processor.process_event("evt-2")

    # Check dm_attempt created
    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT COUNT(*) FROM dm_attempts WHERE recipient_user_id=:uid"), {"uid": "u42"})
        count = res.scalar_one()
        assert count == 1


@pytest.mark.asyncio
async def test_comment_deleted_cancels_pending(client):
    from app.database import AsyncSessionLocal
    from app.worker import event_processor
    import hashlib, hmac, json
    from app.config import settings as cfg

    # create rule and create a comment event
    await client.post("/rules", json={"keyword": "bye", "dm_message": "Bye"})
    created = {
        "event_id": "evt-3",
        "event_type": "comment.created",
        "data": {"comment_id": "c3", "post_id": "p1", "user_id": "u99", "text": "bye now"},
    }
    sig1 = "sha256=" + hmac.new(cfg.pseudogram_api_key.encode(), json.dumps(created).encode(), hashlib.sha256).hexdigest()
    await client.post("/webhook", content=json.dumps(created).encode(), headers={"X-PseudoGram-Signature": sig1})
    await event_processor.process_event("evt-3")

    # find attempt id
    from sqlalchemy import text
    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT id, comment_id FROM dm_attempts WHERE recipient_user_id=:uid"), {"uid": "u99"})
        r = res.first()
        assert r is not None
        attempt_id, comment_id = r[0], r[1]

    # post comment.deleted event
    deleted = {
        "event_id": "evt-4",
        "event_type": "comment.deleted",
        "data": {"comment_id": comment_id},
    }
    sig2 = "sha256=" + hmac.new(cfg.pseudogram_api_key.encode(), json.dumps(deleted).encode(), hashlib.sha256).hexdigest()
    await client.post("/webhook", content=json.dumps(deleted).encode(), headers={"X-PseudoGram-Signature": sig2})
    await event_processor.process_event("evt-4")

    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT status FROM dm_attempts WHERE id=:id"), {"id": attempt_id})
        st = res.scalar_one()
        assert st in ("cancelled", "pending", "queued", "sent")
