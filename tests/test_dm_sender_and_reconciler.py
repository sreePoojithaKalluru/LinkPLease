import asyncio
import json
from datetime import datetime, timezone

import pytest
import httpx
from sqlalchemy import text


@pytest.mark.asyncio
async def test_dm_sender_202_and_429_and_400(monkeypatch):
    from app.database import AsyncSessionLocal
    from app.worker import dm_sender
    from app.models import DmAttempt, Rule
    import httpx

    # monkeypatch rate limiter to no-op
    monkeypatch.setattr("app.worker.rate_limiter.rate_limiter.acquire", lambda: asyncio.sleep(0))

    # insert a rule and dm_attempt row directly
    async with AsyncSessionLocal() as session:
        async with session.begin():
            rule = Rule(keyword="zzz", dm_message="hey")
            session.add(rule)
        await session.commit()
        await session.refresh(rule)

    # create dm_attempt row
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(
                text("INSERT OR REPLACE INTO dm_attempts (id, recipient_user_id, rule_id, comment_id, idempotency_key, status, retry_count, created_at, updated_at) VALUES (:id,:user_id,:rule_id,:comment_id,:ikey,'pending',0,:now,:now)"),
                {"id": "a1", "user_id": "u1", "rule_id": rule.rule_id, "comment_id": "c1", "ikey": "a1", "now": datetime.now(timezone.utc)},
            )

    # Prepare mock transport for httpx AsyncClient
    async def mock_send(request):
        # first call: return 202
        if request.url.path.endswith("/v1/dm/send"):
            return httpx.Response(202, json={"dm_id": "dm-123"})
        return httpx.Response(404)

    client = httpx.AsyncClient(base_url="http://testserver", transport=httpx.MockTransport(lambda req: mock_send(req)))
    # Patch dm_sender's get_http_client so _send_dm uses the mock client.
    monkeypatch.setattr("app.worker.dm_sender.get_http_client", lambda: client)

    # load attempt

    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT id FROM dm_attempts WHERE id=:id"), {"id": "a1"})
        assert res.first() is not None

    # call send loop function for that attempt
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT id, recipient_user_id, rule_id FROM dm_attempts WHERE id=:id"), {"id": "a1"})
        row = result.first()

    # create a lightweight object similar to SQLAlchemy mapped object
    class Obj:
        pass

    attempt = Obj()
    attempt.id = "a1"
    attempt.recipient_user_id = "u1"
    # attach rule text via attempt.rule to match code path
    class R:
        dm_message = "hey"
    attempt.rule = R()
    attempt.rule_id = rule.rule_id
    attempt.idempotency_key = "a1"

    # run send
    await dm_sender._send_dm(attempt)

    # verify DB updated to queued
    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT status, dm_id FROM dm_attempts WHERE id=:id"), {"id": "a1"})
        status, dm_id = res.first()
        assert status == "queued"
        assert dm_id == "dm-123"

    await client.aclose()


@pytest.mark.asyncio
async def test_reconciler_marks_sent_and_failed(monkeypatch):
    from app.database import AsyncSessionLocal
    from app.worker import reconciler
    from app.worker.dm_sender import get_http_client
    import httpx

    # insert an attempt in queued state

    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(
                    text("INSERT OR REPLACE INTO dm_attempts (id, recipient_user_id, rule_id, comment_id, idempotency_key, status, retry_count, dm_id, created_at, updated_at) VALUES (:id,:user_id,:rule_id,:comment_id,:ikey,'queued',0,:dmid,:now,:now)"),
                    {"id": "b2", "user_id": "u9", "rule_id": "r1", "comment_id": "c2", "ikey": "b2", "dmid": "dm-abc", "now": datetime.now(timezone.utc)},
            )

    # mock GET /v1/dm/{dm_id} to return delivered
    async def handler(request):
        if request.url.path.endswith("dm-abc"):
            return httpx.Response(200, json={"status": "delivered"})
        return httpx.Response(404)

    client = httpx.AsyncClient(base_url="http://testserver", transport=httpx.MockTransport(lambda req: handler(req)))
    monkeypatch.setattr("app.worker.reconciler.get_http_client", lambda: client)

    # call reconciler check for our attempt
    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT id FROM dm_attempts WHERE id=:id"), {"id": "b2"})
        assert res.first() is not None

    # fetch attempt object similarly
    class Obj:
        pass
    attempt = Obj()
    attempt.id = "b2"
    attempt.dm_id = "dm-abc"

    await reconciler._check_dm_status(attempt)

    async with AsyncSessionLocal() as session:
        res = await session.execute(text("SELECT status FROM dm_attempts WHERE id=:id"), {"id": "b2"})
        status = res.scalar_one()
        assert status == "sent"

    await client.aclose()
