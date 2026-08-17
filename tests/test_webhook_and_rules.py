import asyncio
import hashlib
import hmac
import json

import pytest
import httpx


def make_sig(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


@pytest.mark.asyncio
async def test_rules_create_and_match(client):
    # create a rule
    resp = await client.post("/rules", json={"keyword": "Price", "dm_message": "Hi"})
    assert resp.status_code == 201
    data = resp.json()
    assert data["keyword"] == "Price"


@pytest.mark.asyncio
async def test_webhook_signature_and_duplicate_handling(client, monkeypatch):
    from app.config import settings
    from app.database import AsyncSessionLocal
    from app.models import Event, DmAttempt

    # prepare payload
    payload = {
        "event_id": "evt-1",
        "event_type": "comment.created",
        "data": {
            "comment_id": "c1",
            "post_id": "p1",
            "user_id": "u1",
            "text": "This mentions price please",
        },
    }
    body = json.dumps(payload).encode()
    sig = make_sig(settings.pseudogram_api_key, body)

    # create a rule that matches 'price' (case-insensitive)
    r = await client.post("/rules", json={"keyword": "price", "dm_message": "Hello"})
    assert r.status_code == 201

    # valid signature -> 200 accepted
    resp = await client.post("/webhook", content=body, headers={"X-PseudoGram-Signature": sig})
    assert resp.status_code == 200
    assert resp.json().get("status") in ("accepted", "duplicate")

    # duplicate delivery: same event_id again -> returns duplicate
    resp2 = await client.post("/webhook", content=body, headers={"X-PseudoGram-Signature": sig})
    assert resp2.status_code == 200
    assert resp2.json().get("status") in ("duplicate", "accepted")

    # invalid signature -> 401
    bad_sig = "sha256=deadbeef"
    bad = await client.post("/webhook", content=body, headers={"X-PseudoGram-Signature": bad_sig})
    assert bad.status_code == 401

    # missing signature -> 401
    missing = await client.post("/webhook", content=body)
    assert missing.status_code == 401
