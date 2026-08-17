# LinkPlease — Instagram DM Automation Backend

[![CI](https://github.com/sreePoojithaKalluru/LinkPLease/actions/workflows/ci.yml/badge.svg)](https://github.com/sreePoojithaKalluru/LinkPLease/actions/workflows/ci.yml)


Production-grade FastAPI backend for the LinkPlease take-home assignment.

## Stack
- **Python 3.11+** / **FastAPI** / **uvicorn** (single worker)
- **SQLite** (local dev) or **Postgres** (production) via `DATABASE_URL` env var
- **SQLAlchemy 2.0** async ORM
- **httpx** for async HTTP calls to the mock API
- **No Redis / Celery** — in-process asyncio workers backed by DB-persisted queue rows

## API Contract

| Method | Path | Status | Description |
|--------|------|--------|-------------|
| POST | `/webhook` | 200 always | Receive webhook events |
| POST | `/rules` | 201 | Create keyword → DM rule |
| GET | `/stats` | 200 | Live stats from DB counts |
| GET | `/health` | 200 | Deploy health check |

## Quick Start (local)

```bash
# 1. Clone and install
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env: set PSEUDOGRAM_API_URL, PSEUDOGRAM_API_KEY, WEBHOOK_SECRET

# 3. Run
uvicorn app.main:app --reload --workers 1
```

Server will be at `http://localhost:8000`. Tables are created automatically on startup.

## Architecture

Three background asyncio tasks run alongside the FastAPI server in the same process:

```
POST /webhook
    │
    ├─ Verify HMAC-SHA256 signature (raw bytes)
    ├─ INSERT events ON CONFLICT DO NOTHING  ← DB-level dedup
    └─ asyncio.Queue.put(event_id)
            │
            ▼
    [event_processor]
        comment.created → INSERT dm_attempts ON CONFLICT (user, rule) DO NOTHING
        comment.deleted → UPDATE dm_attempts SET status='cancelled' WHERE pending/queued
            │
            ▼
    [dm_sender]  (polls DB every 2s)
        pending rows → POST /v1/dm/send (rate-limited: 10 req/60s token bucket)
        202 → status='queued', store dm_id
        429 → back off (Retry-After), stay pending
        400 → status='failed' (permanent)
        5xx → exponential backoff + jitter, retry up to MAX_DM_RETRIES
            │
            ▼
    [reconciler]  (runs every 10s)
        queued rows → GET /v1/dm/{dm_id}
        delivered  → status='sent'
        failed     → retry (new idempotency key) or status='failed' if exhausted
```

## Key Design Decisions

### DB-enforced deduplication
- `events.event_id` is the PRIMARY KEY — `ON CONFLICT DO NOTHING` is atomic
- `dm_attempts` has `UNIQUE(recipient_user_id, rule_id)` — no Python if-check for double-DM

### Stats computed from DB only
```
sent              = COUNT(*) WHERE dm_attempts.status = 'sent'
failed            = COUNT(*) WHERE dm_attempts.status = 'failed'
queued (in stats) = COUNT(*) WHERE dm_attempts.status IN ('pending', 'queued')
duplicates_blocked = COUNT(*) FROM dedup_events
```

### Rate limiter
- Token bucket: 10 tokens / 60 seconds
- In-process (asyncio.Lock) — correct only with `--workers 1`
- Respects `Retry-After` header on 429

### Restart recovery
- On startup, all `events WHERE processed_at IS NULL` are re-enqueued
- dm_sender polls DB directly, so pending rows are picked up automatically

## Deployment (Render)

1. Push to GitHub
2. Create new Render Web Service → connect repo
3. Render detects `render.yaml` and provisions a Postgres database automatically
4. Set these env vars in the Render dashboard:
   - `PSEUDOGRAM_API_URL`
   - `PSEUDOGRAM_API_KEY`
   - `WEBHOOK_SECRET`
5. Deploy → note the service URL for the grading script

## Testing Against the Simulator

```bash
# 1. Start simulation (replace with your deployed URL and run_id)
curl -X POST https://mock-api.example.com/v1/simulate/start \
  -H "Authorization: Bearer $PSEUDOGRAM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"webhook_url": "https://your-app.onrender.com/webhook", "count": 500, "duration_seconds": 10}'

# 2. Wait ~30 seconds for reconciler to catch up

# 3. Check your stats
curl https://your-app.onrender.com/stats

# 4. Check ground truth
curl https://mock-api.example.com/v1/simulate/{run_id}/truth \
  -H "Authorization: Bearer $PSEUDOGRAM_API_KEY"
```

See `FAILURES.md` for known limitations and honest tradeoff documentation.
