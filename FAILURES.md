# FAILURES.md

This document describes the known failure modes of the LinkPlease backend.
Specificity is intentional: "handles all edge cases" is not a useful answer.
Every bullet names a trigger condition, a consequence, and (where applicable)
why the tradeoff was accepted.

---

## 1. In-flight crash between HTTP send and DB write (potential double-send)

**Trigger**: The process is killed (SIGKILL, OOM, hardware failure) after
`POST /v1/dm/send` has been sent over the wire and a `202` is received from the
mock API, but *before* the `UPDATE dm_attempts SET status='queued', dm_id=...`
transaction commits to the database.

**Consequence**: On restart, the `dm_sender` loop finds the attempt still in
`status='pending'` and re-sends it with the same `idempotency_key` (e.g.
`"abc123-uuid"`). If the mock API honours the idempotency key, it will return
the same `dm_id` without actually delivering a second DM — no double-send.
If the mock API does **not** honour idempotency keys, the user receives two DMs.

**Why not fixed**: Eliminating this window entirely requires a distributed
transaction or a write-ahead log on the HTTP call itself (e.g. recording the
send intent to DB *before* the HTTP call, then marking it committed after). That
is more complex than the spec warrants. The idempotency key is our best
protection; its effectiveness depends on the mock API's implementation.

---

## 2. `comment.deleted` race with the `dm_sender` loop

**Trigger**: A `comment.deleted` event arrives at the event_processor at almost
the same moment the `dm_sender` loop fetches a `pending` attempt for that
comment. Sequence: (a) dm_sender reads the row (status='pending'), (b)
event_processor runs `UPDATE dm_attempts SET status='cancelled' WHERE
comment_id=X AND status IN ('pending','queued')` — this update succeeds and the
row is now 'cancelled', (c) dm_sender has already fetched the row object and
calls `POST /v1/dm/send` — the DM goes out even though the comment was deleted.

**Consequence**: A DM is sent for a deleted comment. The `/stats` response will
show it as `queued` (then `sent` after reconciliation), which is accurate but
undesirable.

**Why not fixed**: Preventing this requires a `SELECT FOR UPDATE` row lock
around the fetch-then-send sequence in the dm_sender, or a status check
*immediately before* the HTTP call (which reintroduces a TOCTOU race if not
locked). Given the spec's single-process design and the very small timing
window (~dm_sender_poll_seconds), this is accepted. It is documented here
rather than silently ignored.

**Decision on already-sent DMs**: If the `comment.deleted` event arrives after
the DM is already in `status='sent'` or `status='queued'`, we leave it as-is.
DMs that have been delivered cannot be recalled; pretending they didn't happen
would misrepresent the stats. The `cancelled` status only applies to
`pending`/`queued` attempts.

---

## 3. Reconciler polling latency causes `queued` to appear as `queued` in stats

**Trigger**: The grading script fires `POST /v1/simulate/start` with
`count=500, duration_seconds=10` and then immediately compares `/stats` with
the ground-truth endpoint. The reconciler runs every `RECONCILER_INTERVAL_SECONDS`
(default: 15 seconds). DMs accepted with `202` will sit in `status='queued'`
until the next reconciler cycle.

**Consequence**: During the 10-second burst test (and for up to 15 seconds
after), `stats.sent` will be lower than the truth, and `stats.queued` will be
higher. If the grader checks immediately after the 10-second window ends, some
DMs that were delivered will not yet be reflected in `sent`. Running the grading
comparison ~30 seconds after the simulation ends allows two reconciler cycles to
complete and gives accurate numbers.

**Why not fixed**: The reconciler interval is configurable via
`RECONCILER_INTERVAL_SECONDS`. Setting it to 5 seconds (or lower) reduces this
window. It cannot be zero without continuously polling every queued DM, which
would add unnecessary API load between simulations.

---

## 4. `asyncio.Queue` loss if the process exits before the worker consumes an event

**Trigger**: The webhook handler inserts an event to the DB, pushes its
`event_id` to the in-process `asyncio.Queue`, and the process crashes between
these two operations — or after both, but before the `event_processor` worker
dequeues and processes it.

**Consequence**: On restart, `requeue_unprocessed_events()` runs and queries
`events WHERE processed_at IS NULL`. All such events are re-pushed to the queue.
**This window is safe**: the DB insert always happens *before* the queue push.
An event that is in the queue but not yet processed will have
`processed_at IS NULL` and will be re-enqueued. An event where the queue push
failed (very unlikely with unbounded `asyncio.Queue`) is also re-enqueued from
DB.

**One residual gap**: If the process crashes while the `event_processor` is
mid-transaction (between `BEGIN` and `COMMIT` of the `events.processed_at`
update), that event will be re-processed on restart. For `comment.created`,
this is safe because `INSERT INTO dm_attempts ON CONFLICT DO NOTHING` is
idempotent — re-processing doesn't create duplicate DM attempts.

---

## 5. Rate limiter state resets on process restart

**Trigger**: The process crashes or restarts mid-burst (e.g. during the 500-
event / 10-second load test). The in-process token bucket is reset to full (10
tokens) on startup.

**Consequence**: If the process restarts during a burst, it may immediately
send up to 10 DMs without waiting for the 60-second window to elapse from the
last batch. This can temporarily breach the 10 req/60s rate limit if the mock
API's window hasn't reset. The mock API will return `429`, which the dm_sender
handles correctly by backing off — so no DMs are lost, but the rate limit
*technically* may be exceeded for one cycle before the 429 is received and
respected.

**Why not fixed**: Persisting token-bucket state to the DB on every acquire
call would add a DB write per DM send — expensive and complex. The `429`
handler is the recovery mechanism. This is documented as a "rate-limit exposure
window" rather than a correctness failure.

---

## Architectural tradeoffs (not failures, but explicit choices)

- **No Redis / Celery**: The volume (500 events/10s, single API key, 10 DMs/60s)
  does not justify a separate broker. The `asyncio.Queue` + DB-persisted rows
  achieve the same durability guarantee for this scale. Adding Celery would
  introduce operational complexity (separate worker process, broker state, retry
  semantics overlap with our own) with no correctness benefit at this volume.

- **Single uvicorn worker**: The token-bucket rate limiter lives in process
  memory. Multiple workers would each maintain independent buckets, multiplying
  the effective rate limit by the number of workers. The deploy config enforces
  `--workers 1`. If horizontal scaling is needed, the limiter must move to Redis.

- **SQLite for local dev, Postgres on Render**: SQLite's WAL mode provides
  sufficient read/write concurrency for a single-process server. The
  `DATABASE_URL` env var is the only thing that changes between environments.
