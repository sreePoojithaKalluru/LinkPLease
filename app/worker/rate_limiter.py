"""
app/worker/rate_limiter.py
───────────────────────────
Token-bucket rate limiter: 10 requests / 60 seconds.

Design notes (read before touching this):

- This is a PROCESS-LEVEL singleton. It works correctly only because we run
  a single uvicorn worker process. If you ever add --workers N, you must move
  the limiter state into Redis or Postgres — in-memory is no longer safe.

- acquire() is the ONLY entry point for the dm_sender loop. It either returns
  immediately (token available) or sleeps until a token is available.

- The limiter also exposes set_retry_after() so the 429 handler can tell it
  "don't touch the API until at least T seconds from now". This overrides the
  normal refill schedule for that window.

- We do NOT count reads (GET /v1/dm/{dm_id}) against this limit because the
  spec says reads are free.
"""
import asyncio
import logging
import time

from app.config import settings

logger = logging.getLogger(__name__)


class TokenBucketLimiter:
    """
    Classic token-bucket:
    - capacity = settings.rate_limit_requests (10)
    - refill rate = capacity tokens per settings.rate_limit_window_seconds (60s)
    - Each acquire() costs one token.
    - If the bucket is empty, acquire() sleeps until refill time.
    """

    def __init__(self, capacity: int, window_seconds: float) -> None:
        self._capacity = capacity
        self._window = window_seconds
        self._tokens = float(capacity)          # start full
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        # Absolute monotonic time before which we must not send anything.
        # Set by set_retry_after() when we receive a 429.
        self._blocked_until: float = 0.0

    async def acquire(self) -> None:
        """
        Wait until a token is available, then consume one token.
        Callers must await this before every POST /v1/dm/send call.
        """
        async with self._lock:
            # 1. Honour any global Retry-After block first.
            now = time.monotonic()
            if now < self._blocked_until:
                wait = self._blocked_until - now
                logger.info("Rate limiter: blocked by Retry-After, sleeping %.1fs", wait)
                await asyncio.sleep(wait)

            # 2. Refill tokens based on elapsed time.
            #    We refill ALL capacity at once after one full window (burst model).
            #    This matches a simple sliding-window: at most 10 calls per 60 s.
            now = time.monotonic()
            elapsed = now - self._last_refill
            if elapsed >= self._window:
                self._tokens = float(self._capacity)
                self._last_refill = now
                logger.debug("Rate limiter: refilled to %d tokens", self._capacity)

            # 3. If no token available, sleep until the next refill window.
            if self._tokens < 1:
                sleep_for = self._window - (time.monotonic() - self._last_refill)
                if sleep_for > 0:
                    logger.info("Rate limiter: bucket empty, sleeping %.1fs", sleep_for)
                    await asyncio.sleep(sleep_for)
                # After sleeping, refill.
                self._tokens = float(self._capacity)
                self._last_refill = time.monotonic()

            # 4. Consume one token.
            self._tokens -= 1
            logger.debug(
                "Rate limiter: token consumed, %.0f remaining", self._tokens
            )

    async def set_retry_after(self, retry_after_seconds: float) -> None:
        """
        Called when the mock API returns 429. Blocks all sends for the
        specified duration. Uses the lock so concurrent acquires see the update.
        """
        async with self._lock:
            blocked_until = time.monotonic() + retry_after_seconds
            if blocked_until > self._blocked_until:
                self._blocked_until = blocked_until
                logger.warning(
                    "Rate limiter: 429 received — blocking sends for %.1fs",
                    retry_after_seconds,
                )


# Process-level singleton — imported by dm_sender.
rate_limiter = TokenBucketLimiter(
    capacity=settings.rate_limit_requests,
    window_seconds=settings.rate_limit_window_seconds,
)
