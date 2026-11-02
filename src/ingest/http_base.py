# Origin: original
"""Shared async HTTP machinery for the HN Firebase, Algolia and Remotive clients.

- aiohttp session with a pooled TCPConnector(limit=50, ttl_dns_cache=300)
- asyncio.Semaphore bounding in-flight requests
- token-bucket rate limiter (requests per second)
- retries on 429/5xx/timeouts: sleep = min(16, 0.5 * 2**attempt) + U(0, 1), 5 attempts,
  honouring Retry-After when the server sends it
- optional on-disk JSON cache so re-runs do not refetch
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
from pathlib import Path
from typing import Any

import aiohttp

from src.common import config
from src.common.log import get_logger

log = get_logger(__name__)

RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5


class TokenBucket:
    """Classic token bucket: `rate` tokens/s, burst up to `capacity`."""

    def __init__(self, rate: float, capacity: float | None = None):
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else max(1.0, rate))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / self.rate)


def backoff_seconds(attempt: int) -> float:
    return min(16.0, 0.5 * 2 ** attempt) + random.uniform(0, 1)


class HTTPError(RuntimeError):
    def __init__(self, url: str, status: int | None, message: str):
        super().__init__(f"{message} ({status}) for {url}")
        self.url, self.status = url, status


class AsyncJSONClient:
    def __init__(self, rate: float, concurrency: int = 50, timeout_s: float = 30.0,
                 cache_dir: Path | None = None):
        self.bucket = TokenBucket(rate)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.cache_dir = cache_dir
        self.session: aiohttp.ClientSession | None = None
        self.stats = {"requests": 0, "cache_hits": 0, "retries": 0, "failures": 0}

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=self.timeout,
            headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"})
        return self

    async def __aexit__(self, *exc):
        if self.session is not None:
            await self.session.close()
            self.session = None

    # ---- cache -------------------------------------------------------------------------------
    def cache_path(self, key: str) -> Path | None:
        if self.cache_dir is None:
            return None
        digest = hashlib.sha1(key.encode()).hexdigest()
        return self.cache_dir / digest[:2] / f"{digest}.json"

    def _cache_read(self, path: Path | None):
        if path is None or not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def _cache_write(path: Path | None, payload) -> None:
        if path is None or payload is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, path)

    # ---- request -----------------------------------------------------------------------------
    async def get_json(self, url: str, params: dict | None = None, *, use_cache: bool = True,
                       cache_key: str | None = None, cache_path: Path | None = None) -> Any:
        assert self.session is not None, "use `async with`"
        key = cache_key or (url + "?" + "&".join(f"{k}={v}" for k, v in sorted((params or {}).items())))
        path = cache_path if cache_path is not None else self.cache_path(key)
        if use_cache:
            cached = self._cache_read(path)
            if cached is not None:
                self.stats["cache_hits"] += 1
                return cached

        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            await self.bucket.acquire()
            try:
                async with self.semaphore:
                    self.stats["requests"] += 1
                    async with self.session.get(url, params=params) as resp:
                        if resp.status in RETRY_STATUSES:
                            retry_after = resp.headers.get("Retry-After")
                            delay = backoff_seconds(attempt)
                            if retry_after:
                                try:
                                    delay = max(delay, float(retry_after))
                                except ValueError:
                                    pass
                            last_error = HTTPError(url, resp.status, "retryable status")
                        elif resp.status >= 400:
                            raise HTTPError(url, resp.status, "client error")
                        else:
                            payload = await resp.json(content_type=None)
                            self._cache_write(path, payload)
                            return payload
            except (asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as exc:
                last_error = exc
                delay = backoff_seconds(attempt)
            if attempt < MAX_ATTEMPTS - 1:
                self.stats["retries"] += 1
                log.warning("retry %d/%d for %s after %s (sleep %.1fs)",
                            attempt + 1, MAX_ATTEMPTS - 1, url, last_error, delay)
                await asyncio.sleep(delay)
        self.stats["failures"] += 1
        raise HTTPError(url, getattr(last_error, "status", None), f"gave up after {MAX_ATTEMPTS} attempts")
