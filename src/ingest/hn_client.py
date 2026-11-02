# Origin: original
"""Async HN Firebase client: fetch a "Who is hiring?" story and its top-level comments.

Item cache: data/cache/hn_items/<id % 1000>/<id>.json (keyed by item id). Stories are always
refetched because their `kids` list grows during the month.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path

from src.common import config
from src.common.log import get_logger
from src.ingest.http_base import AsyncJSONClient

log = get_logger(__name__)


class HNClient(AsyncJSONClient):
    def __init__(self, rate: float | None = None, concurrency: int = 50,
                 cache_dir: Path | None = None):
        super().__init__(rate=rate or config.FIREBASE_RATE, concurrency=concurrency,
                         cache_dir=cache_dir or (config.CACHE_DIR / "hn_items"))

    def item_cache_path(self, item_id: int) -> Path:
        return self.cache_dir / f"{item_id % 1000:03d}" / f"{item_id}.json"

    async def get_item(self, item_id: int, use_cache: bool = True) -> dict | None:
        url = f"{config.FIREBASE_BASE}/item/{item_id}.json"
        return await self.get_json(url, use_cache=use_cache, cache_path=self.item_cache_path(item_id))

    async def get_items(self, ids: list[int]) -> list[dict | None]:
        async def one(i):
            try:
                return await self.get_item(i)
            except Exception as exc:  # noqa: BLE001 - counted as a failed fetch, not fatal
                log.error("item %s failed: %s", i, exc)
                return {"id": i, "_fetch_failed": True}
        return await asyncio.gather(*(one(i) for i in ids))


def to_record(item: dict, story_id: int, thread_month: str) -> dict:
    return {
        "id": int(item["id"]),
        "thread_id": int(story_id),
        "thread_month": thread_month,
        "parent_id": int(item.get("parent", story_id)),
        "author": item.get("by"),
        "created_at_i": int(item.get("time", 0)),
        "text": item.get("text"),
        "api": "firebase",
    }


async def fetch_thread_postings(story_id: int, thread_month: str,
                                client: HNClient | None = None) -> tuple[list[dict], dict]:
    """Return (records, stats) for every live top-level comment of `story_id`."""
    own = client is None
    client = client or HNClient()
    if own:
        await client.__aenter__()
    try:
        story = await client.get_item(story_id, use_cache=False)
        if not story:
            raise RuntimeError(f"story {story_id} not found")
        kids = [int(k) for k in story.get("kids", [])]
        items = await client.get_items(kids)
        reasons: Counter = Counter()
        records = []
        for item in items:
            if item is None:
                reasons["null_item"] += 1
            elif item.get("_fetch_failed"):
                reasons["fetch_failed"] += 1
            elif item.get("deleted"):
                reasons["deleted"] += 1
            elif item.get("dead"):
                reasons["dead"] += 1
            elif not item.get("text"):
                reasons["no_text"] += 1
            elif int(item.get("parent", -1)) != story_id:
                reasons["not_top_level"] += 1
            else:
                records.append(to_record(item, story_id, thread_month))
        stats = {
            "api": "firebase",
            "story_id": story_id,
            "fetched": len(kids),
            "kept": len(records),
            "dropped": int(sum(reasons.values())),
            "dropped_by_reason": dict(reasons),
            "http": dict(client.stats),
        }
        log.info("thread %s (%s): fetched %d kids, kept %d, dropped %s",
                 story_id, thread_month, len(kids), len(records), dict(reasons))
        return records, stats
    finally:
        if own:
            await client.__aexit__(None, None, None)
