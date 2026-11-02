# Origin: original
"""Algolia HN Search client for thread discovery and the historical backfill.

Algolia lets one query page through at most ~1000 hits. A thread with more comments than that is
split recursively into created_at_i windows (numericFilters) until every window fits.
"""
from __future__ import annotations

from pathlib import Path

from src.common import config
from src.common.log import get_logger
from src.ingest.http_base import AsyncJSONClient

log = get_logger(__name__)

HITS_PER_PAGE = 1000
PAGINATION_CAP = 1000
# Fixed upper bound (2100-01-01) so window queries, and therefore cache keys, are deterministic.
FAR_FUTURE_I = 4102444800


class AlgoliaClient(AsyncJSONClient):
    def __init__(self, rate: float | None = None, concurrency: int = 10,
                 cache_dir: Path | None = None):
        super().__init__(rate=rate or config.ALGOLIA_RATE, concurrency=concurrency,
                         cache_dir=cache_dir or (config.CACHE_DIR / "algolia"))

    async def search(self, endpoint: str, params: dict, use_cache: bool = True) -> dict:
        return await self.get_json(f"{config.ALGOLIA_BASE}/{endpoint}", params=params,
                                   use_cache=use_cache)

    async def _window(self, story_id: int, lo: int, hi: int, use_cache: bool) -> list[dict]:
        """All comment hits of `story_id` with lo <= created_at_i < hi."""
        params = {
            "tags": f"comment,story_{story_id}",
            "hitsPerPage": HITS_PER_PAGE,
            "numericFilters": f"created_at_i>={lo},created_at_i<{hi}",
            "page": 0,
        }
        first = await self.search("search_by_date", params, use_cache=use_cache)
        nb = int(first.get("nbHits", 0))
        if nb > PAGINATION_CAP and hi - lo > 1:
            mid = lo + (hi - lo) // 2
            log.info("story %s window [%d,%d) has %d hits > cap; splitting", story_id, lo, hi, nb)
            return (await self._window(story_id, lo, mid, use_cache)
                    + await self._window(story_id, mid, hi, use_cache))
        hits = list(first.get("hits", []))
        for page in range(1, int(first.get("nbPages", 1))):
            nxt = await self.search("search_by_date", {**params, "page": page}, use_cache=use_cache)
            hits.extend(nxt.get("hits", []))
        return hits

    async def thread_comments(self, story_id: int, story_created_i: int,
                              use_cache: bool = True) -> list[dict]:
        # Most comments land in the first days; start from windows shaped like that so a large
        # thread needs few splits instead of halving down from the far-future bound.
        day = 86400
        start = int(story_created_i) - 1
        edges = [start, start + 2 * day, start + 7 * day, start + 35 * day, FAR_FUTURE_I]
        hits = []
        for lo, hi in zip(edges, edges[1:]):
            hits += await self._window(story_id, lo, hi, use_cache)
        uniq = {h["objectID"]: h for h in hits}
        return list(uniq.values())


def to_record(hit: dict, story_id: int, thread_month: str) -> dict:
    return {
        "id": int(hit["objectID"]),
        "thread_id": int(story_id),
        "thread_month": thread_month,
        "parent_id": int(hit["parent_id"]) if hit.get("parent_id") is not None else None,
        "author": hit.get("author"),
        "created_at_i": int(hit.get("created_at_i") or 0),
        "text": hit.get("comment_text"),
        "api": "algolia",
    }


async def fetch_thread_postings(client: AlgoliaClient, story_id: int, story_created_i: int,
                                thread_month: str, use_cache: bool = True) -> tuple[list[dict], dict]:
    hits = await client.thread_comments(story_id, story_created_i, use_cache=use_cache)
    top = [h for h in hits if h.get("parent_id") is not None and int(h["parent_id"]) == story_id]
    records = [to_record(h, story_id, thread_month) for h in top if h.get("comment_text")]
    stats = {
        "api": "algolia",
        "story_id": story_id,
        "fetched": len(hits),                 # every comment in the thread, replies included
        "top_level": len(top),
        "kept": len(records),
        "dropped": len(hits) - len(records),
        "dropped_by_reason": {"reply": len(hits) - len(top), "no_text": len(top) - len(records)},
    }
    return records, stats
