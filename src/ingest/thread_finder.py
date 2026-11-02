# Origin: original
"""Map a calendar month to its "Ask HN: Who is hiring? (Month YYYY)" story.

Lookup order:
 1. stories by `whoishiring` (one search_by_date request lists all ~500 of them),
 2. stories by `_whoishiring` (April and May 2015 were posted from that account),
 3. a full-text search whose title must match the exact pattern and that has >= 20 comments
    (e.g. July 2011, posted by a regular user).
The exact title regex excludes "Who wants to be hired?", "Freelancer?" and "Who is hiring
remote workers?" threads.
"""
from __future__ import annotations

import asyncio
import calendar
import re

from src.common.log import get_logger
from src.ingest.algolia_client import AlgoliaClient

log = get_logger(__name__)

TITLE_RE = re.compile(r"^\s*Ask HN:\s*Who is hiring\?\s*\(\s*([A-Za-z]+)\s+(\d{4})\s*\)\s*$", re.I)
MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
MIN_FALLBACK_COMMENTS = 20


def parse_title(title: str | None) -> tuple[int, int] | None:
    m = TITLE_RE.match(title or "")
    if not m or m.group(1).lower() not in MONTHS:
        return None
    return int(m.group(2)), MONTHS[m.group(1).lower()]


def _thread(hit: dict) -> dict:
    return {
        "story_id": int(hit["objectID"]),
        "title": hit["title"].strip(),
        "author": hit.get("author"),
        "num_comments": int(hit.get("num_comments") or 0),
        "created_at_i": int(hit.get("created_at_i") or 0),
    }


async def _by_author(client: AlgoliaClient, author: str, use_cache: bool) -> dict[tuple[int, int], dict]:
    found: dict[tuple[int, int], dict] = {}
    page, pages = 0, 1
    while page < pages:
        res = await client.search("search_by_date",
                                  {"tags": f"story,author_{author}", "hitsPerPage": 1000, "page": page},
                                  use_cache=use_cache)
        pages = int(res.get("nbPages", 1))
        for hit in res.get("hits", []):
            ym = parse_title(hit.get("title"))
            if ym is None:
                continue
            cand = _thread(hit)
            if ym not in found or cand["num_comments"] > found[ym]["num_comments"]:
                found[ym] = cand
        page += 1
    return found


async def _fallback(client: AlgoliaClient, year: int, month: int, use_cache: bool) -> dict | None:
    name = calendar.month_name[month]
    res = await client.search("search", {"query": f'"Who is hiring" {name} {year}', "tags": "story",
                                         "hitsPerPage": 50}, use_cache=use_cache)
    cands = [_thread(h) for h in res.get("hits", []) if parse_title(h.get("title")) == (year, month)]
    cands = [c for c in cands if c["num_comments"] >= MIN_FALLBACK_COMMENTS]
    return max(cands, key=lambda c: c["num_comments"]) if cands else None


async def find_threads(months: list[tuple[int, int]], client: AlgoliaClient,
                       use_cache: bool = False) -> dict[tuple[int, int], dict | None]:
    """Story metadata per month; None where no qualifying thread exists."""
    primary = await _by_author(client, "whoishiring", use_cache)
    secondary = await _by_author(client, "_whoishiring", use_cache)
    out: dict[tuple[int, int], dict | None] = {}
    for ym in months:
        thread = primary.get(ym) or secondary.get(ym)
        if thread is None:
            thread = await _fallback(client, *ym, use_cache=use_cache)
        if thread is None:
            log.warning("no 'Who is hiring?' thread found for %04d-%02d", *ym)
        out[ym] = thread
    return out


async def find_thread_async(year: int, month: int, use_cache: bool = False) -> dict | None:
    async with AlgoliaClient() as client:
        return (await find_threads([(year, month)], client, use_cache=use_cache))[(year, month)]


def find_thread(year: int, month: int, use_cache: bool = False) -> dict | None:
    return asyncio.run(find_thread_async(year, month, use_cache=use_cache))
