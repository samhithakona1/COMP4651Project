# Origin: original
"""Remotive remote-jobs snapshot (sanity check for remote pay only, not the main corpus).

Remotive's API terms ask for infrequent calls (no more than a few per day), attribution with a link
back to Remotive, and note that listings are delayed. This client therefore fetches at most once
per UTC day: the response is cached as data/cache/remotive/<YYYY-MM-DD>.json and reused.
Attribution: "Job data from Remotive (https://remotive.com)" is shown on the dashboard.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from src.common import config
from src.common.log import get_logger
from src.ingest.http_base import AsyncJSONClient

log = get_logger(__name__)

ATTRIBUTION = "Job data from Remotive (https://remotive.com)"


def _cache_file(day: dt.date, category: str, cache_dir: Path | None = None) -> Path:
    return (cache_dir or config.CACHE_DIR / "remotive") / f"{day.isoformat()}_{category or 'all'}.json"


async def fetch_snapshot(day: dt.date | None = None, cache_dir: Path | None = None,
                         category: str | None = None) -> dict:
    """One snapshot per UTC day and category. No API key is needed; `category` is a plain filter."""
    day = day or dt.datetime.now(dt.timezone.utc).date()
    category = config.REMOTIVE_CATEGORY if category is None else category
    path = _cache_file(day, category, cache_dir)
    params = {"category": category} if category else None
    async with AsyncJSONClient(rate=1, concurrency=1, timeout_s=60) as client:
        payload = await client.get_json(config.REMOTIVE_URL, params=params, cache_path=path)
    log.info("remotive snapshot %s: %d jobs (cache hits=%d)", day,
             len(payload.get("jobs", [])), client.stats["cache_hits"])
    return payload


def _epoch(value: str | None) -> int:
    if not value:
        return 0
    try:
        ts = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return int(ts.timestamp())


def to_record(job: dict, thread_month: str) -> dict:
    """Map a Remotive job onto the raw-lake record shape.

    `text` gets an HN-style header line (company | title | location | REMOTE | salary) followed by
    the HTML description, so the same Spark parsers handle both sources.
    """
    header = " | ".join(str(x).strip() for x in (
        job.get("company_name"), job.get("title"), job.get("candidate_required_location"),
        "REMOTE", job.get("salary")) if x)
    description = job.get("description") or ""
    return {
        "id": int(job["id"]),
        "thread_id": None,
        "thread_month": thread_month,
        "parent_id": None,
        "author": None,
        "created_at_i": _epoch(job.get("publication_date")),
        "text": f"{header}<p>{description}" if description else None,
        "api": "remotive",
        "title": job.get("title"),
        "company_name": job.get("company_name"),
        "category": job.get("category"),
        "job_type": job.get("job_type"),
        "candidate_required_location": job.get("candidate_required_location"),
        "salary_text": job.get("salary"),
        "tags": job.get("tags") or [],
        "url": job.get("url"),
    }


def snapshot_records(thread_month: str, day: dt.date | None = None) -> tuple[list[dict], dict]:
    payload = asyncio.run(fetch_snapshot(day))
    jobs = payload.get("jobs", [])
    records = [to_record(j, thread_month) for j in jobs if j.get("id") is not None]
    kept = [r for r in records if r["text"]]
    stats = {"api": "remotive", "fetched": len(jobs), "kept": len(kept),
             "dropped": len(jobs) - len(kept), "dropped_by_reason": {"no_description": len(records) - len(kept),
                                                                    "no_id": len(jobs) - len(records)},
             "attribution": ATTRIBUTION}
    return kept, stats
