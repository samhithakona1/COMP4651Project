# Origin: original
"""Write one month of raw postings to the lake: partition file + `_stats.json` sidecar.

Used by both the Airflow DAG (Firebase, current month) and the backfill CLI (Algolia, history).
Each call replaces exactly one partition, so re-runs are idempotent.
"""
from __future__ import annotations

import asyncio
import datetime as dt

from src.common.log import get_logger
from src.common.paths import atomic_write_json, atomic_write_jsonl, raw_partition_file, stats_file
from src.ingest import algolia_client, hn_client, remotive_client

log = get_logger(__name__)


def _write(source: str, year: int, month: int, records: list[dict], stats: dict) -> dict:
    path = raw_partition_file(source, year, month)
    records = sorted(records, key=lambda r: r["id"])
    n = atomic_write_jsonl(path, records)
    stats = {**stats, "month": f"{year:04d}-{month:02d}", "written": n,
             "extracted_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    atomic_write_json(stats_file(source, year, month), stats)
    log.info("wrote %d %s records to %s", n, source, path)
    return {"path": str(path), "fetched": stats.get("fetched"), "kept": n}


def extract_hn_firebase(year: int, month: int, story_id: int) -> dict:
    records, stats = asyncio.run(hn_client.fetch_thread_postings(story_id, f"{year:04d}-{month:02d}"))
    return _write("hn", year, month, records, stats)


async def extract_hn_algolia_async(client: algolia_client.AlgoliaClient, year: int, month: int,
                                   thread: dict, use_cache: bool = True) -> dict:
    records, stats = await algolia_client.fetch_thread_postings(
        client, thread["story_id"], thread["created_at_i"], f"{year:04d}-{month:02d}", use_cache=use_cache)
    stats["thread_title"] = thread["title"]
    stats["thread_author"] = thread["author"]
    return _write("hn", year, month, records, stats)


def extract_remotive(year: int, month: int) -> dict:
    records, stats = remotive_client.snapshot_records(f"{year:04d}-{month:02d}")
    return _write("remotive", year, month, records, stats)
