# Origin: original
"""Historical backfill via Algolia.

    python -m src.ingest.backfill --from 2011-01 --to 2026-10 [--refresh]

Finds every month's thread, writes its raw partition, runs the quality gate, and writes
results/ingest/backfill_summary.json with real per-month and total counts.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt

from src.common import config
from src.common.log import get_logger
from src.common.paths import atomic_write_json, month_range
from src.ingest import extract, quality_gate, thread_finder
from src.ingest.algolia_client import AlgoliaClient

log = get_logger("backfill")


MONTH_CONCURRENCY = 4  # months fetched at once; the shared token bucket still caps requests/s


async def run(start: str, end: str, refresh: bool, summary_name: str = "backfill_summary.json") -> dict:
    months = month_range(start, end)
    rows: dict[tuple[int, int], dict] = {}
    async with AlgoliaClient() as client:
        threads = await thread_finder.find_threads(months, client, use_cache=False)
        sem = asyncio.Semaphore(MONTH_CONCURRENCY)

        async def fetch(y: int, m: int):
            thread = threads[(y, m)]
            row = {"month": f"{y:04d}-{m:02d}", "story_id": None, "title": None,
                   "fetched": 0, "kept": 0, "gate": "skipped_no_thread"}
            if thread is not None:
                async with sem:
                    res = await extract.extract_hn_algolia_async(client, y, m, thread, use_cache=not refresh)
                row.update(story_id=thread["story_id"], title=thread["title"], author=thread["author"],
                           fetched=res["fetched"], kept=res["kept"])
            rows[(y, m)] = row
            log.info("%s: %s", row["month"], {k: row[k] for k in ("story_id", "fetched", "kept")})

        await asyncio.gather(*(fetch(y, m) for y, m in months))
        http = dict(client.stats)
    # gate in calendar order, so each month is compared with the previous month's partition
    per_month = []
    for (y, m) in months:
        row = rows[(y, m)]
        if row["story_id"]:
            gate = quality_gate.validate_month(y, m, {"hn": True}, raise_on_error=False)
            row.update(gate=gate["status"], warnings=[w for r in gate["sources"].values() for w in r["warnings"]])
        per_month.append(row)
    summary = {
        "from": start, "to": end,
        "run_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "months_requested": len(months),
        "months_with_thread": sum(1 for r in per_month if r["story_id"]),
        "months_missing_thread": [r["month"] for r in per_month if not r["story_id"]],
        "months_failed_gate": [r["month"] for r in per_month if r["gate"] == "failed"],
        "total_comments_fetched": sum(r["fetched"] or 0 for r in per_month),
        "total_postings_kept": sum(r["kept"] or 0 for r in per_month),
        "http": http,
        "per_month": per_month,
    }
    atomic_write_json(config.RESULTS_DIR / "ingest" / summary_name, summary)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", required=True, help="YYYY-MM")
    ap.add_argument("--to", dest="end", required=True, help="YYYY-MM")
    ap.add_argument("--refresh", action="store_true", help="ignore the Algolia response cache")
    ap.add_argument("--summary-name", default="backfill_summary.json",
                    help="file name under results/ingest/ (the memory profile uses its own)")
    args = ap.parse_args()
    s = asyncio.run(run(args.start, args.end, args.refresh, args.summary_name))
    print(f"months with thread: {s['months_with_thread']}/{s['months_requested']}  "
          f"comments fetched: {s['total_comments_fetched']}  postings kept: {s['total_postings_kept']}  "
          f"missing: {s['months_missing_thread']}  failed gate: {s['months_failed_gate']}")


if __name__ == "__main__":
    main()
