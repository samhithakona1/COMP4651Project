# Origin: original
"""Quality gate for one raw-lake month.

Fails (raises QualityGateError) when a required partition file is missing or empty, or when
malformed rows (bad JSON, missing `id`, missing/empty `text`) exceed `max_malformed` (5%).
Warns when the row count differs from the previous month by more than `max_delta` (60%).
Writes results/ingest/<year>-<month>.json with fetched / kept / dropped / malformed counts.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from src.common import config
from src.common.log import get_logger
from src.common.paths import atomic_write_json, prev_month, raw_partition_file, stats_file

log = get_logger(__name__)

MAX_MALFORMED = 0.05
MAX_DELTA = 0.60


class QualityGateError(RuntimeError):
    pass


def scan_file(path: Path) -> dict:
    rows = malformed = 0
    reasons = {"bad_json": 0, "missing_id": 0, "missing_text": 0}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            rows += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                reasons["bad_json"] += 1
                malformed += 1
                continue
            if not isinstance(rec, dict) or rec.get("id") in (None, ""):
                reasons["missing_id"] += 1
                malformed += 1
            elif not (isinstance(rec.get("text"), str) and rec["text"].strip()):
                reasons["missing_text"] += 1
                malformed += 1
    return {"rows": rows, "malformed": malformed, "malformed_by_reason": reasons}


def _line_count(path: Path) -> int | None:
    if not path.exists():
        return None
    with path.open("rb") as fh:
        return sum(1 for line in fh if line.strip())


def check_source(source: str, year: int, month: int, *, required: bool = True,
                 raw_dir: Path | None = None, max_malformed: float = MAX_MALFORMED,
                 max_delta: float = MAX_DELTA) -> dict:
    path = raw_partition_file(source, year, month, raw_dir)
    report: dict = {"source": source, "path": str(path), "required": required,
                    "errors": [], "warnings": []}
    sidecar = stats_file(source, year, month, raw_dir)
    if sidecar.exists():
        extract = json.loads(sidecar.read_text())
        report.update({k: extract.get(k) for k in ("fetched", "kept", "dropped", "dropped_by_reason", "api",
                                                    "story_id")})
    if not path.exists():
        report["errors"].append("partition file missing")
        return report
    scan = scan_file(path)
    report.update(scan)
    if scan["rows"] == 0:
        report["errors"].append("partition file empty")
        return report
    rate = scan["malformed"] / scan["rows"]
    report["malformed_rate"] = round(rate, 6)
    if rate > max_malformed:
        report["errors"].append(f"malformed rate {rate:.2%} > {max_malformed:.0%}")

    py, pm = prev_month(year, month)
    prev = _line_count(raw_partition_file(source, py, pm, raw_dir))
    report["previous_month_rows"] = prev
    if prev:
        delta = abs(scan["rows"] - prev) / prev
        report["delta_vs_previous"] = round(delta, 4)
        if delta > max_delta:
            report["warnings"].append(
                f"row count {scan['rows']} differs from previous month ({prev}) by {delta:.0%} > {max_delta:.0%}")
    return report


def validate_month(year: int, month: int, sources: dict[str, bool] | None = None, *,
                   raw_dir: Path | None = None, results_dir: Path | None = None,
                   raise_on_error: bool = True) -> dict:
    """`sources` maps source name -> required. Optional sources only produce warnings."""
    sources = sources or {"hn": True}
    reports = {s: check_source(s, year, month, required=req, raw_dir=raw_dir) for s, req in sources.items()}
    failures = []
    for s, rep in reports.items():
        for w in rep["warnings"]:
            log.warning("[%s %04d-%02d] %s", s, year, month, w)
        if rep["errors"]:
            if rep["required"]:
                failures.extend(f"{s}: {e}" for e in rep["errors"])
            else:
                rep["warnings"].extend(f"optional source: {e}" for e in rep["errors"])
                log.warning("[%s %04d-%02d] optional source problems: %s", s, year, month, rep["errors"])
    summary = {
        "month": f"{year:04d}-{month:02d}",
        "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "status": "failed" if failures else "passed",
        "failures": failures,
        "thresholds": {"max_malformed": MAX_MALFORMED, "max_delta": MAX_DELTA},
        "sources": reports,
    }
    out = (results_dir or config.RESULTS_DIR) / "ingest" / f"{year:04d}-{month:02d}.json"
    atomic_write_json(out, summary)
    log.info("quality gate %04d-%02d: %s %s", year, month, summary["status"],
             {s: r.get("rows") for s, r in reports.items()})
    if failures and raise_on_error:
        raise QualityGateError("; ".join(failures))
    return summary
