# Origin: original
"""Raw-lake paths and atomic file writes."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from src.common import config


def raw_partition_dir(source: str, year: int, month: int, raw_dir: Path | None = None) -> Path:
    base = raw_dir or config.RAW_DIR
    return base / f"source={source}" / f"year={year:04d}" / f"month={month:02d}"


def raw_partition_file(source: str, year: int, month: int, raw_dir: Path | None = None) -> Path:
    return raw_partition_dir(source, year, month, raw_dir) / "part-0.jsonl"


def stats_file(source: str, year: int, month: int, raw_dir: Path | None = None) -> Path:
    # Leading underscore: Spark's file listing skips it, so it never pollutes the dataset.
    return raw_partition_dir(source, year, month, raw_dir) / "_stats.json"


def _atomic_replace(path: Path, write) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Hidden temp name in the same directory so the rename is atomic and readers never see it.
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            write(fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def atomic_write_jsonl(path: Path, records: Iterable[dict]) -> int:
    count = 0

    def write(fh):
        nonlocal count
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")
            count += 1

    _atomic_replace(path, write)
    return count


def atomic_write_json(path: Path, obj) -> None:
    _atomic_replace(path, lambda fh: json.dump(obj, fh, indent=2, sort_keys=True, default=str))


def parse_month(value: str) -> tuple[int, int]:
    year, month = value.split("-")
    y, m = int(year), int(month)
    if not (1 <= m <= 12):
        raise ValueError(f"bad month {value!r}")
    return y, m


def month_range(start: str, end: str) -> list[tuple[int, int]]:
    y, m = parse_month(start)
    ey, em = parse_month(end)
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def prev_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)
