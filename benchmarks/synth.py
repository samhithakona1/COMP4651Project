# Origin: original
"""Synthetic scale points for the benchmarks (labelled synthetic everywhere).

Resamples the real HN postings *with replacement* (seed 4651) to 10k / 50k / 200k / 1M rows and
assigns new ids, keeping the joint distribution of text, skills and salary:
  data/synthetic/n=<N>/raw/source=hn/year=Y/month=M/part-0.jsonl   (Benchmark 1 input)
  data/synthetic/n=<N>/processed/<table>/part-0.parquet            (Benchmark 2 star schema)
The real corpus itself is the "real" scale point (is_synthetic = false).

    python -m benchmarks.synth [--scales 10000,50000,200000,1000000] [--list-scales]
"""
from __future__ import annotations

import argparse
import glob
import json
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.common import config
from src.common.log import get_logger
from src.common.paths import atomic_write_json

log = get_logger("synth")
SEED = 4651
DEFAULT_SCALES = [10_000, 50_000, 200_000, 1_000_000]
# Synthetic rows live only in their own bench_* schemas, so new ids never meet real (xxhash) ids.
SYNTH_ID_BASE = 10 ** 15
DIM_TABLES = ["dim_date", "dim_skills", "dim_companies", "dim_locations"]


def scale_label(n: int | str) -> str:
    return "real" if n == "real" else f"{int(n) // 1000}k" if int(n) < 1_000_000 else f"{int(n) // 1_000_000}m"


def scale_dirs(label: str) -> tuple[Path, Path, bool]:
    """(raw_dir, processed_dir, is_synthetic) for a scale label."""
    if label == "real":
        return config.RAW_DIR, config.PROCESSED_DIR, False
    root = config.SYNTH_DIR / f"n={label}"
    return root / "raw", root / "processed", True


def all_labels(scales=DEFAULT_SCALES) -> list[str]:
    real_n = real_count()
    labels = [(n, scale_label(n)) for n in scales] + [(real_n, "real")]
    return [lbl for _, lbl in sorted(labels)]


def real_raw_files() -> list[str]:
    return sorted(glob.glob(str(config.RAW_DIR / "source=hn" / "year=*" / "month=*" / "part-0.jsonl")))


def real_count() -> int:
    n = 0
    for f in real_raw_files():
        with open(f, "rb") as fh:
            n += sum(1 for _ in fh)
    return n


def synth_raw(n: int, out: Path, rng: np.random.Generator) -> np.ndarray:
    lines, parts = [], []
    for f in real_raw_files():
        p = Path(f).parent
        y, m = int(p.parent.name.split("=")[1]), int(p.name.split("=")[1])
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                lines.append(line)
                parts.append((y, m))
    idx = rng.integers(0, len(lines), size=n)
    groups: dict[tuple[int, int], list[str]] = defaultdict(list)
    for new_i, src in enumerate(idx):
        rec = json.loads(lines[src])
        rec["source_id"] = rec["id"]
        rec["id"] = SYNTH_ID_BASE + new_i
        groups[parts[src]].append(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    if out.exists():
        shutil.rmtree(out)
    for (y, m), recs in groups.items():
        d = out / "source=hn" / f"year={y:04d}" / f"month={m:02d}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "part-0.jsonl").write_text("\n".join(recs) + "\n", encoding="utf-8")
    return idx


def synth_star(n: int, out: Path, rng: np.random.Generator) -> dict:
    proc = config.PROCESSED_DIR
    fact = ds.dataset(str(proc / "fact_job_postings"), format="parquet").to_table()
    fact = fact.filter(pc.equal(fact["source_type"], "hn"))
    bridge = ds.dataset(str(proc / "bridge_job_skills"), format="parquet").to_table()
    idx = rng.integers(0, fact.num_rows, size=n)
    new_ids = np.arange(n, dtype=np.int64) + SYNTH_ID_BASE
    sampled = fact.take(pa.array(idx))
    old_ids = sampled["posting_id"].to_numpy()
    sampled = (sampled.set_column(sampled.schema.get_field_index("posting_id"), "posting_id", pa.array(new_ids))
               .set_column(sampled.schema.get_field_index("external_id"), "external_id",
                           pa.array([f"synth-{i}" for i in range(n)]))
               .set_column(sampled.schema.get_field_index("is_synthetic"), "is_synthetic", pa.array(np.ones(n, bool))))
    # replicate each sampled posting's bridge rows under its new id
    b_pid = bridge["posting_id"].to_numpy()
    b_sid = bridge["skill_id"].to_numpy()
    order = np.argsort(b_pid, kind="stable")
    b_pid, b_sid = b_pid[order], b_sid[order]
    starts = np.searchsorted(b_pid, old_ids, side="left")
    ends = np.searchsorted(b_pid, old_ids, side="right")
    lens = ends - starts
    rep_new = np.repeat(new_ids, lens)
    take = np.concatenate([np.arange(s, e) for s, e in zip(starts, ends)]) if lens.sum() else np.array([], int)
    new_bridge = pa.table({"posting_id": pa.array(rep_new, pa.int64()), "skill_id": pa.array(b_sid[take], pa.int64())})
    if out.exists():
        shutil.rmtree(out)
    for name, table in (("fact_job_postings", sampled), ("bridge_job_skills", new_bridge)):
        (out / name).mkdir(parents=True)
        pq.write_table(table, out / name / "part-0.parquet")
    for name in DIM_TABLES:
        (out / name).mkdir(parents=True)
        pq.write_table(ds.dataset(str(proc / name), format="parquet").to_table(), out / name / "part-0.parquet")
    return {"fact_rows": int(sampled.num_rows), "bridge_rows": int(new_bridge.num_rows),
            "real_fact_rows": int(fact.num_rows),
            "salaried_share": float(pc.mean(pc.or_(pc.is_valid(sampled["salary_min_usd"]),
                                                    pc.is_valid(sampled["salary_max_usd"]))).as_py())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default=",".join(map(str, DEFAULT_SCALES)))
    ap.add_argument("--list-scales", action="store_true", help="print the scale labels used by the benchmarks")
    args = ap.parse_args()
    scales = [int(s) for s in args.scales.split(",") if s]
    if args.list_scales:
        print(" ".join(all_labels(scales)))
        return
    manifest = {"seed": SEED, "method": "resample real HN postings with replacement, new ids",
                "real_postings": real_count(), "scales": {}}
    for n in scales:
        label = scale_label(n)
        raw_dir, proc_dir, _ = scale_dirs(label)
        rng = np.random.default_rng(SEED + n)
        synth_raw(n, raw_dir, rng)
        star = synth_star(n, proc_dir, np.random.default_rng(SEED + n))
        manifest["scales"][label] = {"rows": n, "is_synthetic": True, "raw_dir": str(raw_dir),
                                     "processed_dir": str(proc_dir), **star}
        log.info("scale %s: %s", label, manifest["scales"][label])
    manifest["scales"]["real"] = {"rows": manifest["real_postings"], "is_synthetic": False,
                                  "raw_dir": str(config.RAW_DIR), "processed_dir": str(config.PROCESSED_DIR)}
    atomic_write_json(config.RESULTS_DIR / "bench" / "synth_manifest.json", manifest)


if __name__ == "__main__":
    main()
