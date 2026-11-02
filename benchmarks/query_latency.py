# Origin: original
"""Benchmark 2: query latency, indexed star schema vs one denormalised table (same engine, same data).

Arms:  star = fact + dims + bridge with the indexes in sql/02_indexes.sql
       flat = raw_flat_postings (skills text[]), no secondary indexes
Workloads (benchmarks/queries/{arm}_{a,b,c}.sql):
  a  top-5 highest-paying skills co-occurring with Kubernetes, remote, 2025, >= 100 postings
  b  yearly median pay for Rust
  c  remote vs hybrid median by quarter
Each scale lives in its own schema bench_<scale>.

  prepare   load every scale's star schema + build raw_flat_postings
  warm      per (scale, arm, workload): 5 warm-up + 50 timed runs; EXPLAIN (ANALYZE, BUFFERS) saved
  cold-one  one timed run (benchmarks/run_cold.sh restarts Postgres and drops the OS page cache first)
Raw rows: results/bench/query_latency.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import time
from pathlib import Path

from benchmarks.meta import run_metadata
from benchmarks.synth import all_labels, scale_dirs
from src.common import config
from src.common.log import get_logger
from src.load.load_postgres import connect, load_all

log = get_logger("bench2")
QDIR = config.PROJECT_ROOT / "benchmarks" / "queries"
CSV = config.RESULTS_DIR / "bench" / "query_latency.csv"
EXPLAIN_DIR = config.RESULTS_DIR / "bench" / "explain"
ARMS, WORKLOADS = ["star", "flat"], ["a", "b", "c"]
WARMUP, TIMED = 5, 50


def schema_for(scale: str) -> str:
    return f"bench_{scale}"


def sql_for(arm: str, workload: str) -> str:
    return (QDIR / f"{arm}_{workload}.sql").read_text()


def prepare(scales: list[str]) -> None:
    for scale in scales:
        _, proc_dir, synthetic = scale_dirs(scale)
        schema = schema_for(scale)
        conn = connect()
        with conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
        t0 = time.perf_counter()
        load_all(proc_dir, schema=schema, refresh=False,
                 summary_path=config.RESULTS_DIR / "bench" / f"load_{schema}.json")
        conn = connect()
        with conn, conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute((QDIR / "flat_build.sql").read_text())
            cur.execute("SELECT count(*) FROM raw_flat_postings")
            n = cur.fetchone()[0]
            cur.execute("SELECT pg_total_relation_size('raw_flat_postings'), "
                        "pg_total_relation_size('fact_job_postings') + pg_total_relation_size('bridge_job_skills')")
            flat_b, star_b = cur.fetchone()
        conn.close()
        log.info("prepared %s (%s rows, synthetic=%s) in %.0fs; flat %.1f MB, star fact+bridge %.1f MB",
                 schema, n, synthetic, time.perf_counter() - t0, flat_b / 2**20, star_b / 2**20)


def _timed(cur, sql: str) -> tuple[float, int]:
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return (time.perf_counter() - t0) * 1000.0, len(rows)


def _append(rows: list[dict]) -> None:
    CSV.parent.mkdir(parents=True, exist_ok=True)
    new = not CSV.exists()
    with CSV.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        if new:
            w.writeheader()
        w.writerows(rows)


def _base(scale, arm, workload, cache, meta):
    _, _, synthetic = scale_dirs(scale)
    return {"recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "scale": scale,
            "is_synthetic": synthetic, "arm": arm, "workload": workload, "cache": cache,
            "pg_version": meta.get("pg_version"), "git_commit": meta["git_commit"], "host_cpus": meta["host_cpus"],
            "machine": meta["machine"], "vm_mem_total": meta["vm_mem_total"], "host_label": meta["host_label"]}


def warm(scales: list[str]) -> None:
    meta = run_metadata()
    EXPLAIN_DIR.mkdir(parents=True, exist_ok=True)
    for scale in scales:
        conn = connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            meta["pg_version"] = cur.fetchone()[0]
            cur.execute(f'SET search_path TO "{schema_for(scale)}"')
            for arm in ARMS:
                for wl in WORKLOADS:
                    sql = sql_for(arm, wl)
                    for _ in range(WARMUP):
                        _timed(cur, sql)
                    rows = []
                    for i in range(TIMED):
                        ms, n = _timed(cur, sql)
                        rows.append({**_base(scale, arm, wl, "warm", meta), "run": i, "latency_ms": round(ms, 3),
                                     "result_rows": n})
                    _append(rows)
                    cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
                    plan = "\n".join(r[0] for r in cur.fetchall())
                    (EXPLAIN_DIR / f"{scale}_{arm}_{wl}.txt").write_text(
                        f"-- scale={scale} arm={arm} workload={wl} (warm cache, recorded by query_latency.py)\n{sql}\n{plan}\n")
                    lat = sorted(r["latency_ms"] for r in rows)
                    log.info("%s %s %s warm p50=%.2fms p99=%.2fms", scale, arm, wl, lat[len(lat) // 2], lat[-1])
        conn.close()


def cold_one(scale: str, arm: str, workload: str, run: int) -> None:
    meta = run_metadata()
    conn = connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        meta["pg_version"] = cur.fetchone()[0]
        cur.execute(f'SET search_path TO "{schema_for(scale)}"')
        ms, n = _timed(cur, sql_for(arm, workload))
    conn.close()
    _append([{**_base(scale, arm, workload, "cold", meta), "run": run, "latency_ms": round(ms, 3), "result_rows": n}])
    log.info("%s %s %s cold run %d: %.2fms", scale, arm, workload, run, ms)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["prepare", "warm", "cold-one", "list"])
    ap.add_argument("--scales", default=None, help="comma-separated labels (default: all)")
    ap.add_argument("--scale")
    ap.add_argument("--arm", choices=ARMS)
    ap.add_argument("--workload", choices=WORKLOADS)
    ap.add_argument("--run", type=int, default=0)
    args = ap.parse_args()
    scales = args.scales.split(",") if args.scales else all_labels()
    if args.mode == "prepare":
        prepare(scales)
    elif args.mode == "warm":
        warm(scales)
    elif args.mode == "list":
        print(" ".join(scales))
    else:
        cold_one(args.scale, args.arm, args.workload, args.run)


if __name__ == "__main__":
    main()
