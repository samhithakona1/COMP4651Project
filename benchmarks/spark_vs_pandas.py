# Origin: original
"""Benchmark 1: Spark vs Pandas on the same workload, per scale.

Per (engine, scale): 1 warm-up run + 5 measured runs, each a fresh subprocess. Wall-clock is the
subprocess lifetime (includes Spark JVM start-up, the honest end-to-end cost); peak RSS is the
max over 200 ms psutil samples of the whole process tree (Spark: driver JVM + Python workers).
The container has a hard memory limit (BENCH_MEM_LIMIT); a run killed by the OOM killer is
recorded as status=oom, and the remaining runs at that (engine, scale) are skipped.
Rows are appended to results/bench/spark_vs_pandas.csv.

    python -m benchmarks.spark_vs_pandas --engine pandas --scale 50k
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import shutil
import subprocess
import threading
import time

import psutil

from benchmarks.meta import run_metadata
from benchmarks.synth import scale_dirs
from src.common import config
from src.common.log import get_logger

log = get_logger("bench1")
CSV = config.RESULTS_DIR / "bench" / "spark_vs_pandas.csv"
WARMUP, MEASURED = 1, 5
# A Pandas run costs ~3 s per 1k postings here, so the full protocol at 200k/1M would take 7+ hours.
# Those scales get one measured run (no warm-up) unless BENCH_FULL=1; every row records its protocol.
LARGE_SCALES = {"200k", "1m"}
SAMPLE_S = 0.2
TIMEOUT_S = int(os.environ.get("BENCH_TIMEOUT_S", "5400"))
SPARK_MASTER = os.environ.get("BENCH_SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("BENCH_SPARK_DRIVER_MEMORY", "3g")


def command(engine: str, raw_dir, out_dir) -> list[str]:
    if engine == "pandas":
        return ["python", "-m", "src.baseline.pandas_pipeline", "--input", str(raw_dir / "source=hn"), "--out", str(out_dir)]
    return ["spark-submit", "--master", SPARK_MASTER, "--driver-memory", SPARK_DRIVER_MEMORY,
            "--conf", "spark.ui.enabled=false", str(config.PROJECT_ROOT / "benchmarks" / "spark_pipeline.py"),
            "--input", str(raw_dir), "--out", str(out_dir)]


def tree_rss(proc: psutil.Process) -> int:
    total = 0
    try:
        procs = [proc, *proc.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def run_once(engine: str, raw_dir, out_dir) -> dict:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    t0 = time.perf_counter()
    proc = subprocess.Popen(command(engine, raw_dir, out_dir), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env={**os.environ, "SPARK_LOG_LEVEL": "ERROR"})
    ps = psutil.Process(proc.pid)
    peak = [0]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            peak[0] = max(peak[0], tree_rss(ps))
            stop.wait(SAMPLE_S)

    th = threading.Thread(target=sample, daemon=True)
    th.start()
    try:
        out, err = proc.communicate(timeout=TIMEOUT_S)
        status = "ok" if proc.returncode == 0 else ("oom" if proc.returncode in (-9, 137) else "error")
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        status = "timeout"
    wall = time.perf_counter() - t0
    stop.set()
    th.join()
    if status == "error" and ("MemoryError" in err or "OutOfMemory" in err or "Cannot allocate memory" in err):
        status = "oom"
    if status != "ok":
        log.warning("%s run status=%s rc=%s stderr tail: %s", engine, status, proc.returncode, err[-800:])
    return {"status": status, "wall_s": round(wall, 3), "peak_rss_mb": round(peak[0] / 2**20, 1),
            "returncode": proc.returncode}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["pandas", "spark"], required=True)
    ap.add_argument("--scale", required=True, help="scale label, e.g. 10k, real, 1m")
    ap.add_argument("--runs", type=int, default=None)
    args = ap.parse_args()
    reduced = args.scale in LARGE_SCALES and os.environ.get("BENCH_FULL") != "1"
    warmup = 0 if reduced else WARMUP
    runs = args.runs if args.runs is not None else (1 if reduced else MEASURED)
    protocol = f"{warmup} warm-up + {runs} measured" + (" (reduced: long runtime)" if reduced else "")
    raw_dir, _, synthetic = scale_dirs(args.scale)
    out_dir = config.DATA_DIR / "bench_out" / f"{args.engine}_{args.scale}"
    meta = run_metadata()
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    rows = []
    for i in range(warmup + runs):
        kind = "warmup" if i < warmup else "measured"
        r = run_once(args.engine, raw_dir, out_dir)
        rows.append({"started_at": started, "engine": args.engine, "scale": args.scale, "is_synthetic": synthetic,
                     "run": i, "kind": kind, "protocol": protocol, **r,
                     "spark_master": SPARK_MASTER if args.engine == "spark" else "",
                     "spark_driver_memory": SPARK_DRIVER_MEMORY if args.engine == "spark" else "", **meta})
        log.info("%s %s run %d (%s): %s", args.engine, args.scale, i, kind, r)
        if r["status"] != "ok":
            for j in range(i + 1, warmup + runs):
                rows.append({**rows[-1], "run": j, "kind": "warmup" if j < warmup else "measured",
                             "status": f"skipped_after_{r['status']}", "wall_s": "", "peak_rss_mb": ""})
            break
    shutil.rmtree(out_dir, ignore_errors=True)
    CSV.parent.mkdir(parents=True, exist_ok=True)
    new = not CSV.exists()
    with CSV.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        if new:
            w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
