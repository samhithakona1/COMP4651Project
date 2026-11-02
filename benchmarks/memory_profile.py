#!/usr/bin/env python3
# Origin: original
"""Memory profile of the running stack: samples `docker stats` about once per second while each
stage runs, then reports the peak per service and the peak total (sum over services at one
sample). Runs on the host with the standard library only.

Stages (each runs where the DAG runs it, i.e. in the Airflow scheduler container):
  idle       stack up, nothing running (30 s)
  ingest     Algolia re-fetch of the last 12 months (network, no cache)
  transform  spark-submit build_dims.py --all
  load       loader + materialized-view refresh
  dashboard  every Streamlit page executed (app/smoke_test.py in the streamlit container), then 30 s idle

Writes results/bench/memory_profile.csv (every sample) and results/bench/memory_profile.json.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results" / "bench"
UNITS = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "kB": 1e3, "MB": 1e6, "GB": 1e9}
SERVICES = ("postgres", "airflow-scheduler", "airflow-webserver", "streamlit")


def months_back(n: int) -> tuple[str, str]:
    today = dt.date.today()
    y, m = today.year, today.month - 1 or 12
    if today.month == 1:
        y -= 1
    end = f"{y:04d}-{m:02d}"
    total = y * 12 + (m - 1) - (n - 1)
    return f"{total // 12:04d}-{total % 12 + 1:02d}", end


START, END = months_back(12)
STAGES = [
    ("idle", ["python3", "-c", "import time; time.sleep(30)"]),
    ("ingest", ["docker", "compose", "exec", "-T", "airflow-scheduler", "python", "-m", "src.ingest.backfill",
                "--from", START, "--to", END, "--refresh", "--summary-name", "memprofile_backfill_summary.json"]),
    ("transform", ["docker", "compose", "exec", "-T", "airflow-scheduler", "bash", "-c",
                   'spark-submit --master "$SPARK_MASTER" --driver-memory "$SPARK_DRIVER_MEMORY" '
                   "src/spark/build_dims.py --all"]),
    ("load", ["docker", "compose", "exec", "-T", "airflow-scheduler", "python", "-m", "src.load.load_postgres", "--refresh"]),
    ("dashboard", ["bash", "-c", "docker compose exec -T streamlit python -m app.smoke_test && sleep 30"]),
]


def parse_mem(s: str) -> float:
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", s.strip())
    return float(m.group(1)) * UNITS.get(m.group(2), 1) / 2**20 if m else 0.0


def service_of(name: str) -> str:
    for s in SERVICES:
        if f"-{s}-" in name:
            return s
    return name


def sampler(stage: list[str], rows: list[dict], stop: threading.Event) -> None:
    while not stop.is_set():
        t = time.time()
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"], cwd=ROOT,
                             capture_output=True, text=True).stdout
        for line in out.splitlines():
            d = json.loads(line)
            name = d["Name"]
            if "comp4651" not in name and not any(f"-{s}-" in name for s in SERVICES):
                continue
            rows.append({"ts": round(t, 2), "stage": stage[0], "container": name, "service": service_of(name),
                         "mem_mb": round(parse_mem(d["MemUsage"].split("/")[0]), 1),
                         "limit_mb": round(parse_mem(d["MemUsage"].split("/")[1]), 1), "cpu_pct": d["CPUPerc"].rstrip("%")})
        stop.wait(max(0.0, 1.0 - (time.time() - t)))


def main() -> int:
    rows: list[dict] = []
    stage = ["setup"]
    stop = threading.Event()
    th = threading.Thread(target=sampler, args=(stage, rows, stop), daemon=True)
    th.start()
    results = {}
    for name, cmd in STAGES:
        stage[0] = name
        t0 = time.time()
        rc = subprocess.run(cmd, cwd=ROOT).returncode
        results[name] = {"seconds": round(time.time() - t0, 1), "returncode": rc}
        print(f"[memory_profile] {name}: rc={rc} in {results[name]['seconds']}s", flush=True)
    stop.set()
    th.join()
    RESULTS.mkdir(parents=True, exist_ok=True)
    with (RESULTS / "memory_profile.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = {"recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "stages": {}}
    for name, _ in STAGES:
        srows = [r for r in rows if r["stage"] == name]
        per_service: dict[str, float] = {}
        totals: dict[float, float] = {}
        for r in srows:
            per_service[r["service"]] = max(per_service.get(r["service"], 0.0), r["mem_mb"])
            totals[r["ts"]] = totals.get(r["ts"], 0.0) + r["mem_mb"]
        summary["stages"][name] = {**results[name], "samples": len({r["ts"] for r in srows}),
                                   "peak_mb_by_service": per_service,
                                   "peak_total_mb": round(max(totals.values()), 1) if totals else None}
    summary["overall_peak_total_mb"] = max((s["peak_total_mb"] or 0) for s in summary["stages"].values())
    dash = [r["mem_mb"] for r in rows if r["stage"] == "dashboard" and r["service"] == "streamlit"]
    summary["streamlit_steady_state_mb"] = round(sorted(dash)[len(dash) // 2], 1) if dash else None
    summary["note"] = ("Per-service peaks can occur at different moments; peak_total_mb is the largest sum over "
                       "services within one sample. Docker reports cgroup memory (RSS + page cache).")
    (RESULTS / "memory_profile.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: (v["peak_total_mb"], v["peak_mb_by_service"]) for k, v in summary["stages"].items()}, indent=1))
    return 0 if all(r["returncode"] == 0 for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
