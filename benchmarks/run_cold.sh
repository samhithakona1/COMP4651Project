#!/usr/bin/env bash
# Origin: original
# Benchmark 2 cold-cache runs: before every single measurement, restart PostgreSQL (empties
# shared_buffers) and drop the Docker VM's OS page cache, then time one query.
set -euo pipefail
cd "$(dirname "$0")/.."
COLD_RUNS="${COLD_RUNS:-10}"
SCALES="${SCALES:-$(docker compose run --rm -T runner python -m benchmarks.query_latency list 2>/dev/null | tail -1)}"
PG_IMAGE="$(docker compose config --images postgres | head -1)"
for scale in $SCALES; do
  for arm in star flat; do
    for wl in a b c; do
      for run in $(seq 0 $((COLD_RUNS - 1))); do
        docker compose restart postgres >/dev/null
        until docker compose exec -T postgres pg_isready -q -U "$(grep ^POSTGRES_USER .env | cut -d= -f2)"; do sleep 1; done
        docker run --rm --privileged "$PG_IMAGE" sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
        docker compose run --rm -T -e GIT_COMMIT="${GIT_COMMIT:-uncommitted}" -e HOST_LABEL="$(uname -sm)" runner python -m benchmarks.query_latency cold-one \
          --scale "$scale" --arm "$arm" --workload "$wl" --run "$run" 2>&1 | grep -E "cold run|Error|Traceback" || true
      done
    done
  done
done
