# Origin: original
"""Run metadata recorded with every benchmark row: hardware, versions, git commit, memory limit."""
from __future__ import annotations

import os
import platform
import subprocess
from pathlib import Path


def _cgroup_limit() -> str | None:
    for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            v = Path(p).read_text().strip()
            return v if v == "max" else f"{int(v) / 2**30:.2f}GiB"
        except (OSError, ValueError):
            continue
    return None


def git_commit() -> str:
    if os.environ.get("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "uncommitted"


def run_metadata() -> dict:
    import pandas
    import pyarrow
    try:
        import pyspark
        spark_v = pyspark.__version__
    except ImportError:
        spark_v = None
    mem_total = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                mem_total = f"{int(line.split()[1]) / 2**20:.2f}GiB"
    except OSError:
        pass
    return {
        "host_cpus": os.cpu_count(), "machine": platform.machine(), "kernel": platform.release(),
        "vm_mem_total": mem_total, "container_mem_limit": _cgroup_limit(),
        "python": platform.python_version(), "pandas": pandas.__version__, "pyarrow": pyarrow.__version__,
        "pyspark": spark_v, "git_commit": git_commit(),
        "host_label": os.environ.get("HOST_LABEL", "unspecified"),
    }
