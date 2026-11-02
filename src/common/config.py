# Origin: original
"""Central configuration read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data"))
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", PROJECT_ROOT / "results"))

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SILVER_DIR = PROCESSED_DIR / "_silver" / "postings"
CACHE_DIR = DATA_DIR / "cache"
SYNTH_DIR = DATA_DIR / "synthetic"
SPARK_TMP_DIR = Path(os.environ.get("SPARK_LOCAL_DIR", DATA_DIR / "spark-tmp"))

TAXONOMY_PATH = PROJECT_ROOT / "src" / "spark" / "taxonomy.json"
SQL_DIR = PROJECT_ROOT / "sql"

PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "hiring"),
    "user": os.environ.get("POSTGRES_USER", "hiring"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")

FIREBASE_RATE = float(os.environ.get("FIREBASE_RATE", "40"))
ALGOLIA_RATE = float(os.environ.get("ALGOLIA_RATE", "10"))
HTTP_CONTACT = os.environ.get("HTTP_CONTACT", "COMP4651 Group 14, HKUST")
USER_AGENT = (
    "COMP4651-TechHiringAnalytics/1.0 "
    f"(academic course project; polite, cached, rate-limited; {HTTP_CONTACT})"
)

FIREBASE_BASE = "https://hacker-news.firebaseio.com/v0"
ALGOLIA_BASE = "https://hn.algolia.com/api/v1"
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
# Public API, no key. Restrict the snapshot to tech jobs; "" fetches all categories.
REMOTIVE_CATEGORY = os.environ.get("REMOTIVE_CATEGORY", "software-dev")


def pg_dsn(**overrides) -> str:
    cfg = {**PG, **overrides}
    return " ".join(f"{k}={v}" for k, v in cfg.items() if v != "")
