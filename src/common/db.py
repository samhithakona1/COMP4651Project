# Origin: original
"""Small psycopg2 helpers shared by analysis scripts and the dashboard."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common import config


def read_query(name: str) -> str:
    """Named SQL from sql/queries/<name>.sql (parameters use psycopg2 %(name)s style)."""
    return (Path(config.SQL_DIR) / "queries" / f"{name}.sql").read_text()


def query_df(conn, sql: str, params: dict | None = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    conn.rollback()  # read-only helper: never leave a transaction open
    return pd.DataFrame(rows, columns=cols)
