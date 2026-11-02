# Origin: original
"""Pooled PostgreSQL access for the dashboard. SQL lives in sql/queries/*.sql and is always
parameterised (psycopg2 %(name)s placeholders); widget input never reaches an f-string."""
from __future__ import annotations

import json

import pandas as pd
import psycopg2.pool
import streamlit as st

from src.common import config
from src.common.db import read_query


@st.cache_resource
def pool() -> psycopg2.pool.ThreadedConnectionPool:
    return psycopg2.pool.ThreadedConnectionPool(2, 10, dsn=config.pg_dsn(), connect_timeout=5)


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(name: str, params: dict | None = None) -> pd.DataFrame:
    sql = read_query(name)
    p = pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.rollback()
    finally:
        p.putconn(conn)
    df = pd.DataFrame(rows, columns=cols)
    for c in df.columns:  # NUMERIC -> Decimal; charts want floats
        if df[c].dtype == object and len(df) and df[c].map(lambda v: v.__class__.__name__ == "Decimal").any():
            df[c] = df[c].astype(float)
    return df


@st.cache_data(ttl=600, show_spinner=False)
def results_json(name: str) -> dict | None:
    path = config.RESULTS_DIR / name
    return json.loads(path.read_text()) if path.exists() else None
