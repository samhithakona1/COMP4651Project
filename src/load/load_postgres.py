# Origin: original
"""Parquet (data/processed) -> PostgreSQL star schema, idempotently.

For each table, Parquet record batches are streamed through pyarrow -> CSV -> COPY into a TEMP
staging table, then merged with INSERT ... ON CONFLICT DO UPDATE.
- dims and the fact table: one transaction per batch (50k rows by default);
- bridge_job_skills and skill_pairs are fully derived sets, so each is replaced in a single
  transaction (stale rows of re-loaded postings are deleted, new ones inserted).
Re-loading the same Parquet leaves every row count unchanged (tests/test_loader.py).
ANALYZE runs afterwards; `--refresh` also refreshes the dashboard materialized views.

    python -m src.load.load_postgres [--processed-dir DIR] [--schema public] [--refresh]
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import pyarrow.csv as pacsv
import pyarrow.dataset as ds

from src.common import config
from src.common.log import get_logger
from src.common.paths import atomic_write_json

log = get_logger("load_postgres")

BATCH_ROWS = 50_000
SCHEMA_FILES = ["01_schema.sql", "02_indexes.sql", "03_dim_date_seed.sql", "04_views.sql"]
MATERIALIZED_VIEWS = ["mv_skill_year_stats", "mv_modality_quarter_stats"]


@dataclass(frozen=True)
class Table:
    name: str
    parquet: str
    columns: list[tuple[str, str]]          # staging (name, pg type) == parquet columns used
    key: list[str]
    select: dict[str, str] | None = None    # target column -> expression over staging


TABLES = [
    Table("dim_date", "dim_date",
          [("date_id", "integer"), ("full_date", "date"), ("year", "smallint"), ("quarter", "smallint"),
           ("month", "smallint"), ("day", "smallint"), ("day_of_week", "smallint"), ("is_weekend", "boolean")],
          ["date_id"]),
    Table("dim_skills", "dim_skills",
          [("skill_id", "bigint"), ("skill_name", "text"), ("category", "text"), ("emerging_ai", "boolean")],
          ["skill_id"]),
    Table("dim_companies", "dim_companies",
          [("company_id", "bigint"), ("company_key", "text"), ("company_name", "text")], ["company_id"]),
    Table("dim_locations", "dim_locations",
          [("location_id", "bigint"), ("location_key", "text"), ("city", "text"), ("state_province", "text"),
           ("country", "text"), ("is_tech_hub", "boolean")], ["location_id"]),
    Table("fact_job_postings", "fact_job_postings",
          [("posting_id", "bigint"), ("source_type", "text"), ("external_id", "text"), ("thread_id", "bigint"),
           ("date_id", "integer"), ("created_at_i", "bigint"), ("company_id", "bigint"), ("location_id", "bigint"),
           ("header_line", "text"), ("work_modality", "text"), ("experience_level", "text"), ("yoe_min", "smallint"),
           ("is_remote", "boolean"), ("salary_min_usd", "double precision"), ("salary_max_usd", "double precision"),
           ("salary_currency", "text"), ("salary_flag", "text"), ("skill_count", "smallint"),
           ("text_length", "integer"), ("is_synthetic", "boolean")],
          ["posting_id"],
          select={"posting_id": "posting_id", "source_type": "source_type", "external_id": "external_id",
                  "thread_id": "thread_id", "date_id": "date_id", "company_id": "company_id",
                  "location_id": "location_id", "posted_at": "to_timestamp(created_at_i)",
                  "header_line": "header_line", "work_modality": "work_modality::work_modality_enum",
                  "experience_level": "experience_level::experience_level_enum", "yoe_min": "yoe_min",
                  "is_remote": "is_remote", "salary_min_usd": "salary_min_usd", "salary_max_usd": "salary_max_usd",
                  "salary_currency": "salary_currency", "salary_flag": "salary_flag", "skill_count": "skill_count",
                  "text_length": "text_length", "is_synthetic": "is_synthetic", "loaded_at": "now()"}),
]
BRIDGE = Table("bridge_job_skills", "bridge_job_skills", [("posting_id", "bigint"), ("skill_id", "bigint")],
               ["posting_id", "skill_id"])
PAIRS = Table("skill_pairs", "skill_pairs",
              [("skill_a_id", "bigint"), ("skill_b_id", "bigint"), ("year", "smallint"), ("pair_count", "integer"),
               ("n_postings", "integer"), ("support", "double precision"), ("confidence_a_to_b", "double precision"),
               ("confidence_b_to_a", "double precision"), ("lift", "double precision")],
              ["skill_a_id", "skill_b_id", "year"])


def connect(**overrides):
    return psycopg2.connect(config.pg_dsn(**overrides))


def apply_schema(conn, schema: str = "public") -> None:
    with conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'SET search_path TO "{schema}"')
        for name in SCHEMA_FILES:
            cur.execute((config.SQL_DIR / name).read_text())


def _batches(path: Path, columns: list[str], batch_rows: int):
    dataset = ds.dataset(str(path), format="parquet")
    for batch in dataset.to_batches(columns=columns, batch_size=batch_rows):
        if batch.num_rows:
            yield batch


def _csv(batch) -> io.BytesIO:
    buf = io.BytesIO()
    pacsv.write_csv(batch, buf, write_options=pacsv.WriteOptions(include_header=False))
    buf.seek(0)
    return buf


def _create_stage(cur, table: Table, stage: str) -> None:
    cols = ", ".join(f'"{c}" {t}' for c, t in table.columns)
    cur.execute(f"DROP TABLE IF EXISTS {stage}; CREATE TEMP TABLE {stage} ({cols})")


def _copy(cur, table: Table, stage: str, batch) -> None:
    cols = ", ".join(f'"{c}"' for c, _ in table.columns)
    cur.copy_expert(f"COPY {stage} ({cols}) FROM STDIN WITH (FORMAT csv)", _csv(batch))


def _upsert_sql(table: Table, stage: str) -> str:
    select = table.select or {c: c for c, _ in table.columns}
    target = ", ".join(f'"{c}"' for c in select)
    exprs = ", ".join(select.values())
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in select if c not in table.key)
    keys = ", ".join(table.key)
    return (f"INSERT INTO {table.name} ({target}) SELECT {exprs} FROM {stage} "
            f"ON CONFLICT ({keys}) DO UPDATE SET {updates}")


def load_upsert(conn, table: Table, processed_dir: Path, batch_rows: int) -> int:
    stage = f"stg_{table.name}"
    total = 0
    with conn.cursor() as cur:
        _create_stage(cur, table, stage)
        if table.name == "fact_job_postings":
            cur.execute("DROP TABLE IF EXISTS loaded_postings; CREATE TEMP TABLE loaded_postings (posting_id bigint PRIMARY KEY)")
    conn.commit()
    sql = _upsert_sql(table, stage)
    for batch in _batches(processed_dir / table.parquet, [c for c, _ in table.columns], batch_rows):
        with conn, conn.cursor() as cur:            # one transaction per batch
            cur.execute(f"TRUNCATE {stage}")
            _copy(cur, table, stage, batch)
            cur.execute(sql)
            if table.name == "fact_job_postings":
                cur.execute(f"INSERT INTO loaded_postings SELECT posting_id FROM {stage} ON CONFLICT DO NOTHING")
        total += batch.num_rows
    log.info("%s: upserted %d rows", table.name, total)
    return total


def load_bridge(conn, processed_dir: Path, batch_rows: int) -> int:
    stage, total = "stg_bridge", 0
    with conn, conn.cursor() as cur:                # single transaction: replace the derived set
        _create_stage(cur, BRIDGE, stage)
        for batch in _batches(processed_dir / BRIDGE.parquet, ["posting_id", "skill_id"], batch_rows):
            _copy(cur, BRIDGE, stage, batch)
            total += batch.num_rows
        cur.execute(f"CREATE INDEX ON {stage} (posting_id, skill_id); ANALYZE {stage}")
        cur.execute(f"""
            DELETE FROM bridge_job_skills b USING loaded_postings lp
            WHERE b.posting_id = lp.posting_id
              AND NOT EXISTS (SELECT 1 FROM {stage} s WHERE s.posting_id = b.posting_id AND s.skill_id = b.skill_id)""")
        deleted = cur.rowcount
        cur.execute(f"INSERT INTO bridge_job_skills (posting_id, skill_id) SELECT DISTINCT posting_id, skill_id "
                    f"FROM {stage} ON CONFLICT DO NOTHING")
        inserted = cur.rowcount
    log.info("bridge_job_skills: staged %d, deleted %d stale, inserted %d new", total, deleted, inserted)
    return total


def load_pairs(conn, processed_dir: Path) -> int:
    stage, total = "stg_pairs", 0
    cols = [c for c, _ in PAIRS.columns]
    with conn, conn.cursor() as cur:                # single transaction: replace the derived set
        _create_stage(cur, PAIRS, stage)
        for name in ("skill_pairs", "skill_pairs_yearly"):
            path = processed_dir / name
            if path.exists():
                for batch in _batches(path, cols, BATCH_ROWS):
                    _copy(cur, PAIRS, stage, batch)
                    total += batch.num_rows
        cur.execute("DELETE FROM skill_pairs")
        cur.execute(f"INSERT INTO skill_pairs ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {stage}")
    log.info("skill_pairs: replaced with %d rows", total)
    return total


def refresh_materialized_views(conn=None, schema: str = "public") -> list[str]:
    own = conn is None
    conn = conn or connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
            for mv in MATERIALIZED_VIEWS:
                t0 = time.perf_counter()
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                log.info("refreshed %s in %.2fs", mv, time.perf_counter() - t0)
        conn.autocommit = False
        return MATERIALIZED_VIEWS
    finally:
        if own:
            conn.close()


def table_counts(conn, schema: str = "public") -> dict[str, int]:
    out = {}
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}"')
        for t in [*(t.name for t in TABLES), BRIDGE.name, PAIRS.name]:
            cur.execute(f"SELECT count(*) FROM {t}")
            out[t] = cur.fetchone()[0]
    conn.commit()
    return out


def load_all(processed_dir: Path | str | None = None, schema: str = "public", refresh: bool = False,
             batch_rows: int = BATCH_ROWS, summary_path: Path | None = None, ensure_schema: bool = True) -> dict:
    processed_dir = Path(processed_dir or config.PROCESSED_DIR)
    conn = connect()
    t0 = time.perf_counter()
    try:
        if ensure_schema:
            apply_schema(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
        conn.commit()
        staged = {t.name: load_upsert(conn, t, processed_dir, batch_rows) for t in TABLES}
        staged[BRIDGE.name] = load_bridge(conn, processed_dir, batch_rows)
        staged[PAIRS.name] = load_pairs(conn, processed_dir)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ANALYZE")
        conn.autocommit = False
        if refresh:
            refresh_materialized_views(conn, schema)
        counts = table_counts(conn, schema)
    finally:
        conn.close()
    summary = {"loaded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "schema": schema,
               "processed_dir": str(processed_dir), "rows_staged": staged, "table_counts": counts,
               "seconds": round(time.perf_counter() - t0, 2)}
    if summary_path is not False:
        atomic_write_json(Path(summary_path or config.RESULTS_DIR / "load" / f"load_summary_{schema}.json"), summary)
    log.info("load complete in %.1fs: %s", summary["seconds"], counts)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed-dir", default=str(config.PROCESSED_DIR))
    ap.add_argument("--schema", default="public")
    ap.add_argument("--batch-rows", type=int, default=BATCH_ROWS)
    ap.add_argument("--refresh", action="store_true", help="refresh materialized views after loading")
    ap.add_argument("--refresh-only", action="store_true")
    args = ap.parse_args()
    if args.refresh_only:
        print(refresh_materialized_views(schema=args.schema))
        return
    s = load_all(args.processed_dir, args.schema, args.refresh, args.batch_rows)
    print(s["table_counts"])


if __name__ == "__main__":
    main()
