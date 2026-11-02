# Origin: original
"""Loading the same Parquet twice leaves row counts unchanged; stale bridge rows are removed."""
import datetime as dt
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.conftest import requires_pg


def write(dirpath, name, table):
    (dirpath / name).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dirpath / name / "part-0.parquet")


def build(dirpath, bridge_rows):
    write(dirpath, "dim_date", pa.table({
        "date_id": pa.array([20250101, 20250102], pa.int32()),
        "full_date": pa.array([dt.date(2025, 1, 1), dt.date(2025, 1, 2)]),
        "year": pa.array([2025, 2025], pa.int16()), "quarter": pa.array([1, 1], pa.int16()),
        "month": pa.array([1, 1], pa.int16()), "day": pa.array([1, 2], pa.int16()),
        "day_of_week": pa.array([3, 4], pa.int16()), "is_weekend": [False, False]}))
    write(dirpath, "dim_skills", pa.table({"skill_id": pa.array([1, 2, 3], pa.int64()),
                                           "skill_name": ["Go", "Rust", "Kubernetes"],
                                           "category": ["Languages", "Languages", "Cloud/DevOps"],
                                           "emerging_ai": [False, False, False]}))
    write(dirpath, "dim_companies", pa.table({"company_id": pa.array([10, 11], pa.int64()),
                                              "company_key": ["acme", "unknown"], "company_name": ["Acme", "Unknown"]}))
    write(dirpath, "dim_locations", pa.table({"location_id": pa.array([20], pa.int64()), "location_key": ["unknown"],
                                              "city": pa.array([None], pa.string()),
                                              "state_province": pa.array([None], pa.string()),
                                              "country": pa.array([None], pa.string()), "is_tech_hub": [False]}))
    write(dirpath, "fact_job_postings", pa.table({
        "posting_id": pa.array([100, 101], pa.int64()), "source_type": ["hn", "hn"], "external_id": ["1", "2"],
        "thread_id": pa.array([9, 9], pa.int64()), "date_id": pa.array([20250101, 20250102], pa.int32()),
        "created_at_i": pa.array([1735689600, 1735776000], pa.int64()),
        "company_id": pa.array([10, 11], pa.int64()), "location_id": pa.array([20, 20], pa.int64()),
        "header_line": ['Acme | "Go" engineer, remote', None], "work_modality": ["Remote", "Unspecified"],
        "experience_level": ["Senior", "Unspecified"], "yoe_min": pa.array([5, None], pa.int16()),
        "is_remote": [True, False], "salary_min_usd": [150000.0, None], "salary_max_usd": [180000.0, None],
        "salary_currency": ["USD", None], "salary_flag": ["ok", "none"], "skill_count": pa.array([2, 1], pa.int16()),
        "text_length": pa.array([100, 50], pa.int32()), "is_synthetic": [False, False]}))
    write(dirpath, "bridge_job_skills", pa.table({"posting_id": pa.array([r[0] for r in bridge_rows], pa.int64()),
                                                  "skill_id": pa.array([r[1] for r in bridge_rows], pa.int64())}))
    write(dirpath, "skill_pairs", pa.table({
        "skill_a_id": pa.array([1], pa.int64()), "skill_b_id": pa.array([3], pa.int64()),
        "year": pa.array([None], pa.int16()), "pair_count": pa.array([1], pa.int32()),
        "n_postings": pa.array([2], pa.int32()), "support": [0.5], "confidence_a_to_b": [1.0],
        "confidence_b_to_a": [1.0], "lift": [2.0]}))


@requires_pg
def test_reload_is_idempotent(tmp_path):
    from src.load import load_postgres as L

    schema = f"test_loader_{uuid.uuid4().hex[:8]}"
    build(tmp_path, [(100, 1), (100, 3), (101, 2)])
    try:
        first = L.load_all(tmp_path, schema=schema, refresh=True, summary_path=False)["table_counts"]
        second = L.load_all(tmp_path, schema=schema, refresh=True, summary_path=False)["table_counts"]
        assert first == second
        assert first["fact_job_postings"] == 2 and first["bridge_job_skills"] == 3 and first["skill_pairs"] == 1
        conn = L.connect()
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("SELECT salary_avg_usd, work_modality::text, header_line FROM fact_job_postings WHERE posting_id = 100")
            avg, modality, header = cur.fetchone()
            assert float(avg) == 165000.0 and modality == "Remote" and header == 'Acme | "Go" engineer, remote'
            cur.execute("SELECT count(*) FROM mv_skill_year_stats")
            assert cur.fetchone()[0] == 3
        conn.close()
        # posting 100 loses Kubernetes: the stale bridge row must disappear
        build(tmp_path, [(100, 1), (101, 2)])
        third = L.load_all(tmp_path, schema=schema, summary_path=False)["table_counts"]
        assert third["bridge_job_skills"] == 2 and third["fact_job_postings"] == 2
    finally:
        conn = L.connect()
        with conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
