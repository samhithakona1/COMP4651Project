# Origin: original
"""Monthly pipeline: find_thread → extract_hn → extract_remotive → validate_quality →
spark_transform → load_postgres → refresh_aggregates.

Target month: `dag_run.conf["month"]` ("YYYY-MM") when triggered manually, otherwise the month of
the run's logical date. The run scheduled on the 2nd of month M+1 has logical date = 2nd of M, so
it ingests month M's thread once that thread is complete.
XCom carries only small values (month, story id, paths, counts).
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "group14",
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=15),
}


@dag(
    dag_id="tech_hiring_ingest_pipeline",
    schedule="0 6 2 * *",
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["comp4651", "ingest"],
    doc_md=__doc__,
)
def tech_hiring_ingest_pipeline():

    @task
    def find_thread(**context) -> dict:
        from src.ingest.thread_finder import find_thread as lookup

        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        if conf.get("month"):
            year, month = (int(x) for x in conf["month"].split("-"))
        else:
            logical = context["logical_date"]
            year, month = logical.year, logical.month
        thread = lookup(year, month)
        if thread is None:
            raise ValueError(f"no 'Who is hiring?' thread found for {year:04d}-{month:02d}")
        return {"month": f"{year:04d}-{month:02d}", "year": year, "mon": month,
                "story_id": thread["story_id"], "title": thread["title"]}

    @task
    def extract_hn(thread: dict) -> dict:
        from src.ingest.extract import extract_hn_firebase

        return extract_hn_firebase(thread["year"], thread["mon"], thread["story_id"])

    @task
    def extract_remotive(thread: dict, _hn: dict) -> dict:
        """Remotive only exposes a current snapshot; attach it only to a recent target month."""
        from src.ingest.extract import extract_remotive as fetch

        today = dt.datetime.now(dt.timezone.utc).date()
        months_ago = (today.year - thread["year"]) * 12 + today.month - thread["mon"]
        if months_ago > 1:
            return {"skipped": True, "reason": f"target month is {months_ago} months old"}
        try:
            return fetch(thread["year"], thread["mon"])
        except Exception as exc:  # noqa: BLE001 - optional sanity-check source must not block HN
            return {"skipped": True, "reason": f"remotive fetch failed: {exc}"}

    @task
    def validate_quality(thread: dict, hn: dict, remotive: dict) -> dict:
        from src.ingest.quality_gate import validate_month

        sources = {"hn": True}
        if not remotive.get("skipped"):
            sources["remotive"] = False
        report = validate_month(thread["year"], thread["mon"], sources)
        return {"status": report["status"], "rows": {s: r.get("rows") for s, r in report["sources"].items()}}

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit --master \"${SPARK_MASTER:-local[*]}\" "
            "--driver-memory \"${SPARK_DRIVER_MEMORY:-2g}\" "
            "/opt/project/src/spark/build_dims.py "
            "--months {{ ti.xcom_pull(task_ids='find_thread')['month'] }}"
        ),
        cwd="/opt/project",
    )

    @task
    def load_postgres() -> dict:
        from src.load.load_postgres import load_all

        return load_all()

    @task
    def refresh_aggregates() -> list[str]:
        from src.load.load_postgres import refresh_materialized_views

        return refresh_materialized_views()

    thread = find_thread()
    hn = extract_hn(thread)
    remotive = extract_remotive(thread, hn)
    gate = validate_quality(thread, hn, remotive)
    gate >> spark_transform >> load_postgres() >> refresh_aggregates()


tech_hiring_ingest_pipeline()
