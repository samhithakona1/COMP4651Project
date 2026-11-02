# Origin: original
"""Raw lake -> silver postings -> star-schema Parquet (dims, fact, bridge, skill pairs).

    spark-submit src/spark/build_dims.py --months 2026-10          # incremental (DAG)
    spark-submit src/spark/build_dims.py --all                     # full rebuild

1. Parse only the requested raw partitions (regex work is the expensive part) and overwrite
   the matching partitions of the silver table data/processed/_silver/postings (dynamic overwrite).
2. Rebuild every gold table from the whole silver table (cheap: no regexes) so dimensions and
   co-occurrence always reflect the full corpus.
Surrogate keys are xxhash64 of natural keys, so they are stable across re-runs.
Writes real stage counts to results/processing/build_dims_summary.json.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

if __package__ in (None, ""):  # executed by spark-submit as a script
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from src.common import config  # noqa: E402
from src.common.log import get_logger  # noqa: E402
from src.common.paths import atomic_write_json, parse_month  # noqa: E402
from src.common.taxonomy import load_taxonomy  # noqa: E402
from src.spark.clean_parse import enrich, read_raw  # noqa: E402
from src.spark.cooccurrence import DEFAULT_MIN_PAIR_COUNT, pair_stats  # noqa: E402
from src.spark.session import get_spark  # noqa: E402
from src.spark.skills import add_skills  # noqa: E402

log = get_logger("build_dims")

DATE_START, DATE_END = "2011-01-01", "2027-12-31"
SILVER_COLUMNS = [
    "id", "thread_id", "author", "created_at_i", "api", "text_clean", "header_line",
    "company_name_parsed", "company_key", "city", "state_province", "country", "is_tech_hub",
    "work_modality", "experience_level", "yoe_min",
    "salary_min_usd", "salary_max_usd", "salary_currency", "salary_flag", "skills", "skill_count",
]


def stable_id(*cols) -> F.Column:
    return F.xxhash64(*cols)


def location_key_col() -> F.Column:
    all_null = F.col("city").isNull() & F.col("state_province").isNull() & F.col("country").isNull()
    return F.when(all_null, F.lit("unknown")).otherwise(
        F.concat_ws("|", F.coalesce("city", F.lit("")), F.coalesce("state_province", F.lit("")),
                    F.coalesce("country", F.lit(""))))


def build_silver(spark: SparkSession, months, raw_dir, silver_dir: Path, skill_mode: str) -> int:
    raw = read_raw(spark, months, raw_dir)
    parsed = add_skills(enrich(raw), spark, mode=skill_mode)
    out = parsed.select(*SILVER_COLUMNS, "source", "year", "month")
    writer = out.repartition("source", "year", "month").write.mode("overwrite").partitionBy("source", "year", "month")
    if months is None:  # full rebuild: also drop partitions whose raw data disappeared
        writer = writer.option("partitionOverwriteMode", "static")
    writer.parquet(str(silver_dir))
    written = spark.read.parquet(str(silver_dir))
    if months is not None:
        cond = F.lit(False)
        for y, m in months:
            cond = cond | ((F.col("year") == y) & (F.col("month") == m))
        written = written.where(cond)
    return written.count()


def dim_date(spark: SparkSession) -> DataFrame:
    d = spark.sql(f"SELECT explode(sequence(to_date('{DATE_START}'), to_date('{DATE_END}'), interval 1 day)) AS full_date")
    return d.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_id"),
        "full_date",
        F.year("full_date").cast("smallint").alias("year"),
        F.quarter("full_date").cast("smallint").alias("quarter"),
        F.month("full_date").cast("smallint").alias("month"),
        F.dayofmonth("full_date").cast("smallint").alias("day"),
        ((F.dayofweek("full_date") + 5) % 7 + 1).cast("smallint").alias("day_of_week"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )


def build_gold(spark: SparkSession, silver_dir: Path, out_dir: Path, min_pair_count: int) -> dict:
    silver = spark.read.parquet(str(silver_dir))
    silver = silver.withColumn("posted_at", F.to_timestamp(F.from_unixtime("created_at_i"))) \
                   .withColumn("date_id", F.date_format(F.to_date(F.from_unixtime("created_at_i")), "yyyyMMdd").cast("int"))
    in_range = (F.col("date_id") >= int(DATE_START.replace("-", ""))) & (F.col("date_id") <= int(DATE_END.replace("-", "")))
    out_of_range = silver.where(~in_range | F.col("date_id").isNull()).count()
    silver = silver.where(in_range).cache()

    # dim_skills from the taxonomy (every skill, matched or not)
    taxonomy = load_taxonomy()
    dim_skills = spark.createDataFrame(
        [(s.name, s.category, s.emerging_ai) for s in taxonomy],
        T.StructType([T.StructField("skill_name", T.StringType()), T.StructField("category", T.StringType()),
                      T.StructField("emerging_ai", T.BooleanType())]),
    ).select(stable_id("skill_name").alias("skill_id"), "skill_name", "category", "emerging_ai")

    # dim_companies: the most frequent spelling per normalised key; 'unknown' for unparseable headers
    from pyspark.sql.window import Window
    names = (silver.where(F.col("company_key").isNotNull())
             .groupBy("company_key", F.col("company_name_parsed").alias("company_name")).count())
    w = Window.partitionBy("company_key").orderBy(F.desc("count"), F.asc("company_name"))
    companies = names.withColumn("rn", F.row_number().over(w)).where("rn = 1").select("company_key", "company_name")
    unknown_company = spark.createDataFrame([("unknown", "Unknown")], "company_key string, company_name string")
    dim_companies = companies.unionByName(unknown_company).select(
        stable_id("company_key").alias("company_id"), F.substring("company_key", 1, 200).alias("company_key"),
        F.substring("company_name", 1, 200).alias("company_name"))

    # dim_locations
    locs = silver.select("city", "state_province", "country", "is_tech_hub").withColumn("location_key", location_key_col())
    unknown_loc = spark.createDataFrame([(None, None, None, False, "unknown")],
                                        "city string, state_province string, country string, is_tech_hub boolean, location_key string")
    dim_locations = (locs.unionByName(unknown_loc)
                     .groupBy("location_key").agg(F.first("city").alias("city"), F.first("state_province").alias("state_province"),
                                                  F.first("country").alias("country"), F.max("is_tech_hub").alias("is_tech_hub"))
                     .select(stable_id("location_key").alias("location_id"), "location_key", "city", "state_province",
                             "country", "is_tech_hub"))

    # fact
    fact = silver.select(
        stable_id(F.concat_ws(":", "source", F.col("id").cast("string"))).alias("posting_id"),
        F.col("source").alias("source_type"),
        F.col("id").cast("string").alias("external_id"),
        "thread_id", "date_id", "created_at_i",
        stable_id(F.coalesce("company_key", F.lit("unknown"))).alias("company_id"),
        stable_id(location_key_col()).alias("location_id"),
        "header_line", "work_modality", "experience_level", "yoe_min",
        (F.col("work_modality") == "Remote").alias("is_remote"),
        F.round("salary_min_usd", 2).alias("salary_min_usd"), F.round("salary_max_usd", 2).alias("salary_max_usd"),
        "salary_currency", "salary_flag", "skill_count",
        F.length("text_clean").alias("text_length"),
        F.lit(False).alias("is_synthetic"),
        F.col("year").alias("thread_year"), F.col("month").alias("thread_month"),
    )

    # bridge
    skill_ids = dim_skills.select(F.col("skill_name").alias("skill"), "skill_id")
    exploded = silver.select(stable_id(F.concat_ws(":", "source", F.col("id").cast("string"))).alias("posting_id"),
                             "source", F.year(F.to_date(F.from_unixtime("created_at_i"))).alias("year"),
                             F.explode("skills").alias("skill"))
    bridge = exploded.join(F.broadcast(skill_ids), "skill").select("posting_id", "skill_id").distinct()

    # co-occurrence: real HN postings only (Remotive is a sanity-check source)
    hn_ps = exploded.where(F.col("source") == "hn").select("posting_id", "skill", "year")
    ids_a = skill_ids.select(F.col("skill").alias("skill_a"), F.col("skill_id").alias("skill_a_id"))
    ids_b = skill_ids.select(F.col("skill").alias("skill_b"), F.col("skill_id").alias("skill_b_id"))
    overall = pair_stats(hn_ps, None, min_pair_count).withColumn("year", F.lit(None).cast("smallint"))
    yearly = pair_stats(hn_ps, ["year"], min_pair_count).withColumn("year", F.col("year").cast("smallint"))
    cols = ["skill_a_id", "skill_b_id", "skill_a", "skill_b", "year", "pair_count", "n_a", "n_b", "n_postings",
            "support", "confidence_a_to_b", "confidence_b_to_a", "lift"]
    overall = overall.join(F.broadcast(ids_a), "skill_a").join(F.broadcast(ids_b), "skill_b").select(*cols)
    yearly = yearly.join(F.broadcast(ids_a), "skill_a").join(F.broadcast(ids_b), "skill_b").select(*cols)

    tables = {
        "dim_date": dim_date(spark), "dim_skills": dim_skills, "dim_companies": dim_companies,
        "dim_locations": dim_locations, "fact_job_postings": fact, "bridge_job_skills": bridge,
        "skill_pairs": overall, "skill_pairs_yearly": yearly,
    }
    counts = {}
    for name, df in tables.items():
        n_files = 4 if name in ("fact_job_postings", "bridge_job_skills") else 1
        df.coalesce(n_files).write.mode("overwrite").parquet(str(out_dir / name))
        counts[name] = spark.read.parquet(str(out_dir / name)).count()
        log.info("wrote %s: %d rows", name, counts[name])

    # real stage counts (CLAUDE.md "honest data volume")
    stage = (silver.groupBy("source").agg(
        F.count("*").alias("postings"),
        F.sum((F.col("salary_min_usd").isNotNull() | F.col("salary_max_usd").isNotNull()).cast("int")).alias("with_salary_usd"),
        F.sum((F.col("salary_flag") == "non_usd").cast("int")).alias("salary_non_usd"),
        F.sum((F.col("salary_flag") == "out_of_range").cast("int")).alias("salary_out_of_range"),
        F.sum((F.col("skill_count") >= 1).cast("int")).alias("with_ge1_skill"),
        F.sum((F.col("skill_count") >= 2).cast("int")).alias("with_ge2_skills"),
    ).collect())
    by_year = (silver.where(F.col("source") == "hn").groupBy("year").agg(
        F.count("*").alias("postings"),
        F.sum((F.col("salary_min_usd").isNotNull() | F.col("salary_max_usd").isNotNull()).cast("int")).alias("with_salary_usd"),
        F.sum((F.col("skill_count") >= 2).cast("int")).alias("with_ge2_skills")).orderBy("year").collect())
    silver.unpersist()
    return {"tables": counts, "dropped_out_of_date_range": out_of_range,
            "by_source": {r["source"]: r.asDict() for r in stage},
            "hn_by_thread_year": [r.asDict() for r in by_year]}


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--months", help="comma-separated YYYY-MM list")
    grp.add_argument("--all", action="store_true")
    ap.add_argument("--min-pair-count", type=int, default=DEFAULT_MIN_PAIR_COUNT)
    ap.add_argument("--skill-mode", choices=["builtin", "udf"], default="builtin")
    ap.add_argument("--raw-dir", default=str(config.RAW_DIR))
    ap.add_argument("--out-dir", default=str(config.PROCESSED_DIR))
    ap.add_argument("--summary", default=str(config.RESULTS_DIR / "processing" / "build_dims_summary.json"))
    args = ap.parse_args(argv)

    months = None if args.all else [parse_month(m.strip()) for m in args.months.split(",") if m.strip()]
    out_dir = Path(args.out_dir)
    silver_dir = out_dir / "_silver" / "postings"
    spark = get_spark("build_dims")
    t0 = time.perf_counter()
    parsed = build_silver(spark, months, args.raw_dir, silver_dir, args.skill_mode)
    t1 = time.perf_counter()
    gold = build_gold(spark, silver_dir, out_dir, args.min_pair_count)
    t2 = time.perf_counter()
    summary = {
        "run_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "months": "all" if months is None else [f"{y:04d}-{m:02d}" for y, m in months],
        "skill_mode": args.skill_mode, "min_pair_count": args.min_pair_count,
        "spark_master": spark.sparkContext.master, "spark_version": spark.version,
        "postings_parsed_this_run": parsed,
        "seconds": {"silver": round(t1 - t0, 2), "gold": round(t2 - t1, 2)},
        **gold,
    }
    atomic_write_json(Path(args.summary), summary)
    log.info("summary: %s", {k: summary[k] for k in ("postings_parsed_this_run", "seconds", "tables")})
    spark.stop()
    return summary


if __name__ == "__main__":
    main()
