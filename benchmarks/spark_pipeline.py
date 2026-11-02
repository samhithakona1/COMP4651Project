# Origin: original
"""Benchmark 1, Spark arm: raw JSONL -> clean -> salary -> skills -> co-occurrence/lift -> Parquet.
Same steps as src/baseline/pandas_pipeline.py, using the production Spark functions.

    spark-submit benchmarks/spark_pipeline.py --input <raw dir> --out <dir>
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.spark.clean_parse import enrich, read_raw  # noqa: E402
from src.spark.cooccurrence import DEFAULT_MIN_PAIR_COUNT, pair_stats  # noqa: E402
from src.spark.session import get_spark  # noqa: E402
from src.spark.skills import add_skills  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-pair-count", type=int, default=DEFAULT_MIN_PAIR_COUNT)
    args = ap.parse_args()
    t0 = time.perf_counter()
    spark = get_spark("bench-spark-pipeline")
    df = add_skills(enrich(read_raw(spark, None, args.input)), spark)
    df = df.withColumn("posting_id", F.concat_ws(":", "source", F.col("id").cast("string"))) \
           .withColumn("created_year", F.year(F.to_date(F.from_unixtime("created_at_i")))).cache()
    keep = ["posting_id", "source", "year", "month", "header_line", "company_name_parsed", "company_key", "city",
            "state_province", "country", "is_tech_hub", "work_modality", "experience_level", "yoe_min",
            "salary_min_usd", "salary_max_usd", "salary_currency", "salary_flag", "skills", "skill_count"]
    out = Path(args.out)
    df.select(*keep).write.mode("overwrite").parquet(str(out / "postings"))
    ps = df.select("posting_id", F.explode("skills").alias("skill"), F.col("created_year").alias("year"))
    pair_stats(ps, None, args.min_pair_count).write.mode("overwrite").parquet(str(out / "skill_pairs"))
    pair_stats(ps, ["year"], args.min_pair_count).write.mode("overwrite").parquet(str(out / "skill_pairs_yearly"))
    rows = spark.read.parquet(str(out / "postings")).count()
    spark.stop()
    print(json.dumps({"rows": rows, "seconds": round(time.perf_counter() - t0, 3)}))


if __name__ == "__main__":
    main()
