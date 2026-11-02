# Origin: original
"""Spark vs Pandas parity on one real raw-lake month (skipped when no raw data is present)."""
import glob

import pandas as pd
import pytest

from src.common import config

FILES = sorted(glob.glob(str(config.RAW_DIR / "source=hn" / "year=*" / "month=*" / "part-0.jsonl")))
COLS = ["text_clean", "header_line", "company_name_parsed", "company_key", "city", "country", "is_tech_hub",
        "work_modality", "experience_level", "yoe_min", "salary_min_usd", "salary_max_usd", "salary_currency",
        "salary_flag", "skills"]


@pytest.mark.skipif(not FILES, reason="no raw HN data ingested")
def test_spark_pandas_parity_on_real_month(spark):
    from src.baseline.pandas_pipeline import add_skills, enrich, read_raw
    from src.spark.clean_parse import RAW_SCHEMA
    from src.spark.clean_parse import enrich as s_enrich
    from src.spark.skills import add_skills as s_add_skills

    path = FILES[-1]
    p = add_skills(enrich(read_raw([path]))).set_index("id").sort_index()
    raw = spark.read.schema(RAW_SCHEMA).json(path)
    from pyspark.sql import functions as F
    raw = raw.withColumn("source", F.lit("hn"))
    s = s_add_skills(s_enrich(raw), spark).toPandas().set_index("id").sort_index()
    assert list(p.index) == list(s.index)
    def norm(x):
        if hasattr(x, "__len__") and not isinstance(x, str):
            return tuple(x)
        return "<NA>" if pd.isna(x) else x

    mismatches = {}
    for c in COLS:
        bad = sum(norm(x) != norm(y) for x, y in zip(s[c], p[c]))
        if bad:
            mismatches[c] = int(bad)
    assert not mismatches, mismatches
