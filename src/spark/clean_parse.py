# Origin: original
"""Raw lake -> cleaned, parsed postings (Spark).

Everything except salary uses built-in Spark SQL functions (regexp_replace / regexp_extract /
rlike) with the shared patterns in src/common/patterns.py. Salary parsing needs multi-candidate
logic with context checks, so it runs as a vectorised pandas UDF around src/common/salary.py.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common import config
from src.common import patterns as P
from src.common.salary import parse_salary

RAW_SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("thread_id", T.LongType()),
    T.StructField("thread_month", T.StringType()),
    T.StructField("parent_id", T.LongType()),
    T.StructField("author", T.StringType()),
    T.StructField("created_at_i", T.LongType()),
    T.StructField("text", T.StringType()),
    T.StructField("api", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("company_name", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("job_type", T.StringType()),
    T.StructField("candidate_required_location", T.StringType()),
    T.StructField("salary_text", T.StringType()),
    T.StructField("tags", T.ArrayType(T.StringType())),
    T.StructField("url", T.StringType()),
])

SALARY_SCHEMA = T.StructType([
    T.StructField("min_usd", T.DoubleType()),
    T.StructField("max_usd", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("flag", T.StringType()),
])


def read_raw(spark: SparkSession, months: list[tuple[int, int]] | None = None,
             raw_dir: Path | str | None = None) -> DataFrame:
    """Read the partitioned raw lake; a month filter prunes partition folders."""
    df = spark.read.schema(RAW_SCHEMA).json(str(raw_dir or config.RAW_DIR))
    if months:
        cond = None
        for y, m in months:
            c = (F.col("year") == y) & (F.col("month") == m)
            cond = c if cond is None else (cond | c)
        df = df.where(cond)
    return df


def replace_chain(col: Column, rules: list[tuple[str, str]]) -> Column:
    for pattern, repl in rules:
        col = F.regexp_replace(col, pattern, P.spark_replacement(repl))
    return col


def clean_html(col: Column) -> Column:
    return replace_chain(col, P.HTML_RULES)


def first_label(cols_and_rules: list[tuple[Column, list[tuple[str, str]]]], default: str) -> Column:
    expr = None
    for col, rules in cols_and_rules:
        for label, pattern in rules:
            expr = F.when(col.rlike(pattern), F.lit(label)) if expr is None else expr.when(col.rlike(pattern), F.lit(label))
    return expr.otherwise(F.lit(default))


def company_columns(header: Column) -> tuple[Column, Column]:
    raw = F.when(header.contains("|"), F.split(header, r"\|").getItem(0)) \
           .otherwise(F.regexp_extract(header, P.COMPANY_FALLBACK_RE, 1))
    cleaned = F.regexp_replace(replace_chain(raw, P.COMPANY_CLEAN_RULES), P.COMPANY_TRIM_RE, "")
    name = F.when((F.length(cleaned) >= 1) & (F.length(cleaned) <= P.COMPANY_MAX_LEN), cleaned)
    key = F.regexp_replace(F.regexp_replace(F.lower(name), P.COMPANY_SUFFIX_RE, ""), P.COMPANY_KEY_STRIP_RE, "")
    key = F.when(F.length(key) > 0, key)
    return name, key


def location_columns(header: Column) -> dict[str, Column]:
    idx = None
    for i, (*_, pattern) in enumerate(P.LOCATION_HUBS):
        idx = F.when(header.rlike(pattern), F.lit(i)) if idx is None else idx.when(header.rlike(pattern), F.lit(i))
    idx = idx.otherwise(F.lit(None).cast("int"))

    def pick(values, dtype):
        arr = F.array(*[F.lit(v).cast(dtype) for v in values])
        return F.when(idx.isNotNull(), F.element_at(arr, idx + 1))

    hubs = P.LOCATION_HUBS
    return {
        "city": pick([h[0] for h in hubs], "string"),
        "state_province": pick([h[1] for h in hubs], "string"),
        "country": pick([h[2] for h in hubs], "string"),
        "is_tech_hub": F.coalesce(pick([h[3] for h in hubs], "boolean"), F.lit(False)),
    }


def experience_column(header: Column, yoe: Column) -> Column:
    expr = None
    for label, pattern in P.LEVEL_ORDER:
        expr = F.when(header.rlike(pattern), F.lit(label)) if expr is None else expr.when(header.rlike(pattern), F.lit(label))
    for lo, hi, label in P.YOE_LEVELS:
        expr = expr.when(yoe.between(lo, hi), F.lit(label))
    return expr.otherwise(F.lit("Unspecified"))


@F.pandas_udf(SALARY_SCHEMA)
def salary_udf(texts: pd.Series) -> pd.DataFrame:
    rows = [parse_salary(t) if isinstance(t, str) else parse_salary(None) for t in texts]
    return pd.DataFrame(rows, columns=["min_usd", "max_usd", "currency", "flag"])


def enrich(raw: DataFrame) -> DataFrame:
    """Add text_clean, header and every parsed attribute; one row per raw posting."""
    if "company_name" not in raw.columns:
        raw = raw.withColumn("company_name", F.lit(None).cast("string"))
    df = raw.withColumn("text_clean", clean_html(F.col("text")))
    header = F.substring(F.substring_index(F.col("text_clean"), "\n", 1), 1, 500)
    df = df.withColumn("header_line", header)
    h, body = F.col("header_line"), F.col("text_clean")

    name, key = company_columns(h)
    df = df.withColumn("company_name_parsed", name).withColumn("company_key", key)
    # Remotive gives the company as a structured field; prefer it over the synthetic header.
    df = df.withColumn("company_name_parsed",
                       F.when(F.col("source") == "remotive", F.trim(F.col("company_name"))).otherwise(F.col("company_name_parsed")))
    df = df.withColumn("company_key", F.when(F.col("source") == "remotive",
                       F.regexp_replace(F.regexp_replace(F.lower(F.col("company_name_parsed")), P.COMPANY_SUFFIX_RE, ""),
                                        P.COMPANY_KEY_STRIP_RE, "")).otherwise(F.col("company_key")))
    df = df.withColumn("company_key", F.when(F.length("company_key") > 0, F.col("company_key")))

    for col_name, expr in location_columns(h).items():
        df = df.withColumn(col_name, expr)

    df = df.withColumn("work_modality", first_label([(h, P.MODALITY_ORDER), (body, P.MODALITY_ORDER)], "Unspecified"))
    yoe = F.regexp_extract(body, P.YOE_RE, 1)
    df = df.withColumn("yoe_min", F.when(yoe != "", yoe.cast("int")))
    df = df.withColumn("yoe_min", F.when(F.col("yoe_min") <= P.YOE_MAX, F.col("yoe_min")).cast("smallint"))
    df = df.withColumn("experience_level", experience_column(h, F.col("yoe_min")))

    sal = salary_udf(F.col("text_clean"))
    df = (df.withColumn("_sal", sal)
            .withColumn("salary_min_usd", F.col("_sal.min_usd"))
            .withColumn("salary_max_usd", F.col("_sal.max_usd"))
            .withColumn("salary_currency", F.col("_sal.currency"))
            .withColumn("salary_flag", F.col("_sal.flag"))
            .drop("_sal"))
    return df
