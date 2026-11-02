# Origin: original
"""Skill extraction against the taxonomy (src/spark/taxonomy.json).

Two interchangeable implementations, same output (`skills: array<string>`, distinct, taxonomy order):
- "builtin" (default): one `rlike` per skill inside a single array expression. The taxonomy is
  compiled once on the driver into the physical plan, which Spark ships to every executor task.
- "udf": the compiled taxonomy is broadcast (`SparkContext.broadcast`) and a pandas UDF matches
  Python regexes on the workers. Kept for comparison; it is slower (see results/processing/).
"""
from __future__ import annotations

import re

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.taxonomy import Skill, load_taxonomy


def skills_builtin(text: Column, taxonomy: tuple[Skill, ...] | None = None) -> Column:
    taxonomy = taxonomy or load_taxonomy()
    arr = F.array(*[F.when(text.rlike(s.pattern), F.lit(s.name)) for s in taxonomy])
    return F.filter(arr, lambda x: x.isNotNull())


_COMPILED: dict[tuple, list[tuple[str, re.Pattern]]] = {}


def skills_udf(spark: SparkSession, text: Column, taxonomy: tuple[Skill, ...] | None = None) -> Column:
    taxonomy = taxonomy or load_taxonomy()
    bc = spark.sparkContext.broadcast([(s.name, s.pattern) for s in taxonomy])

    @F.pandas_udf(T.ArrayType(T.StringType()))
    def match(texts: pd.Series) -> pd.Series:
        key = tuple(bc.value)
        compiled = _COMPILED.get(key)
        if compiled is None:  # compile once per Python worker
            compiled = _COMPILED[key] = [(n, re.compile(p, re.ASCII)) for n, p in key]
        return texts.map(lambda t: [n for n, p in compiled if p.search(t)] if isinstance(t, str) else [])

    return match(text)


def add_skills(df, spark: SparkSession, text_col: str = "text_clean", mode: str = "builtin"):
    col = skills_builtin(F.col(text_col)) if mode == "builtin" else skills_udf(spark, F.col(text_col))
    return df.withColumn("skills", col).withColumn("skill_count", F.size("skills").cast("smallint"))
