# Origin: original
"""Pairwise skill co-occurrence with support, confidence and lift.

N = number of postings with >= 1 skill in the scope (all years, or one year). Postings without any
taxonomy skill carry no co-occurrence information, so including them would only scale support down
and lift up uniformly; we document N this way in the report.
  support           = n_ab / N
  confidence_a_to_b = n_ab / n_a
  confidence_b_to_a = n_ab / n_b
  lift              = N * n_ab / (n_a * n_b)
Pairs are ordered skill_a < skill_b (string order), so (a, a) and (b, a) never appear.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_MIN_PAIR_COUNT = 30


def pair_stats(postings_skills: DataFrame, group_cols: list[str] | None = None,
               min_pair_count: int = DEFAULT_MIN_PAIR_COUNT) -> DataFrame:
    """postings_skills: one row per (posting_id, skill[, group_cols])."""
    g = list(group_cols or [])
    ps = postings_skills.select("posting_id", "skill", *g).distinct()

    n_total = ps.groupBy(*g).agg(F.countDistinct("posting_id").alias("n_postings")) if g else \
        ps.agg(F.countDistinct("posting_id").alias("n_postings"))
    single = ps.groupBy(*g, "skill").agg(F.count("*").alias("n"))

    a, b = ps.alias("a"), ps.alias("b")
    join_cond = [F.col("a.posting_id") == F.col("b.posting_id"), F.col("a.skill") < F.col("b.skill")]
    join_cond += [F.col(f"a.{c}") == F.col(f"b.{c}") for c in g]
    pairs = (a.join(b, join_cond)
              .groupBy(*[F.col(f"a.{c}").alias(c) for c in g],
                       F.col("a.skill").alias("skill_a"), F.col("b.skill").alias("skill_b"))
              .agg(F.count("*").alias("pair_count"))
              .where(F.col("pair_count") >= min_pair_count))

    sa = F.broadcast(single.select(*g, F.col("skill").alias("skill_a"), F.col("n").alias("n_a")))
    sb = F.broadcast(single.select(*g, F.col("skill").alias("skill_b"), F.col("n").alias("n_b")))
    out = pairs.join(sa, [*g, "skill_a"]).join(sb, [*g, "skill_b"])
    out = out.join(F.broadcast(n_total), g) if g else out.crossJoin(F.broadcast(n_total))
    return out.select(
        *g, "skill_a", "skill_b", "pair_count", "n_a", "n_b", "n_postings",
        (F.col("pair_count") / F.col("n_postings")).alias("support"),
        (F.col("pair_count") / F.col("n_a")).alias("confidence_a_to_b"),
        (F.col("pair_count") / F.col("n_b")).alias("confidence_b_to_a"),
        (F.col("n_postings") * F.col("pair_count") / (F.col("n_a") * F.col("n_b"))).alias("lift"),
    )
