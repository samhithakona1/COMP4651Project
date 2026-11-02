# Origin: original
"""Hand-computed co-occurrence check.

Postings (N = 5 postings with >= 1 skill):
  p1: A, B, C      p2: A, B      p3: A, C      p4: B      p5: A, B
n_A = 4, n_B = 4, n_C = 2
n_AB = 3 (p1, p2, p5); n_AC = 2 (p1, p3); n_BC = 1 (p1)
"""
import pandas as pd
import pytest

from src.baseline.pandas_pipeline import pair_stats as pandas_pairs
from src.spark.cooccurrence import pair_stats

ROWS = [("p1", "A"), ("p1", "B"), ("p1", "C"), ("p2", "A"), ("p2", "B"), ("p3", "A"), ("p3", "C"),
        ("p4", "B"), ("p5", "A"), ("p5", "B"), ("p5", "B")]   # duplicate (p5, B) must not double count
EXPECTED = {
    ("A", "B"): dict(pair_count=3, support=3 / 5, confidence_a_to_b=3 / 4, confidence_b_to_a=3 / 4, lift=5 * 3 / (4 * 4)),
    ("A", "C"): dict(pair_count=2, support=2 / 5, confidence_a_to_b=2 / 4, confidence_b_to_a=2 / 2, lift=5 * 2 / (4 * 2)),
    ("B", "C"): dict(pair_count=1, support=1 / 5, confidence_a_to_b=1 / 4, confidence_b_to_a=1 / 2, lift=5 * 1 / (4 * 2)),
}


def check(rows):
    got = {(r["skill_a"], r["skill_b"]): r for r in rows}
    assert set(got) == set(EXPECTED)
    for pair, exp in EXPECTED.items():
        for k, v in exp.items():
            assert got[pair][k] == pytest.approx(v), (pair, k)
        assert got[pair]["n_postings"] == 5
    assert all(a < b for a, b in got)          # no (a, a) and no (b, a)


def test_spark_exact(spark):
    df = spark.createDataFrame(ROWS, "posting_id string, skill string")
    check([r.asDict() for r in pair_stats(df, None, min_pair_count=1).collect()])


def test_pandas_exact():
    df = pd.DataFrame(ROWS, columns=["posting_id", "skill"])
    check(pandas_pairs(df, None, min_pair_count=1).to_dict("records"))


def test_min_support_filter(spark):
    df = spark.createDataFrame(ROWS, "posting_id string, skill string")
    got = {(r["skill_a"], r["skill_b"]) for r in pair_stats(df, None, min_pair_count=2).collect()}
    assert got == {("A", "B"), ("A", "C")}


def test_grouped_by_year(spark):
    rows = [(p, s, 2024) for p, s in ROWS] + [("q1", "A", 2025), ("q1", "B", 2025)]
    df = spark.createDataFrame(rows, "posting_id string, skill string, year int")
    out = {(r["year"], r["skill_a"], r["skill_b"]): r for r in pair_stats(df, ["year"], 1).collect()}
    assert out[(2025, "A", "B")]["lift"] == pytest.approx(1.0)       # N=1, n_a=n_b=n_ab=1
    assert out[(2024, "A", "B")]["lift"] == pytest.approx(15 / 16)
