# Origin: original
"""Single-node Pandas version of the Spark pipeline (Benchmark 1 baseline).

Same workload and the same rules: raw JSONL -> HTML cleaning -> header/company/location/modality/
seniority -> salary -> skills -> co-occurrence with lift -> Parquet. Every regex comes from
src/common/patterns.py, src/common/salary.py and taxonomy.json, exactly as in src/spark/.

    python -m src.baseline.pandas_pipeline --input 'data/raw/source=hn' --out /tmp/pandas_out
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.common import patterns as P
from src.common.salary import parse_salary
from src.common.taxonomy import load_taxonomy
from src.spark.cooccurrence import DEFAULT_MIN_PAIR_COUNT

PY = P.py


def _sub(series: pd.Series, rules) -> pd.Series:
    for pattern, repl in rules:
        series = series.str.replace(PY(pattern), lambda _m, r=repl: r, regex=True)
    return series


def _first_label(checks: list[tuple[pd.Series, list[tuple[str, str]]]], default: str, index) -> pd.Series:
    out = pd.Series(pd.NA, index=index, dtype="object")
    for col, rules in checks:
        for label, pattern in rules:
            hit = col.str.contains(PY(pattern), regex=True, na=False) & out.isna()
            out[hit] = label
    return out.fillna(default)


def read_raw(paths: list[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        df = pd.read_json(p, lines=True, dtype={"id": "int64"})
        parts = dict(seg.split("=", 1) for seg in Path(p).parent.parts if "=" in seg)
        df["source"] = parts.get("source", "hn")
        df["year"] = int(parts.get("year", 0))
        df["month"] = int(parts.get("month", 0))
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    text = df["text"].astype("object").where(df["text"].notna(), None)
    clean = _sub(text.astype("string"), P.HTML_RULES)
    df["text_clean"] = clean
    header = clean.str.split("\n", n=1).str[0].str.slice(0, 500)
    df["header_line"] = header

    has_pipe = header.str.contains("|", regex=False, na=False)
    first = header.str.split("|", n=1).str[0]
    fallback = header.str.extract(PY(P.COMPANY_FALLBACK_RE), expand=False).fillna("")
    raw_company = first.where(has_pipe, fallback)
    cleaned = _sub(_sub(raw_company, P.COMPANY_CLEAN_RULES), [(P.COMPANY_TRIM_RE, "")])
    name = cleaned.where(cleaned.str.len().between(1, P.COMPANY_MAX_LEN))
    if "company_name" in df and (df["source"] == "remotive").any():
        name = name.where(df["source"] != "remotive", df.get("company_name").astype("string").str.strip())
    key = _sub(_sub(name.str.lower(), [(P.COMPANY_SUFFIX_RE, "")]), [(P.COMPANY_KEY_STRIP_RE, "")])
    df["company_name_parsed"] = name
    df["company_key"] = key.where(key.str.len() > 0)

    idx = pd.Series(np.nan, index=df.index)
    for i, (*_, pattern) in enumerate(P.LOCATION_HUBS):
        hit = header.str.contains(PY(pattern), regex=True, na=False) & idx.isna()
        idx[hit] = i
    hubs = P.LOCATION_HUBS
    for j, col in enumerate(["city", "state_province", "country"]):
        df[col] = idx.map(lambda i, j=j: hubs[int(i)][j] if not np.isnan(i) else None)
    df["is_tech_hub"] = idx.map(lambda i: bool(hubs[int(i)][3]) if not np.isnan(i) else False)

    df["work_modality"] = _first_label([(header, P.MODALITY_ORDER), (clean, P.MODALITY_ORDER)], "Unspecified", df.index)
    yoe = pd.to_numeric(clean.str.extract(PY(P.YOE_RE), expand=False), errors="coerce")
    df["yoe_min"] = yoe.where(yoe <= P.YOE_MAX).astype("Int16")
    level = _first_label([(header, P.LEVEL_ORDER)], "", df.index).replace("", pd.NA)
    for lo, hi, label in P.YOE_LEVELS:
        level = level.where(level.notna() | ~df["yoe_min"].between(lo, hi).fillna(False), label)
    df["experience_level"] = level.fillna("Unspecified")

    sal = [parse_salary(t if isinstance(t, str) else None) for t in clean.astype("object")]
    df["salary_min_usd"] = [s.min_usd for s in sal]
    df["salary_max_usd"] = [s.max_usd for s in sal]
    df["salary_currency"] = [s.currency for s in sal]
    df["salary_flag"] = [s.flag for s in sal]
    return df


def add_skills(df: pd.DataFrame) -> pd.DataFrame:
    clean = df["text_clean"]
    hits = {s.name: clean.str.contains(PY(s.pattern), regex=True, na=False).to_numpy(dtype=bool) for s in load_taxonomy()}
    names = np.array(list(hits))
    matrix = np.column_stack(list(hits.values())) if hits else np.zeros((len(df), 0), bool)
    df = df.copy()
    df["skills"] = [names[row].tolist() for row in matrix]
    df["skill_count"] = matrix.sum(axis=1).astype("int16")
    return df


def pair_stats(ps: pd.DataFrame, group_cols: list[str] | None = None,
               min_pair_count: int = DEFAULT_MIN_PAIR_COUNT) -> pd.DataFrame:
    """Same definitions as src/spark/cooccurrence.py (N = postings with >= 1 skill)."""
    g = list(group_cols or [])
    ps = ps[["posting_id", "skill", *g]].drop_duplicates()
    n_total = ps.groupby(g)["posting_id"].nunique().rename("n_postings") if g else ps["posting_id"].nunique()
    single = ps.groupby([*g, "skill"]).size().rename("n").reset_index()
    pairs = ps.merge(ps, on=["posting_id", *g], suffixes=("_a", "_b"))
    pairs = pairs[pairs["skill_a"] < pairs["skill_b"]]
    pairs = pairs.groupby([*g, "skill_a", "skill_b"]).size().rename("pair_count").reset_index()
    pairs = pairs[pairs["pair_count"] >= min_pair_count]
    pairs = pairs.merge(single.rename(columns={"skill": "skill_a", "n": "n_a"}), on=[*g, "skill_a"])
    pairs = pairs.merge(single.rename(columns={"skill": "skill_b", "n": "n_b"}), on=[*g, "skill_b"])
    if g:
        pairs = pairs.merge(n_total.reset_index(), on=g)
    else:
        pairs["n_postings"] = n_total
    pairs["support"] = pairs["pair_count"] / pairs["n_postings"]
    pairs["confidence_a_to_b"] = pairs["pair_count"] / pairs["n_a"]
    pairs["confidence_b_to_a"] = pairs["pair_count"] / pairs["n_b"]
    pairs["lift"] = pairs["n_postings"] * pairs["pair_count"] / (pairs["n_a"] * pairs["n_b"])
    return pairs


def run(input_glob: str, out_dir: str, min_pair_count: int = DEFAULT_MIN_PAIR_COUNT) -> dict:
    t0 = time.perf_counter()
    paths = sorted(glob.glob(os.path.join(input_glob, "**", "*.jsonl"), recursive=True)) \
        if os.path.isdir(input_glob) else sorted(glob.glob(input_glob))
    df = add_skills(enrich(read_raw(paths)))
    df["posting_id"] = df["source"] + ":" + df["id"].astype(str)
    df["created_year"] = pd.to_datetime(df["created_at_i"], unit="s", utc=True).dt.year
    ps = df[["posting_id", "skills", "created_year"]].explode("skills").dropna(subset=["skills"])
    ps = ps.rename(columns={"skills": "skill", "created_year": "year"})
    overall = pair_stats(ps, None, min_pair_count)
    yearly = pair_stats(ps, ["year"], min_pair_count)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    keep = ["posting_id", "source", "year", "month", "header_line", "company_name_parsed", "company_key", "city",
            "state_province", "country", "is_tech_hub", "work_modality", "experience_level", "yoe_min",
            "salary_min_usd", "salary_max_usd", "salary_currency", "salary_flag", "skills", "skill_count"]
    df[keep].to_parquet(out / "postings.parquet", index=False)
    overall.to_parquet(out / "skill_pairs.parquet", index=False)
    yearly.to_parquet(out / "skill_pairs_yearly.parquet", index=False)
    return {"rows": int(len(df)), "pairs": int(len(overall)), "pairs_yearly": int(len(yearly)),
            "seconds": round(time.perf_counter() - t0, 3)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="raw-lake directory or JSONL glob")
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-pair-count", type=int, default=DEFAULT_MIN_PAIR_COUNT)
    args = ap.parse_args()
    print(json.dumps(run(args.input, args.out, args.min_pair_count)))


if __name__ == "__main__":
    main()
