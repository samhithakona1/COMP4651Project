# Origin: original
"""Every headline number for Q1-Q4 -> results/findings.json (+ tables in results/findings/*.csv).

Reads the warehouse (real HN postings only), the fitted salary model (results/salary_model.json)
and the ingest / processing summaries. Nothing here is typed in by hand.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
from collections import defaultdict

import networkx as nx
import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.common import config
from src.common.db import query_df, read_query
from src.common.log import get_logger
from src.common.paths import atomic_write_json
from src.common.salary_model import load_model
from src.common.taxonomy import GENAI_FRAMEWORKS, LEGACY_STACKS, MODERN_SYSTEMS
from src.load.load_postgres import connect

log = get_logger("findings")
OUT = config.RESULTS_DIR / "findings"
GENAI_FROM_YEAR = 2023
MIN_SALARY_N = 20          # medians with fewer salaried postings are reported as null
CLUSTER_SEED = 4651
CLUSTER_MIN_YEAR_POSTINGS = 1000


def _f(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x)


def _median_or_none(med, n):
    return _f(med) if n is not None and n >= MIN_SALARY_N else None


def save(df: pd.DataFrame, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / f"{name}.csv", index=False)


def term(model, name):
    for r in model["terms"]:
        if r["term"] == name:
            return {"coef": r["coef"], "std_err": r["std_err"], "p_value": r["p_value"],
                    "pct_effect": float(np.expm1(r["coef"])),
                    "pct_ci_low": float(np.expm1(r["ci_low"])), "pct_ci_high": float(np.expm1(r["ci_high"]))}
    return None


# ---- data volume ---------------------------------------------------------------------------------
def volume(conn) -> dict:
    out: dict = {}
    bf = config.RESULTS_DIR / "ingest" / "backfill_summary.json"
    if bf.exists():
        b = json.loads(bf.read_text())
        out["ingest"] = {k: b[k] for k in ("from", "to", "months_requested", "months_with_thread",
                                           "months_missing_thread", "total_comments_fetched", "total_postings_kept")}
    bd = config.RESULTS_DIR / "processing" / "build_dims_summary.json"
    if bd.exists():
        out["processing"] = json.loads(bd.read_text()).get("by_source")
    ov = query_df(conn, read_query("overview")).iloc[0]
    out["warehouse_hn"] = {"postings": int(ov.postings), "with_salary_usd": int(ov.with_salary),
                           "with_ge2_skills": int(ov.with_ge2_skills), "companies": int(ov.companies),
                           "first_date": str(ov.first_date), "last_date": str(ov.last_date),
                           "salary_coverage": float(ov.with_salary / ov.postings) if ov.postings else None}
    by_year = query_df(conn, read_query("postings_by_year"))
    save(by_year, "volume_by_year")
    return out


# ---- Q1 --------------------------------------------------------------------------------------------
def q1(conn, model) -> dict:
    skills = [*MODERN_SYSTEMS, *LEGACY_STACKS]
    trends = query_df(conn, read_query("skill_trends"), {"skills": skills})
    save(trends, "q1_skill_year")
    grp = query_df(conn, read_query("skill_group_year"), {"group_a": list(MODERN_SYSTEMS), "group_b": list(LEGACY_STACKS)})
    piv = grp.pivot(index="year", columns="grp", values=["postings", "salary_n", "median_salary"])
    rows = []
    for year, r in piv.iterrows():
        m_med = _median_or_none(r[("median_salary", "A")], r.get(("salary_n", "A")))
        l_med = _median_or_none(r[("median_salary", "B")], r.get(("salary_n", "B")))
        rows.append({"year": int(year), "modern_postings": _f(r[("postings", "A")]), "legacy_postings": _f(r[("postings", "B")]),
                     "modern_salary_n": _f(r[("salary_n", "A")]), "legacy_salary_n": _f(r[("salary_n", "B")]),
                     "modern_median": m_med, "legacy_median": l_med,
                     "modern_premium_pct": (m_med / l_med - 1) if m_med and l_med else None})
    group_year = pd.DataFrame(rows)
    save(group_year, "q1_group_year")
    latest = group_year.dropna(subset=["modern_premium_pct"]).tail(1)
    per_skill_latest = {}
    for s in skills:
        t = trends[trends.skill_name == s].sort_values("year")
        if len(t):
            last = t.iloc[-1]
            first = t.iloc[0]
            per_skill_latest[s] = {
                "first_year": int(first.year), "first_year_share": _f(first.posting_share),
                "latest_year": int(last.year), "latest_year_share": _f(last.posting_share),
                "latest_year_postings": int(last.posting_count),
                "latest_year_median_salary": _median_or_none(last.median_salary, last.salary_n),
                "latest_year_salary_n": int(last.salary_n),
                "peak_share_year": int(t.loc[t.posting_share.idxmax(), "year"]),
                "model_effect": term(model, f"skill[{s}]") if model else None,
            }
    return {"skills": skills, "modern": list(MODERN_SYSTEMS), "legacy": list(LEGACY_STACKS),
            "latest_year_with_both": latest.to_dict("records")[0] if len(latest) else None,
            "per_skill": per_skill_latest,
            "note": f"medians need >= {MIN_SALARY_N} salaried postings; model effects are percentage differences in "
                    "posted pay holding other skills, modality, level and year fixed."}


# ---- Q2 --------------------------------------------------------------------------------------------
def q2(conn) -> dict:
    params = {"skills": list(GENAI_FRAMEWORKS), "from_year": GENAI_FROM_YEAR}
    quarterly = query_df(conn, read_query("genai_quarterly"), params)
    quarterly["quarter_label"] = quarterly["year"].astype(str) + "Q" + quarterly["quarter"].astype(str)
    save(quarterly, "q2_genai_quarterly")
    per_fw = query_df(conn, read_query("genai_by_framework"), params)
    save(per_fw, "q2_genai_by_framework")

    # Premium, regression-adjusted: ln(salary) ~ genai + level + modality + year FE, 2023+.
    sample = query_df(conn, read_query("model_sample"))
    sample = sample[sample.year >= GENAI_FROM_YEAR]
    skills = query_df(conn, read_query("model_sample_skills"))
    genai_ids = set(skills[skills.skill_name.isin(GENAI_FRAMEWORKS)].posting_id)
    sample = sample.assign(genai=sample.posting_id.isin(genai_ids).astype(float))
    adjusted = None
    if sample.genai.sum() >= MIN_SALARY_N:
        X = pd.get_dummies(sample[["experience_level", "work_modality", "year"]].astype(str), drop_first=True).astype(float)
        X.insert(0, "genai", sample.genai.to_numpy())
        X = sm.add_constant(X)
        res = sm.OLS(np.log(sample.salary.astype(float)).to_numpy(), X.to_numpy()).fit(cov_type="HC1")
        b, se = res.params[1], res.bse[1]
        lo, hi = res.conf_int()[1]
        adjusted = {"n_obs": int(res.nobs), "n_genai": int(sample.genai.sum()), "coef": float(b), "std_err": float(se),
                    "p_value": float(res.pvalues[1]), "pct_effect": float(np.expm1(b)),
                    "pct_ci_low": float(np.expm1(lo)), "pct_ci_high": float(np.expm1(hi)),
                    "controls": "experience level, work modality, year fixed effects; HC1 SEs"}
    raw_g = sample[sample.genai == 1].salary
    raw_o = sample[sample.genai == 0].salary
    first, last = quarterly.iloc[0], quarterly.iloc[-1]
    return {
        "frameworks": list(GENAI_FRAMEWORKS), "from_year": GENAI_FROM_YEAR,
        "first_quarter": {"label": first.quarter_label, "share": _f(first.genai_share), "postings": int(first.postings)},
        "latest_quarter": {"label": last.quarter_label, "share": _f(last.genai_share), "postings": int(last.postings)},
        "peak_quarter": quarterly.loc[quarterly.genai_share.idxmax(), ["quarter_label", "genai_share"]].to_dict(),
        "raw_median_premium_since_2023": {
            "genai_median": _f(raw_g.median()), "other_median": _f(raw_o.median()),
            "genai_n": int(len(raw_g)), "other_n": int(len(raw_o)),
            "pct": _f(raw_g.median() / raw_o.median() - 1) if len(raw_g) >= MIN_SALARY_N else None},
        "adjusted_premium_since_2023": adjusted,
    }


# ---- Q3 --------------------------------------------------------------------------------------------
def q3(conn, model) -> dict:
    by_year = query_df(conn, read_query("modality_year"))
    save(by_year, "q3_modality_year")
    by_q = query_df(conn, read_query("modality_quarter"), {"from_year": 2011})
    save(by_q, "q3_modality_quarter")
    latest_year = int(by_year.year.max())
    latest = by_year[by_year.year == latest_year].set_index("work_modality")
    raw = {m: {"postings": int(r.postings), "salary_n": int(r.salary_n),
               "median": _median_or_none(r.median_salary, r.salary_n)} for m, r in latest.iterrows()}
    remotive = query_df(conn, read_query("remotive_salary")).iloc[0]
    return {
        "latest_year": latest_year, "raw_latest_year": raw,
        "adjusted_vs_hybrid": {m: term(model, f"modality[{m}]") for m in ("Remote", "Onsite", "Unspecified")} if model else None,
        "model_n_obs": model["n_obs"] if model else None,
        "remotive_sanity_check": {"postings": int(remotive.postings), "salary_n": int(remotive.salary_n),
                                  "median": _f(remotive.median_salary),
                                  "note": "Remotive snapshot is small and delayed; used only as a sanity check"},
    }


# ---- Q4 --------------------------------------------------------------------------------------------
def clusters_for(pairs: pd.DataFrame) -> list[dict]:
    g = nx.Graph()
    for r in pairs.itertuples():
        if r.lift > 1:
            g.add_edge(r.skill_a, r.skill_b, weight=float(r.lift))
    if g.number_of_edges() == 0:
        return []
    comms = nx.community.louvain_communities(g, weight="weight", seed=CLUSTER_SEED)
    lift = {(r.skill_a, r.skill_b): r.lift for r in pairs.itertuples()}
    out = []
    for c in sorted(comms, key=len, reverse=True):
        if len(c) < 2:
            continue
        members = sorted(c)
        vals = [lift[(a, b)] for a, b in itertools.combinations(members, 2) if (a, b) in lift and lift[(a, b)] > 1]
        out.append({"size": len(members), "members": members, "edges": len(vals),
                    "mean_pairwise_lift": float(np.mean(vals)) if vals else None})
    return out


def comembership(clusters: list[dict]) -> set:
    return {tuple(p) for c in clusters for p in itertools.combinations(c["members"], 2)}


def q4(conn) -> dict:
    overall = query_df(conn, read_query("skill_pairs"), {"year": None, "min_count": 0, "min_lift": 0, "limit": 1_000_000})
    save(overall.head(200), "q4_top_pairs")
    clusters = clusters_for(overall)
    save(pd.DataFrame([{**c, "members": "; ".join(c["members"])} for c in clusters]), "q4_clusters")
    years = query_df(conn, read_query("pair_years")).year.tolist()
    yearly, prev, stability = {}, None, []
    by_year_n = query_df(conn, read_query("postings_by_year")).set_index("year").postings.to_dict()
    for y in years:
        if by_year_n.get(y, 0) < CLUSTER_MIN_YEAR_POSTINGS:
            continue
        p = query_df(conn, read_query("skill_pairs"), {"year": int(y), "min_count": 0, "min_lift": 0, "limit": 1_000_000})
        cl = clusters_for(p)
        yearly[int(y)] = cl
        cm = comembership(cl)
        if prev is not None:
            union = prev[1] | cm
            stability.append({"from": prev[0], "to": int(y), "jaccard_comembership": len(prev[1] & cm) / len(union) if union else None})
        prev = (int(y), cm)
    save(pd.DataFrame([{"year": y, **c, "members": "; ".join(c["members"])} for y, cl in yearly.items() for c in cl]),
         "q4_clusters_yearly")
    save(pd.DataFrame(stability), "q4_cluster_stability")
    top = overall.sort_values(["lift", "pair_count"], ascending=False).head(10)
    return {
        "n_pairs_overall": int(len(overall)), "min_pair_count": int(overall.pair_count.min()) if len(overall) else None,
        "n_postings_with_skill": int(overall.n_postings.iloc[0]) if len(overall) else None,
        "top_pairs_by_lift": top[["skill_a", "skill_b", "pair_count", "support", "confidence_a_to_b",
                                  "confidence_b_to_a", "lift"]].to_dict("records"),
        "clusters_overall": clusters,
        "clusters_by_year_count": {y: len(c) for y, c in yearly.items()},
        "cluster_stability": stability,
        "method": "Louvain (networkx, seed %d) on the lift > 1 graph weighted by lift; pairs need n_ab >= the "
                  "build's min_pair_count; yearly clusters only for years with >= %d postings"
                  % (CLUSTER_SEED, CLUSTER_MIN_YEAR_POSTINGS),
    }


def main() -> None:
    model = None
    try:
        model = load_model()
    except FileNotFoundError:
        log.warning("results/salary_model.json missing; run analysis.fit_salary_model first")
    conn = connect()
    try:
        findings = {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "data_volume": volume(conn),
            "q1_skill_premium": q1(conn, model),
            "q2_genai_shift": q2(conn),
            "q3_remote_penalty": q3(conn, model),
            "q4_skill_clusters": q4(conn),
            "salary_model": {k: model[k] for k in ("fitted_at", "n_obs", "r2", "adj_r2", "latest_year")} if model else None,
        }
    finally:
        conn.close()
    atomic_write_json(config.RESULTS_DIR / "findings.json", findings)
    log.info("wrote %s", config.RESULTS_DIR / "findings.json")
    print(json.dumps(findings["data_volume"], indent=1, default=str))


if __name__ == "__main__":
    main()
