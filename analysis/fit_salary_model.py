# Origin: original
"""OLS salary model behind the estimator and the regression-adjusted remote gap (Q3).

ln(salary_avg_usd) = b0 + sum(skill dummies) + modality dummies (base: Hybrid)
                     + experience dummies (base: Mid) + year fixed effects (base: first year) + e
Robust HC1 standard errors (statsmodels). Sample: real HN postings with an in-range annual USD
salary (flag 'ok'; hourly contract rates excluded). Skills: the top-N skills by salaried postings
plus every skill named in the research questions that has >= MIN_SKILL_OBS salaried postings.

Writes results/salary_model.json (coefficients, HC1 covariance, residual s.d., metadata),
results/salary_model_coefficients.csv, and replaces the salary_model_coefficients table.
These describe posted salaries, not causal effects.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

import numpy as np
import pandas as pd
import statsmodels
import statsmodels.api as sm

from src.common import config
from src.common.db import query_df, read_query
from src.common.log import get_logger
from src.common.paths import atomic_write_json
from src.common.salary_model import term_name
from src.common.taxonomy import GENAI_FRAMEWORKS, LEGACY_STACKS, MODERN_SYSTEMS
from src.load.load_postgres import connect

log = get_logger("fit_salary_model")

MODALITY_BASE, MODALITIES = "Hybrid", ["Remote", "Onsite", "Unspecified"]
LEVEL_BASE, LEVELS = "Mid", ["Junior", "Senior", "Staff_Principal", "Unspecified"]
DEFAULT_TOP_N = 30
MIN_SKILL_OBS = 30
FORCED_SKILLS = [*MODERN_SYSTEMS, *LEGACY_STACKS, *GENAI_FRAMEWORKS]


def build_design(sample: pd.DataFrame, skill_rows: pd.DataFrame, top_n: int):
    salaried_ids = set(sample["posting_id"])
    skill_rows = skill_rows[skill_rows["posting_id"].isin(salaried_ids)]
    counts = skill_rows.groupby("skill_name")["posting_id"].nunique().sort_values(ascending=False)
    eligible = counts[counts >= MIN_SKILL_OBS]
    skills = list(eligible.index[:top_n])
    skills += [s for s in FORCED_SKILLS if s in eligible.index and s not in skills]

    X = pd.DataFrame(index=sample["posting_id"].to_numpy())
    X["const"] = 1.0
    dummies = (skill_rows[skill_rows["skill_name"].isin(skills)]
               .assign(v=1.0).pivot_table(index="posting_id", columns="skill_name", values="v", aggfunc="max"))
    for s in skills:
        X[term_name("skill", s)] = dummies[s].reindex(X.index).fillna(0.0).to_numpy() if s in dummies else 0.0
    mod = sample.set_index("posting_id")["work_modality"].reindex(X.index)
    lvl = sample.set_index("posting_id")["experience_level"].reindex(X.index)
    yr = sample.set_index("posting_id")["year"].reindex(X.index).astype(int)
    for m in MODALITIES:
        X[term_name("modality", m)] = (mod == m).astype(float).to_numpy()
    for lv in LEVELS:
        X[term_name("level", lv)] = (lvl == lv).astype(float).to_numpy()
    years = sorted(yr.unique())
    for y in years[1:]:
        X[term_name("year", y)] = (yr == y).astype(float).to_numpy()
    y_log = np.log(sample.set_index("posting_id")["salary"].reindex(X.index).astype(float))
    # drop all-zero columns (e.g. a level absent from the sample) so the design has full rank
    X = X.loc[:, (X != 0).any(axis=0)]
    return X, y_log, skills, years, counts


def fit(top_n: int = DEFAULT_TOP_N) -> dict:
    conn = connect()
    try:
        sample = query_df(conn, read_query("model_sample"))
        skill_rows = query_df(conn, read_query("model_sample_skills"))
    finally:
        conn.close()
    if len(sample) < 100:
        raise SystemExit(f"only {len(sample)} salaried postings; load the warehouse first")
    X, y, skills, years, counts = build_design(sample, skill_rows, top_n)
    res = sm.OLS(y.to_numpy(), X.to_numpy()).fit(cov_type="HC1")
    terms = list(X.columns)
    ci = res.conf_int(alpha=0.05)
    rows = [{"term": t, "coef": float(res.params[i]), "std_err": float(res.bse[i]),
             "p_value": float(res.pvalues[i]), "ci_low": float(ci[i][0]), "ci_high": float(ci[i][1]),
             "pct_effect": float(np.expm1(res.params[i])) if t != "const" else None}
            for i, t in enumerate(terms)]
    fitted_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    model = {
        "fitted_at": fitted_at.isoformat(),
        "dependent": "ln(salary_avg_usd)",
        "estimator": f"OLS, HC1 robust standard errors (statsmodels {statsmodels.__version__})",
        "sample": "HN postings, source_type='hn', salary_currency='USD', salary_flag='ok' (hourly excluded)",
        "n_obs": int(res.nobs), "r2": float(res.rsquared), "adj_r2": float(res.rsquared_adj),
        "resid_std": float(np.sqrt(res.mse_resid)),
        "base_levels": {"modality": MODALITY_BASE, "experience_level": LEVEL_BASE, "year": int(years[0])},
        "skills": skills, "skill_salaried_counts": {s: int(counts[s]) for s in skills},
        "modalities": [MODALITY_BASE, *MODALITIES], "experience_levels": [LEVEL_BASE, *LEVELS],
        "years": [int(y_) for y_ in years], "latest_year": int(years[-1]),
        "top_n": top_n, "min_skill_obs": MIN_SKILL_OBS,
        "terms": rows,
        "term_order": terms,
        "cov_hc1": [[float(v) for v in row] for row in np.asarray(res.cov_params())],
    }
    atomic_write_json(config.RESULTS_DIR / "salary_model.json", model)
    pd.DataFrame(rows).to_csv(config.RESULTS_DIR / "salary_model_coefficients.csv", index=False)

    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM salary_model_coefficients")
            cur.executemany(
                "INSERT INTO salary_model_coefficients (term, coef, std_err, p_value, n_obs, fitted_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [(r["term"], r["coef"], r["std_err"], r["p_value"], model["n_obs"], fitted_at) for r in rows])
    finally:
        conn.close()
    log.info("fitted on n=%d, R2=%.3f, %d terms (%d skills); latest year %d",
             model["n_obs"], model["r2"], len(terms), len(skills), model["latest_year"])
    return model


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    args = ap.parse_args()
    m = fit(args.top_n)
    print(json.dumps({k: m[k] for k in ("n_obs", "r2", "adj_r2", "latest_year", "skills")}, indent=1))


if __name__ == "__main__":
    main()
