# Origin: original
"""Prediction from the fitted salary model (results/salary_model.json); numpy only, so the
dashboard does not need statsmodels. The model itself is fitted by analysis/fit_salary_model.py."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from src.common import config

Z95 = 1.959963984540054


def term_name(kind: str, value) -> str:
    return f"{kind}[{value}]"


def load_model(path: Path | None = None) -> dict:
    return json.loads(Path(path or config.RESULTS_DIR / "salary_model.json").read_text())


def predict(model: dict, skills: list[str], modality: str, level: str, year: int | None = None) -> dict:
    """exp(x'b): the model's geometric-mean salary for these inputs, with a 95% CI for that mean
    (HC1 covariance) and a 95% prediction interval for a single posting (adds residual variance)."""
    order = model["term_order"]
    x = np.zeros(len(order))
    x[order.index("const")] = 1.0
    wanted = [term_name("skill", s) for s in skills]
    wanted += [term_name("modality", modality), term_name("level", level),
               term_name("year", year or model["latest_year"])]
    for t in wanted:
        if t in order:
            x[order.index(t)] = 1.0
    beta = np.array([r["coef"] for r in model["terms"]])
    cov = np.array(model["cov_hc1"])
    mu = float(x @ beta)
    se = float(np.sqrt(x @ cov @ x))
    se_pred = float(np.sqrt(se ** 2 + model["resid_std"] ** 2))
    return {"estimate": float(np.exp(mu)),
            "ci_low": float(np.exp(mu - Z95 * se)), "ci_high": float(np.exp(mu + Z95 * se)),
            "pi_low": float(np.exp(mu - Z95 * se_pred)), "pi_high": float(np.exp(mu + Z95 * se_pred)),
            "log_mean": mu, "se_log_mean": se}
