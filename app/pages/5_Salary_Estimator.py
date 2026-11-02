# Origin: original
"""Salary estimator: exp(b0 + sum b) from the fitted OLS model (results/salary_model.json)."""
import numpy as np
import streamlit as st

from app.db import results_json
from app.style import footer
from src.common.salary_model import predict, term_name

st.set_page_config(page_title="Salary estimator", layout="wide")
st.title("Salary estimator")
model = results_json("salary_model.json")
if not model:
    st.warning("Salary model not fitted yet (run `make analysis`).")
    st.stop()

c = st.columns(3)
skills = c[0].multiselect("Skills", sorted(model["skills"]), default=[s for s in ("Python", "Kubernetes") if s in model["skills"]])
modality = c[1].selectbox("Work modality", model["modalities"], index=model["modalities"].index("Remote")
                          if "Remote" in model["modalities"] else 0)
level = c[2].selectbox("Experience level", model["experience_levels"],
                       index=model["experience_levels"].index("Senior") if "Senior" in model["experience_levels"] else 0)
p = predict(model, skills, modality, level)

m = st.columns(3)
m[0].metric(f"Estimated posted salary ({model['latest_year']})", f"${p['estimate']:,.0f}")
m[1].metric("95% CI for the model mean", f"${p['ci_low']:,.0f} – ${p['ci_high']:,.0f}")
m[2].metric("95% range for one posting", f"${p['pi_low']:,.0f} – ${p['pi_high']:,.0f}")
st.caption(f"OLS on ln(salary) with HC1 robust SEs, n = {model['n_obs']:,} salaried HN postings, R² = {model['r2']:.2f}, "
           f"fitted {model['fitted_at']}. Base levels: {model['base_levels']['modality']} modality, "
           f"{model['base_levels']['experience_level']} level; uses the {model['latest_year']} year effect. "
           "The estimate is exp(x′β) (a geometric-mean posted salary). It describes what postings advertise, "
           "not what any person would be paid, and the coefficients are associations, not causal effects.")

terms = {r["term"]: r for r in model["terms"]}
used = ["const"] + [term_name("skill", s) for s in skills] + [term_name("modality", modality), term_name("level", level),
                                                             term_name("year", model["latest_year"])]
rows = [{"term": t, "coefficient": round(terms[t]["coef"], 4),
         "effect": "" if t == "const" else f"{np.expm1(terms[t]['coef']):+.1%}",
         "95% CI": "" if t == "const" else f"{np.expm1(terms[t]['ci_low']):+.1%} to {np.expm1(terms[t]['ci_high']):+.1%}",
         "p": round(terms[t]["p_value"], 4)} for t in used if t in terms]
st.subheader("Terms used")
st.dataframe(rows, hide_index=True, use_container_width=True)
footer()
