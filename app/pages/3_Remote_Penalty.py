# Origin: original
"""Q3: Remote vs Hybrid vs Onsite pay, raw and regression-adjusted."""
import numpy as np
import plotly.graph_objects as go
import streamlit as st

from app.db import results_json, run_query
from app.style import TEXT, color_map, footer, layout, symbol_map

st.set_page_config(page_title="Remote penalty", layout="wide")
st.title("Remote vs hybrid vs onsite pay")
MODES = ["Remote", "Hybrid", "Onsite", "Unspecified"]
colors, symbols = color_map(MODES), symbol_map(MODES)

model = results_json("salary_model.json")
if model:
    rows = [r for r in model["terms"] if r["term"].startswith("modality[")]
    labels = [r["term"][9:-1] for r in rows]
    eff = [np.expm1(r["coef"]) for r in rows]
    lo = [np.expm1(r["ci_low"]) for r in rows]
    hi = [np.expm1(r["ci_high"]) for r in rows]
    fig = go.Figure(go.Bar(
        x=labels, y=eff, marker_color=[colors[m] for m in labels], width=0.4,
        error_y={"type": "data", "symmetric": False, "array": [h - e for h, e in zip(hi, eff)],
                 "arrayminus": [e - l for e, l in zip(eff, lo)], "color": TEXT, "thickness": 1.5},
        hovertemplate="%{x}: %{y:+.1%} vs Hybrid<extra></extra>"))
    fig.add_hline(y=0, line_color="#c3c2b7", line_width=1)
    fig.update_layout(hovermode="closest")
    st.plotly_chart(layout(fig, "Regression-adjusted pay difference vs Hybrid (95% CI)", "Difference in posted pay",
                           y_format="+.0%"), use_container_width=True)
    st.caption(f"OLS on ln(salary) with {len(model['skills'])} skill dummies, experience level and year fixed "
               f"effects; HC1 robust SEs; n = {model['n_obs']:,} salaried postings; fitted {model['fitted_at']}. "
               "Bars are exp(β) − 1. Associations in postings, not causal effects.")
else:
    st.warning("Salary model not fitted yet (run `make analysis`).")

y = run_query("modality_year")
min_n = st.slider("Hide medians with fewer salaried postings than", 5, 100, 30, step=5)
fig = go.Figure()
for m in MODES:
    d = y[(y.work_modality == m) & (y.salary_n >= min_n)]
    fig.add_trace(go.Scatter(x=d.year, y=d.median_salary, name=m, mode="lines+markers",
                             line={"width": 2, "color": colors[m]}, marker={"size": 8, "symbol": symbols[m]},
                             customdata=d.salary_n, hovertemplate=f"{m}: $%{{y:,.0f}} (n=%{{customdata}})<extra></extra>"))
st.plotly_chart(layout(fig, "Raw median posted salary by modality", "Median salary", y_format="$,.0f"),
                use_container_width=True)

fig = go.Figure()
for m in MODES:
    d = y[y.work_modality == m]
    tot = y.groupby("year").postings.transform("sum")[d.index]
    fig.add_trace(go.Scatter(x=d.year, y=d.postings / tot, name=m, mode="lines+markers",
                             line={"width": 2, "color": colors[m]}, marker={"size": 8, "symbol": symbols[m]},
                             hovertemplate=f"{m}: %{{y:.0%}}<extra></extra>"))
st.plotly_chart(layout(fig, "Share of postings by modality", "Share of postings", y_format=".0%"),
                use_container_width=True)
with st.expander("Quarterly table (materialized view)"):
    st.dataframe(run_query("modality_quarter", {"from_year": 2011}), hide_index=True, use_container_width=True)
footer()
