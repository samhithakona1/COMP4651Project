# Origin: original
"""Q2: quarterly GenAI-framework penetration and pay premium since 2023."""
import plotly.graph_objects as go
import streamlit as st

from app.db import results_json, run_query
from app.style import SERIES, color_map, footer, layout, symbol_map
from src.common.taxonomy import GENAI_FRAMEWORKS

st.set_page_config(page_title="GenAI shift", layout="wide")
st.title("The GenAI shift")
st.write("Frameworks tracked: " + ", ".join(GENAI_FRAMEWORKS) + ".")
from_year = st.slider("From year", 2019, 2026, 2023)
params = {"skills": list(GENAI_FRAMEWORKS), "from_year": from_year}
q = run_query("genai_quarterly", params)
q["label"] = q.year.astype(str) + " Q" + q.quarter.astype(str)

f = results_json("findings.json") or {}
adj = (f.get("q2_genai_shift") or {}).get("adjusted_premium_since_2023")
c = st.columns(3)
if len(q):
    c[0].metric(f"Share of postings, {q.label.iloc[-1]}", f"{q.genai_share.iloc[-1]:.1%}",
                f"{(q.genai_share.iloc[-1] - q.genai_share.iloc[0]) * 100:+.1f} pts since {q.label.iloc[0]}", delta_color="off")
if adj:
    c[1].metric("Adjusted pay premium (2023+)", f"{adj['pct_effect']:+.1%}",
                f"95% CI {adj['pct_ci_low']:+.1%} to {adj['pct_ci_high']:+.1%}", delta_color="off")
    c[2].metric("Salaried postings in that model", f"{adj['n_obs']:,}", f"{adj['n_genai']:,} mention GenAI", delta_color="off")
    st.caption("Adjusted premium: OLS of ln(salary) on a GenAI indicator with experience-level, modality and year "
               "controls (HC1 SEs), computed by analysis/findings.py.")

fig = go.Figure(go.Scatter(x=q.label, y=q.genai_share, mode="lines+markers", name="Any tracked framework",
                           line={"width": 2, "color": SERIES[0]}, marker={"size": 8}, customdata=q.genai_postings,
                           hovertemplate="%{y:.1%} (%{customdata} postings)<extra></extra>"))
st.plotly_chart(layout(fig, "Share of postings mentioning any tracked GenAI framework", "Share", y_format=".1%"),
                use_container_width=True)

fw = run_query("genai_by_framework", params)
fw["label"] = fw.year.astype(str) + " Q" + fw.quarter.astype(str)
colors, symbols = color_map(list(GENAI_FRAMEWORKS)), symbol_map(list(GENAI_FRAMEWORKS))
fig = go.Figure()
for s in GENAI_FRAMEWORKS:
    d = fw[fw.skill_name == s]
    fig.add_trace(go.Scatter(x=d.label, y=d.share, name=s, mode="lines+markers", line={"width": 2, "color": colors[s]},
                             marker={"size": 8, "symbol": symbols[s]},
                             hovertemplate=f"{s}: %{{y:.1%}}<extra></extra>"))
st.plotly_chart(layout(fig, "Share of postings by framework", "Share", y_format=".1%"), use_container_width=True)

min_n = 20
d = q[(q.genai_salary_n >= min_n) & (q.other_salary_n >= min_n)]
fig = go.Figure()
fig.add_trace(go.Scatter(x=d.label, y=d.genai_median, name="Mentions a GenAI framework", mode="lines+markers",
                         line={"width": 2, "color": SERIES[0]}, marker={"size": 8}, customdata=d.genai_salary_n,
                         hovertemplate="$%{y:,.0f} (n=%{customdata})<extra>GenAI</extra>"))
fig.add_trace(go.Scatter(x=d.label, y=d.other_median, name="Other postings", mode="lines+markers",
                         line={"width": 2, "color": SERIES[1]}, marker={"size": 8, "symbol": "square"},
                         customdata=d.other_salary_n, hovertemplate="$%{y:,.0f} (n=%{customdata})<extra>Other</extra>"))
st.plotly_chart(layout(fig, f"Median posted salary by quarter (quarters with ≥ {min_n} salaried postings in both groups)",
                       "Median salary", y_format="$,.0f"), use_container_width=True)
with st.expander("Table"):
    st.dataframe(q, hide_index=True, use_container_width=True)
footer()
