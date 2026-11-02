# Origin: original
"""Q1: posting share and median pay by year for selected skills."""
import plotly.graph_objects as go
import streamlit as st

from app.db import run_query
from app.style import color_map, footer, layout, symbol_map
from src.common.taxonomy import LEGACY_STACKS, MODERN_SYSTEMS

st.set_page_config(page_title="Skill trends", layout="wide")
st.title("Skill trends: modern systems vs legacy stacks")

skills_all = run_query("skill_list")
names = skills_all.skill_name.tolist()
default = [s for s in (*MODERN_SYSTEMS, *LEGACY_STACKS) if s in names]
picked = st.multiselect("Skills (up to 8)", names, default=default, max_selections=8)
min_n = st.slider("Hide medians with fewer salaried postings than", 5, 100, 20, step=5)
if not picked:
    st.info("Pick at least one skill.")
    st.stop()

df = run_query("skill_trends", {"skills": picked})
colors, symbols = color_map(picked), symbol_map(picked)

share = go.Figure()
pay = go.Figure()
for s in picked:
    d = df[df.skill_name == s]
    share.add_trace(go.Scatter(x=d.year, y=d.posting_share, name=s, mode="lines+markers",
                               line={"width": 2, "color": colors[s]}, marker={"size": 8, "symbol": symbols[s]},
                               hovertemplate=f"{s}: %{{y:.1%}} of postings<extra></extra>"))
    d = d[d.salary_n >= min_n]
    pay.add_trace(go.Scatter(x=d.year, y=d.median_salary, name=s, mode="lines+markers",
                             line={"width": 2, "color": colors[s]}, marker={"size": 8, "symbol": symbols[s]},
                             customdata=d.salary_n,
                             hovertemplate=f"{s}: $%{{y:,.0f}} (n=%{{customdata}})<extra></extra>"))
c1, c2 = st.columns(2)
c1.plotly_chart(layout(share, "Share of postings mentioning the skill", "Share of postings", y_format=".0%"),
                use_container_width=True)
c2.plotly_chart(layout(pay, "Median posted salary (USD, annual)", "Median salary", y_format="$,.0f"),
                use_container_width=True)

groups = run_query("skill_group_year", {"group_a": list(MODERN_SYSTEMS), "group_b": list(LEGACY_STACKS)})
g = groups[groups.salary_n >= min_n]
fig = go.Figure()
for grp, label, i in (("A", "Modern: " + ", ".join(MODERN_SYSTEMS), 0), ("B", "Legacy: " + ", ".join(LEGACY_STACKS), 1)):
    d = g[g.grp == grp]
    fig.add_trace(go.Scatter(x=d.year, y=d.median_salary, name=label, mode="lines+markers",
                             line={"width": 2, "color": ["#2a78d6", "#eb6834"][i]},
                             marker={"size": 8, "symbol": ["circle", "square"][i]}, customdata=d.salary_n,
                             hovertemplate="$%{y:,.0f} (n=%{customdata})<extra>" + label + "</extra>"))
st.plotly_chart(layout(fig, "Median pay: postings with any modern vs any legacy skill", "Median salary",
                       y_format="$,.0f"), use_container_width=True)
with st.expander("Table"):
    st.dataframe(df, hide_index=True, use_container_width=True)
footer()
