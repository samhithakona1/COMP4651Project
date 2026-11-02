# Origin: original
"""Q4: force-directed network of high-lift skill pairs, by year."""
import networkx as nx
import plotly.graph_objects as go
import streamlit as st

from app.db import results_json, run_query
from app.style import MUTED, SERIES, SYMBOLS, footer, layout

st.set_page_config(page_title="Skill network", layout="wide")
st.title("Skill co-occurrence network")
CATEGORIES = ["Languages", "Frameworks/Web", "Cloud/DevOps", "Data", "AI/ML"]

years = run_query("pair_years").year.astype(int).tolist()
options = ["All years"] + [str(y) for y in years]
choice = st.select_slider("Year", options=options, value="All years")
c = st.columns(3)
min_lift = c[0].slider("Minimum lift", 1.0, 10.0, 2.0, 0.5)
min_count = c[1].slider("Minimum co-occurring postings", 30, 300, 30, 10)
top_k = c[2].slider("Edges shown (highest lift)", 20, 200, 80, 10)
year = None if choice == "All years" else int(choice)
pairs = run_query("skill_pairs", {"year": year, "min_count": min_count, "min_lift": min_lift, "limit": top_k})
if pairs.empty:
    st.info("No pairs pass these thresholds.")
    st.stop()

g = nx.Graph()
cat = {}
for r in pairs.itertuples():
    g.add_edge(r.skill_a, r.skill_b, lift=r.lift, n=r.pair_count)
    cat[r.skill_a], cat[r.skill_b] = r.category_a, r.category_b
pos = nx.spring_layout(g, weight="lift", seed=4651, k=1.2 / max(1, len(g) ** 0.5), iterations=200)

edge_x, edge_y = [], []
for a, b in g.edges():
    edge_x += [pos[a][0], pos[b][0], None]
    edge_y += [pos[a][1], pos[b][1], None]
fig = go.Figure(go.Scatter(x=edge_x, y=edge_y, mode="lines", line={"width": 1, "color": "#c3c2b7"},
                           hoverinfo="skip", showlegend=False))
for i, cname in enumerate(CATEGORIES):
    nodes = [n for n in g if cat[n] == cname]
    if not nodes:
        continue
    deg = [g.degree(n) for n in nodes]
    fig.add_trace(go.Scatter(
        x=[pos[n][0] for n in nodes], y=[pos[n][1] for n in nodes], mode="markers+text", name=cname,
        text=nodes, textposition="top center", textfont={"size": 11, "color": MUTED},
        marker={"size": [10 + 2 * d for d in deg], "color": SERIES[i], "symbol": SYMBOLS[i],
                "line": {"width": 2, "color": "#fcfcfb"}},
        customdata=deg, hovertemplate="%{text}: %{customdata} links<extra>" + cname + "</extra>"))
fig.update_xaxes(visible=False)
fig.update_yaxes(visible=False)
fig = layout(fig, f"High-lift skill pairs — {choice}", "", height=640)
fig.update_layout(hovermode="closest")
fig.update_xaxes(visible=False)
fig.update_yaxes(visible=False)
st.plotly_chart(fig, use_container_width=True)
st.caption("lift = N·n_ab / (n_a·n_b), N = postings with ≥ 1 skill in the scope; lift > 1 means the skills appear "
           "together more often than independence predicts. Layout: networkx spring (force-directed) weighted by lift.")
st.dataframe(pairs, hide_index=True, use_container_width=True)

f = results_json("findings.json")
if f and f.get("q4_skill_clusters", {}).get("clusters_overall"):
    st.subheader("Clusters (Louvain, all years)")
    st.dataframe([{"size": c["size"], "mean pairwise lift": round(c["mean_pairwise_lift"] or 0, 2),
                   "members": ", ".join(c["members"])} for c in f["q4_skill_clusters"]["clusters_overall"]],
                 hide_index=True, use_container_width=True)
footer()
