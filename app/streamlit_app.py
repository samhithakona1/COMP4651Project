# Origin: original
"""Dashboard home: data volume and navigation. Pages live in app/pages/."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app.db import results_json, run_query  # noqa: E402
from app.style import SERIES, footer, layout  # noqa: E402

st.set_page_config(page_title="Tech Hiring Trends", layout="wide")
st.title("Tech hiring trends & skill premiums")
st.write("Hacker News “Who is hiring?” postings, 2011 onwards: skill demand, pay, GenAI adoption, "
         "remote-work pay gaps and skill clusters. Use the pages in the sidebar.")

ov = run_query("overview").iloc[0]
c = st.columns(4)
c[0].metric("Postings (HN)", f"{int(ov.postings):,}")
c[1].metric("With a parsed USD salary", f"{int(ov.with_salary):,}", f"{ov.with_salary / max(ov.postings, 1):.0%} of postings",
            delta_color="off")
c[2].metric("With ≥ 2 skills", f"{int(ov.with_ge2_skills):,}")
c[3].metric("Companies", f"{int(ov.companies):,}")
st.caption(f"Posting dates {ov.first_date} → {ov.last_date}.")

by_year = run_query("postings_by_year")
fig = go.Figure(go.Bar(x=by_year.year, y=by_year.postings, marker_color=SERIES[0], name="Postings",
                       hovertemplate="%{x}: %{y:,} postings<extra></extra>"))
st.plotly_chart(layout(fig, "Postings per year", "Postings"), use_container_width=True)

bf = results_json("ingest/backfill_summary.json")
if bf:
    st.caption(f"Ingest: {bf['months_with_thread']} of {bf['months_requested']} months had a thread; "
               f"{bf['total_comments_fetched']:,} comments fetched, {bf['total_postings_kept']:,} top-level postings kept. "
               f"Months without a qualifying thread: {', '.join(bf['months_missing_thread']) or 'none'}.")
with st.expander("Table: postings per year"):
    st.dataframe(by_year, hide_index=True, use_container_width=True)
footer()
