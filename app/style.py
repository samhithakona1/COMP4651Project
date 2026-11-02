# Origin: original
"""Shared chart styling: fixed categorical slots (validated reference palette, light surface),
thin marks, recessive axes, one y-axis per chart."""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SYMBOLS = ["circle", "square", "diamond", "triangle-up", "x", "star", "hexagon", "cross"]
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e6e5e0"
ATTRIBUTION = "Job data: Hacker News “Who is hiring?” threads (via the HN Firebase/Algolia APIs) and Remotive (https://remotive.com)."


def color_map(entities: list[str]) -> dict[str, str]:
    """Colour follows the entity: slot by position in a stable list, never by current rank."""
    return {e: SERIES[i % len(SERIES)] for i, e in enumerate(entities)}


def symbol_map(entities: list[str]) -> dict[str, str]:
    return {e: SYMBOLS[i % len(SYMBOLS)] for i, e in enumerate(entities)}


def layout(fig: go.Figure, title: str, y_title: str, x_title: str = "", y_format: str | None = None,
           height: int = 380) -> go.Figure:
    fig.update_layout(
        title={"text": title, "x": 0, "font": {"size": 16, "color": TEXT}},
        template="plotly_white", height=height, margin={"l": 10, "r": 10, "t": 50, "b": 10},
        legend={"orientation": "h", "y": -0.18, "font": {"color": MUTED}},
        hovermode="x unified", font={"color": MUTED},
    )
    fig.update_xaxes(title=x_title, showgrid=False, linecolor=GRID)
    fig.update_yaxes(title=y_title, gridcolor=GRID, zeroline=False, tickformat=y_format)
    return fig


def footer() -> None:
    st.caption(ATTRIBUTION + " Figures describe job postings, not causal effects.")
