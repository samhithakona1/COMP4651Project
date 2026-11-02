# Origin: original
"""Regenerate every report/slide chart from results/ (CSV/JSON only) -> results/figures/*.png.
Also writes the benchmark summary tables the report quotes:
  results/bench/spark_vs_pandas_summary.csv   median wall-clock, peak RSS, speedup, status per scale
  results/bench/query_latency_summary.csv     p50 / p90 / p99 per (scale, arm, workload, cache)
  results/bench/summary.json                  crossover point and headline ratios
"""
from __future__ import annotations

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.common import config  # noqa: E402
from src.common.log import get_logger  # noqa: E402
from src.common.paths import atomic_write_json  # noqa: E402

log = get_logger("make_figures")
R = config.RESULTS_DIR
FIG = R / "figures"
# Validated reference palette (light surface), fixed slot order; colour follows the entity.
S = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
MARKERS = ["o", "s", "D", "^", "X", "*", "h", "P"]
TEXT, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlecolor": TEXT, "axes.titlelocation": "left", "font.size": 10, "legend.frameon": False,
    "lines.linewidth": 2, "lines.markersize": 6,
})


def save(fig, name: str) -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIG / f"{name}.png", dpi=160)
    plt.close(fig)
    log.info("wrote figures/%s.png", name)


def scale_rows() -> dict[str, int]:
    m = json.loads((R / "bench" / "synth_manifest.json").read_text())
    return {k: v["rows"] for k, v in m["scales"].items()}


def money(ax):
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v / 1000:,.0f}k"))


# ---- Benchmark 1 -------------------------------------------------------------------------------------
def bench1(summary: dict) -> None:
    path = R / "bench" / "spark_vs_pandas.csv"
    if not path.exists():
        log.warning("no %s", path)
        return
    df = pd.read_csv(path)
    rows = scale_rows()
    df["rows"] = df.scale.map(rows)
    out = []
    for (engine, scale), g in df.groupby(["engine", "scale"]):
        meas = g[g.kind == "measured"]
        ok = meas[meas.status == "ok"]
        status = "ok" if len(ok) else (meas.status.iloc[0] if len(meas) else g.status.iloc[0])
        out.append({"engine": engine, "scale": scale, "rows": rows.get(scale), "is_synthetic": bool(g.is_synthetic.iloc[0]),
                    "measured_runs_ok": len(ok), "status": status, "protocol": g.protocol.iloc[0] if "protocol" in g else "",
                    "median_wall_s": float(ok.wall_s.astype(float).median()) if len(ok) else None,
                    "min_wall_s": float(ok.wall_s.astype(float).min()) if len(ok) else None,
                    "max_wall_s": float(ok.wall_s.astype(float).max()) if len(ok) else None,
                    "median_peak_rss_mb": float(ok.peak_rss_mb.astype(float).median()) if len(ok) else None,
                    "max_peak_rss_mb": float(pd.to_numeric(g.peak_rss_mb, errors="coerce").max()),
                    "container_mem_limit": g.container_mem_limit.iloc[0]})
    s = pd.DataFrame(out).sort_values(["rows", "engine"])
    piv = s.pivot(index="scale", columns="engine", values="median_wall_s")
    s["speedup_pandas_over_spark"] = s.scale.map((piv.get("pandas") / piv.get("spark")).to_dict()) \
        if {"pandas", "spark"} <= set(piv.columns) else None
    s.to_csv(R / "bench" / "spark_vs_pandas_summary.csv", index=False)
    sp = s[s.engine == "spark"].set_index("scale")
    pdx = s[s.engine == "pandas"].set_index("scale")
    order = sorted(set(s.scale), key=lambda k: rows.get(k, 0))
    crossover = next((sc for sc in order if sc in sp.index and sc in pdx.index and sp.median_wall_s.get(sc)
                      and (pdx.median_wall_s.get(sc) is None or np.isnan(pdx.median_wall_s.get(sc))
                           or sp.median_wall_s[sc] < pdx.median_wall_s[sc])), None)
    summary["bench1"] = {"crossover_scale": crossover, "crossover_rows": rows.get(crossover) if crossover else None,
                         "pandas_failures": pdx[pdx.status != "ok"].status.to_dict(),
                         "speedups": s[s.engine == "spark"].set_index("scale").speedup_pandas_over_spark.dropna().to_dict(),
                         "mem_limit": s.container_mem_limit.iloc[0]}

    for metric, label, name in (("median_wall_s", "Median wall-clock (s)", "bench1_wall_time"),
                                ("median_peak_rss_mb", "Median peak RSS (MB)", "bench1_peak_rss")):
        fig, ax = plt.subplots(figsize=(7, 4.2))
        for i, eng in enumerate(["pandas", "spark"]):
            d = s[s.engine == eng].sort_values("rows")
            okd = d[d[metric].notna()]
            ax.plot(okd.rows, okd[metric], marker=MARKERS[i], color=S[i], label=eng.capitalize())
            for r in d[d.status != "ok"].itertuples():
                ax.annotate(f"{eng}: {r.status.upper()} at {r.scale}", (r.rows, ax.get_ylim()[1] if len(okd) else 1),
                            textcoords="offset points", xytext=(-60, -14 - 12 * i), fontsize=8, color=TEXT)
        real = rows.get("real")
        if real:
            ax.axvline(real, color=GRID, lw=1, ls="--")
            ax.annotate("real corpus", (real, ax.get_ylim()[0]), xytext=(4, 4), textcoords="offset points",
                        fontsize=8, color=MUTED)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Postings (log; points above the real corpus are synthetic resamples)")
        ax.set_ylabel(label)
        ax.set_title(f"Benchmark 1 — {label.split(' (')[0].lower()}: Spark vs Pandas")
        ax.legend()
        save(fig, name)


# ---- Benchmark 2 -------------------------------------------------------------------------------------
def bench2(summary: dict) -> None:
    path = R / "bench" / "query_latency.csv"
    if not path.exists():
        log.warning("no %s", path)
        return
    df = pd.read_csv(path)
    rows = scale_rows()
    agg = (df.groupby(["scale", "is_synthetic", "arm", "workload", "cache"]).latency_ms
             .agg(n="count", p50=lambda x: x.quantile(0.5), p90=lambda x: x.quantile(0.9),
                  p99=lambda x: x.quantile(0.99), mean="mean").reset_index())
    agg["rows"] = agg.scale.map(rows)
    agg = agg.sort_values(["cache", "workload", "rows", "arm"])
    agg.to_csv(R / "bench" / "query_latency_summary.csv", index=False)
    ratios = {}
    for (cache, wl, sc), g in agg.groupby(["cache", "workload", "scale"]):
        g = g.set_index("arm")
        if {"star", "flat"} <= set(g.index):
            ratios[f"{cache}/{wl}/{sc}"] = float(g.p50["flat"] / g.p50["star"])
    summary["bench2"] = {"p50_ratio_flat_over_star": ratios}
    names = {"a": "(a) Kubernetes co-skills, remote 2025", "b": "(b) yearly median pay, Rust",
             "c": "(c) remote vs hybrid by quarter"}
    for cache in sorted(agg.cache.unique()):
        fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=True)
        for ax, wl in zip(axes, ["a", "b", "c"]):
            for i, arm in enumerate(["star", "flat"]):
                d = agg[(agg.cache == cache) & (agg.workload == wl) & (agg.arm == arm)].sort_values("rows")
                label = "Indexed star schema" if arm == "star" else "Flat table, no indexes"
                ax.plot(d.rows, d.p50, marker=MARKERS[i], color=S[i], label=label)
                ax.fill_between(d.rows, d.p50, d.p99, color=S[i], alpha=0.12, lw=0)
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.set_title(names[wl], fontsize=10)
            ax.set_xlabel("Postings (log)")
        axes[0].set_ylabel("Latency, ms (line p50, band to p99)")
        axes[0].legend(loc="upper left")
        fig.suptitle(f"Benchmark 2 — query latency, {cache} cache", x=0.01, ha="left", fontweight="bold", color=TEXT)
        save(fig, f"bench2_latency_{cache}")


# ---- memory profile ----------------------------------------------------------------------------------
def memory(summary: dict) -> None:
    path = R / "bench" / "memory_profile.json"
    if not path.exists():
        log.warning("no %s", path)
        return
    m = json.loads(path.read_text())
    stages = list(m["stages"])
    services = ["postgres", "airflow-scheduler", "airflow-webserver", "streamlit"]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    x = np.arange(len(stages))
    w = 0.2
    for i, sv in enumerate(services):
        vals = [m["stages"][st]["peak_mb_by_service"].get(sv, 0) for st in stages]
        ax.bar(x + (i - 1.5) * w, vals, w - 0.02, color=S[i], label=sv)
    for j, st in enumerate(stages):
        t = m["stages"][st]["peak_total_mb"]
        ax.annotate(f"total {t:,.0f} MB", (j, max(m['stages'][st]['peak_mb_by_service'].values())), xytext=(0, 6),
                    textcoords="offset points", ha="center", fontsize=8, color=TEXT)
    ax.axhline(3584, color=MUTED, ls="--", lw=1)
    ax.annotate("3.5 GB budget (applies to the total)", (len(stages) - 0.5, 3584), xytext=(0, 4),
                textcoords="offset points", ha="right", fontsize=8, color=MUTED)
    ax.set_xticks(x, stages)
    ax.set_ylabel("Peak memory (MB, docker stats)")
    ax.set_title("Memory profile: peak per service in each stage")
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.1), fontsize=8)
    save(fig, "memory_profile")
    summary["memory"] = {"overall_peak_total_mb": m["overall_peak_total_mb"],
                         "streamlit_steady_state_mb": m.get("streamlit_steady_state_mb")}


# ---- findings ----------------------------------------------------------------------------------------
def findings() -> None:
    F = R / "findings"
    if not (F / "q1_skill_year.csv").exists():
        log.warning("no findings tables")
        return
    q1 = pd.read_csv(F / "q1_skill_year.csv")
    skills = ["Rust", "Go", "Kubernetes", "PHP", "Ruby on Rails", "Java Spring"]
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    for i, sk in enumerate(skills):
        d = q1[q1.skill_name == sk]
        ax.plot(d.year, d.posting_share * 100, marker=MARKERS[i], color=S[i], label=sk, markersize=5)
    ax.set_ylabel("% of postings")
    ax.set_title("Q1 — share of HN postings mentioning each skill")
    ax.legend(ncol=3, fontsize=8)
    save(fig, "q1_skill_share")

    g = pd.read_csv(F / "q1_group_year.csv").dropna(subset=["modern_median", "legacy_median"], how="all")
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.plot(g.year, g.modern_median, marker="o", color=S[0], label="Any of Rust / Go / Kubernetes")
    ax.plot(g.year, g.legacy_median, marker="s", color=S[1], label="Any of PHP / Rails / Java Spring")
    money(ax)
    ax.set_ylabel("Median posted salary (USD)")
    ax.set_title("Q1 — median posted pay, modern vs legacy stacks (years with ≥ 20 salaried postings)")
    ax.legend(fontsize=8)
    save(fig, "q1_group_median_pay")

    q2 = pd.read_csv(F / "q2_genai_quarterly.csv")
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.plot(q2.quarter_label, q2.genai_share * 100, marker="o", color=S[0], label="Any tracked framework")
    ax.set_ylabel("% of postings")
    ax.set_title("Q2 — postings mentioning vLLM, LangChain, LlamaIndex, PyTorch, Hugging Face or Triton")
    ax.tick_params(axis="x", rotation=45)
    save(fig, "q2_genai_share")

    model = json.loads((R / "salary_model.json").read_text())
    rows = [r for r in model["terms"] if r["term"].startswith("modality[")]
    fig, ax = plt.subplots(figsize=(6, 3.8))
    labels = [r["term"][9:-1] for r in rows]
    eff = np.expm1([r["coef"] for r in rows]) * 100
    lo = np.expm1([r["ci_low"] for r in rows]) * 100
    hi = np.expm1([r["ci_high"] for r in rows]) * 100
    ax.bar(labels, eff, 0.45, color=[S[i] for i in range(len(labels))], yerr=[eff - lo, hi - eff],
           error_kw={"ecolor": TEXT, "elinewidth": 1.2, "capsize": 4})
    ax.axhline(0, color=MUTED, lw=1)
    ax.set_ylabel("% difference vs Hybrid (95% CI)")
    ax.set_title(f"Q3 — adjusted posted-pay gap vs Hybrid (n = {model['n_obs']:,})")
    save(fig, "q3_adjusted_gap")

    pairs = pd.read_csv(F / "q4_top_pairs.csv").head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.barh(pairs.skill_a + " + " + pairs.skill_b, pairs.lift, color=S[0], height=0.6)
    for y, (lift, n) in enumerate(zip(pairs.lift, pairs.pair_count)):
        ax.annotate(f"{lift:.0f} (n={n})", (lift, y), xytext=(4, -3), textcoords="offset points", fontsize=8, color=MUTED)
    ax.set_xlabel("Lift (all years)")
    ax.set_title("Q4 — highest-lift skill pairs")
    ax.grid(axis="y", visible=False)
    save(fig, "q4_top_lift_pairs")


def main() -> None:
    summary: dict = {}
    findings()
    bench1(summary)
    bench2(summary)
    memory(summary)
    atomic_write_json(R / "bench" / "summary.json", summary)
    log.info("benchmark summary: %s", summary)


if __name__ == "__main__":
    main()
