# Origin: original
"""Per-skill precision / recall of the regex extractor against manual labels.

Reads results/labels/label_sample.csv (rows with a non-empty `true_skills`; "-" = no skills).
Writes results/extraction_eval.json and results/extraction_eval_per_skill.csv. With no labelled
rows it records status "awaiting_manual_labels" instead of inventing numbers.
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import pandas as pd

from src.common import config
from src.common.log import get_logger
from src.common.paths import atomic_write_json
from src.common.taxonomy import load_taxonomy

log = get_logger("eval_extraction")
LABELS = config.RESULTS_DIR / "labels" / "label_sample.csv"


def split(cell) -> set[str]:
    if not isinstance(cell, str) or cell.strip() in ("", "-"):
        return set()
    return {s.strip() for s in cell.split(";") if s.strip()}


def evaluate(df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    known = {s.name for s in load_taxonomy()}
    labelled = df[df["true_skills"].fillna("").astype(str).str.strip() != ""]
    tp, fp, fn = Counter(), Counter(), Counter()
    unknown = Counter()
    for _, r in labelled.iterrows():
        pred, true = split(r["predicted_skills"]), split(r["true_skills"])
        unknown.update(true - known)
        true &= known
        for s in pred & true:
            tp[s] += 1
        for s in pred - true:
            fp[s] += 1
        for s in true - pred:
            fn[s] += 1
    rows = []
    for s in sorted(set(tp) | set(fp) | set(fn)):
        p = tp[s] / (tp[s] + fp[s]) if tp[s] + fp[s] else None
        r_ = tp[s] / (tp[s] + fn[s]) if tp[s] + fn[s] else None
        rows.append({"skill": s, "tp": tp[s], "fp": fp[s], "fn": fn[s], "precision": p, "recall": r_,
                     "f1": 2 * p * r_ / (p + r_) if p and r_ else None})
    per = pd.DataFrame(rows, columns=["skill", "tp", "fp", "fn", "precision", "recall", "f1"])
    TP, FP, FN = sum(tp.values()), sum(fp.values()), sum(fn.values())
    summary = {
        "n_labelled": int(len(labelled)), "n_sample": int(len(df)),
        "micro_precision": TP / (TP + FP) if TP + FP else None,
        "micro_recall": TP / (TP + FN) if TP + FN else None,
        "macro_precision": float(per.precision.dropna().mean()) if per.precision.notna().any() else None,
        "macro_recall": float(per.recall.dropna().mean()) if per.recall.notna().any() else None,
        "tp": TP, "fp": FP, "fn": FN,
        "labels_not_in_taxonomy": dict(unknown),
    }
    return summary, per


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default=str(LABELS))
    args = ap.parse_args()
    path = Path(args.labels)
    out = config.RESULTS_DIR / "extraction_eval.json"
    if not path.exists():
        atomic_write_json(out, {"status": "no_label_file", "labels": str(path)})
        log.warning("no label file at %s; run analysis.label_sample", path)
        return
    summary, per = evaluate(pd.read_csv(path, dtype=str, keep_default_na=False))
    summary["status"] = "ok" if summary["n_labelled"] else "awaiting_manual_labels"
    atomic_write_json(out, summary)
    per.to_csv(config.RESULTS_DIR / "extraction_eval_per_skill.csv", index=False)
    log.info("extraction eval: %s", summary)


if __name__ == "__main__":
    main()
