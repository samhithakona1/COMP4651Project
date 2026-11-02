# Origin: original
import pandas as pd
import pytest

from analysis.eval_extraction import evaluate


def test_precision_recall():
    df = pd.DataFrame({
        "predicted_skills": ["Go; Rust", "Python", "Kubernetes", "Go"],
        "true_skills": ["Go", "Python; SQL", "-", ""],   # last row unlabelled -> ignored
    })
    s, per = evaluate(df)
    assert s["n_labelled"] == 3
    assert (s["tp"], s["fp"], s["fn"]) == (2, 2, 1)
    assert s["micro_precision"] == pytest.approx(0.5) and s["micro_recall"] == pytest.approx(2 / 3)
    row = per.set_index("skill").loc["Rust"]
    assert row.fp == 1 and row.precision == 0
