# Origin: original
import json

import pytest

from src.common.paths import raw_partition_file
from src.ingest.quality_gate import QualityGateError, validate_month


def write(raw, year, month, lines, source="hn"):
    path = raw_partition_file(source, year, month, raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(l + "\n" for l in lines))


def good(i):
    return json.dumps({"id": i, "text": f"posting {i}"})


def test_passes_and_writes_report(tmp_path):
    raw, res = tmp_path / "raw", tmp_path / "results"
    write(raw, 2025, 1, [good(i) for i in range(100)])
    rep = validate_month(2025, 1, raw_dir=raw, results_dir=res)
    assert rep["status"] == "passed"
    saved = json.loads((res / "ingest" / "2025-01.json").read_text())
    assert saved["sources"]["hn"]["rows"] == 100 and saved["sources"]["hn"]["malformed"] == 0


def test_missing_file_fails(tmp_path):
    with pytest.raises(QualityGateError, match="missing"):
        validate_month(2025, 2, raw_dir=tmp_path / "raw", results_dir=tmp_path / "r")


def test_empty_file_fails(tmp_path):
    raw = tmp_path / "raw"
    write(raw, 2025, 3, [])
    with pytest.raises(QualityGateError, match="empty"):
        validate_month(2025, 3, raw_dir=raw, results_dir=tmp_path / "r")


def test_malformed_threshold(tmp_path):
    raw = tmp_path / "raw"
    # 5 of 100 malformed (5%) passes; 6 of 100 fails
    write(raw, 2025, 4, [good(i) for i in range(95)] + ["{bad json"] * 2 + ['{"text": "no id"}'] * 2 + ['{"id": 7}'])
    assert validate_month(2025, 4, raw_dir=raw, results_dir=tmp_path / "r")["status"] == "passed"
    write(raw, 2025, 5, [good(i) for i in range(94)] + ["{bad json"] * 6)
    with pytest.raises(QualityGateError, match="malformed"):
        validate_month(2025, 5, raw_dir=raw, results_dir=tmp_path / "r")


def test_delta_warning_not_failure(tmp_path):
    raw = tmp_path / "raw"
    write(raw, 2025, 6, [good(i) for i in range(100)])
    write(raw, 2025, 7, [good(i) for i in range(30)])     # -70% vs previous month
    rep = validate_month(2025, 7, raw_dir=raw, results_dir=tmp_path / "r")
    assert rep["status"] == "passed"
    assert any("differs from previous month" in w for w in rep["sources"]["hn"]["warnings"])


def test_optional_source_only_warns(tmp_path):
    raw = tmp_path / "raw"
    write(raw, 2025, 8, [good(i) for i in range(10)])
    rep = validate_month(2025, 8, {"hn": True, "remotive": False}, raw_dir=raw, results_dir=tmp_path / "r")
    assert rep["status"] == "passed"
    assert rep["sources"]["remotive"]["warnings"]
