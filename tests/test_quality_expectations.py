import pandas as pd
import json
from pathlib import Path

import pandas as pd
import pytest

from datacore.quality import DQReport, apply_expectations, evaluate_expectations


def test_apply_expectations_pass():
    df = pd.DataFrame(
        {
            "transaction_id": ["a", "b", "c"],
            "amount": [100, 200, 0],
            "currency": ["USD", "EUR", "USD"],
        }
    )

    report = evaluate_expectations(
        df,
        [
            {"type": "not_null", "column": "transaction_id"},
            {"type": "unique", "column": "transaction_id"},
            {"type": "non_negative", "column": "amount"},
            {"type": "valid_values", "column": "currency", "values": ["USD", "EUR"]},
            {"type": "condition", "expr": "amount >= 0"},
            {"type": "range", "column": "amount", "min": 0, "max": 200},
        ],
    )

    assert isinstance(report, DQReport)
    assert report.passed
    assert not report.error_results
    assert json.loads(report.to_json())["passed"] is True


def test_apply_expectations_failure():
    df = pd.DataFrame({"transaction_id": ["a", "a"]})

    with pytest.raises(ValueError):
        apply_expectations(df, [{"type": "unique", "column": "transaction_id"}])


def test_evaluate_expectations_warn(tmp_path: Path):
    df = pd.DataFrame({"amount": [-1, 2]})

    report = evaluate_expectations(
        df,
        [{"type": "non_negative", "column": "amount", "severity": "warn"}],
    )

    assert report.passed
    assert report.warning_results

    output = tmp_path / "dq/report"
    report.write(output)

    json_report = json.loads((output.with_suffix(".json")).read_text(encoding="utf-8"))
    assert json_report["expectations"][0]["severity"] == "warn"
    assert (output.with_suffix(".md")).exists()
