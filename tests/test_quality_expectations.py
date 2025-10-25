import pandas as pd
import pytest

from datacore.quality import apply_expectations


def test_apply_expectations_pass():
    df = pd.DataFrame(
        {
            "transaction_id": ["a", "b", "c"],
            "amount": [100, 200, 0],
            "currency": ["USD", "EUR", "USD"],
        }
    )

    results = apply_expectations(
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

    assert all(result.passed for result in results)


def test_apply_expectations_failure():
    df = pd.DataFrame({"transaction_id": ["a", "a"]})

    with pytest.raises(ValueError):
        apply_expectations(df, [{"type": "unique", "column": "transaction_id"}])
