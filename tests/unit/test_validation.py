from datacore.core.validation import ValidationResult, apply_validation


def test_validation_rules_with_metrics(spark):
    df = spark.createDataFrame(
        [
            (1, "ada@example.com", "100", "2024-01-01"),
            (1, "invalid", "1000", "2024-01-02"),
            (2, None, "200", "2024-01-03"),
        ],
        ["id", "email", "amount", "date"],
    )

    result = apply_validation(
        df,
        {
            "expect_not_null": ["email"],
            "expect_unique": ["id"],
            "expect_regex": [{"col": "email", "pattern": r".+@.+"}],
            "expect_between": [{"col": "amount", "min": 0, "max": 500}],
        },
    )

    assert isinstance(result, ValidationResult)
    assert result.metrics["input_rows"] == 3
    assert result.metrics["invalid_rows"] >= 1
    unique_rule = result.metrics["rules"]["expect_unique:id"]
    assert unique_rule["invalid_rows"] >= 1
    reasons = [row._reject_reason for row in result.invalid_df.collect() if row._reject_reason]
    assert any("expect_unique" in reason for reason in reasons)


def test_validation_alias_and_set_rule(spark):
    df = spark.createDataFrame([(1, "A"), (2, "C")], ["id", "status"])
    result = apply_validation(
        df,
        {
            "expect_column_values_to_be_unique": ["id"],
            "expect_set": [{"col": "status", "allowed": ["A", "B"]}],
        },
    )

    assert result.metrics["invalid_rows"] == 1
    assert "values_in_set:status" in result.metrics["rules"]


def test_validation_severity_and_quarantine(spark):
    df = spark.createDataFrame([(1, None, None), (2, "ok", "ok")], ["id", "email_warn", "email_strict"])

    result = apply_validation(
        df,
        {
            "rules": [
                {
                    "check": "expect_not_null",
                    "columns": ["email_warn"],
                    "severity": "warn",
                },
                {
                    "check": "expect_not_null",
                    "columns": ["email_strict"],
                    "severity": "error",
                    "on_fail": "quarantine",
                },
            ],
            "quarantine_sink": {"type": "storage", "uri": "memory"},
        },
    )

    # La severidad warn no genera filas inválidas pero registra métricas
    warn_rule = result.metrics["rules"]["expect_not_null:email_warn"]
    assert warn_rule["invalid_rows"] == 1
    assert result.metrics["invalid_rows"] == 1
    # Cuarentena captura filas fallidas
    assert result.quarantine_df.count() == 1
