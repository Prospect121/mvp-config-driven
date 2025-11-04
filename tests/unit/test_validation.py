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
    unique_rule = result.metrics["by_rule"]["expect_unique:id"]
    assert unique_rule["invalid_rows"] >= 1
    assert unique_rule["valid_rows"] <= result.metrics["input_rows"]
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
    assert "values_in_set:status" in result.metrics["by_rule"]


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
    warn_rule = result.metrics["by_rule"]["expect_not_null:email_warn"]
    assert warn_rule["invalid_rows"] == 1
    assert result.metrics["invalid_rows"] == 1
    # Cuarentena captura filas fallidas
    assert result.quarantine_df.count() == 1
    assert "_reject_reason" in result.quarantine_df.columns


def test_validation_threshold_prevents_failure(spark):
    df = spark.createDataFrame([(1, None), (2, "ok")], ["id", "email"])

    result = apply_validation(
        df,
        {
            "rules": [
                {
                    "check": "expect_not_null",
                    "columns": ["email"],
                    "severity": "error",
                    "threshold": 0.6,
                }
            ]
        },
    )

    metrics = result.metrics["by_rule"]["expect_not_null:email"]
    assert metrics["failed"] is False
    assert result.metrics["invalid_rows"] == 0


def test_validation_email_length_and_foreign_key(spark):
    df = spark.createDataFrame(
        [
            ("ada@example.com", "OK", "A1"),
            ("invalid", "TOO_LONG", "X9"),
        ],
        ["email", "code", "ref"],
    )
    reference = spark.createDataFrame([("A1",)], ["ref"])
    reference.createOrReplaceTempView("dim_refs")

    result = apply_validation(
        df,
        {
            "rules": [
                {"check": "expect_email", "columns": ["email"]},
                {"check": "expect_length", "column": "code", "min": 2, "max": 4},
                {
                    "check": "expect_foreign_key",
                    "col": "ref",
                    "ref_table": "dim_refs",
                    "ref_col": "ref",
                    "on_fail": "quarantine",
                },
            ]
        },
    )

    email_rule = result.metrics["by_rule"]["expect_email:email"]
    assert email_rule["invalid_rows"] == 1
    length_rule = result.metrics["by_rule"]["expect_length:code"]
    assert length_rule["invalid_rows"] == 1
    fk_rule = result.metrics["by_rule"]["expect_foreign_key:ref"]
    assert fk_rule["invalid_rows"] == 1
    assert result.quarantine_df.count() >= 1
