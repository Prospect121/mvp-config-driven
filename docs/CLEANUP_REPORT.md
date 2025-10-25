# Cleanup Audit Report

_Generated on 2025-10-25T02:36:42.052453Z_

## Resumen ejecutivo

| Clasificación | Conteo |
| --- | ---: |
| REMOVE | 0 |
| QUARANTINE | 4 |
| KEEP | 131 |

**Ahorro estimado**: 0.02 MB si se aplica REMOVE + QUARANTINE.

## Tabla maestra

| Path | Tipo | Tamaño (KB) | Último commit | # refs | Clasificación | Motivo | Riesgo | Acción |
| --- | --- | ---: | --- | ---: | --- | --- | --- | --- |
| .env | other | 0.48 | 2025-09-28 | 20 | KEEP | Referenced or recently modified. | medium | keep |
| .env.example | other | 0.55 | 2025-10-06 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .gitignore | other | 6.04 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/config.json | config | 0.69 | 2025-09-29 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/share/downloads.json | config | 0.03 | 2025-09-29 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/share/uploads.json | config | 0.03 | 2025-09-29 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .pre-commit-config.yaml | config | 0.34 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| README.md | doc | 2.81 | 2025-10-24 | 12 | KEEP | Governance or generated artifact | low | keep |
| cfg/bronze/template.yml | config | 0.52 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/gold/template.yml | config | 0.51 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/raw/template.yml | config | 0.51 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/silver/template.yml | config | 0.52 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| ci/build-test.yml | config | 0.97 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| ci/check_config.sh | ci | 0.25 | 2025-09-28 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| ci/docker-compose.override.yml | config | 0.10 | 2025-10-08 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| ci/lint.yml | config | 1.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| ci/test_dataset.yml | config | 0.34 | 2025-09-28 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| config/database.yml | config | 1.93 | 2025-10-09 | 22 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/events_multiformat.yml | config | 3.88 | 2025-10-24 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/events_multiformat_expectations.yml | config | 7.29 | 2025-10-16 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/events_multiformat_schema.json | config | 2.14 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/events_multiformat_transforms.yml | config | 0.85 | 2025-10-16 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/payments_high_volume.yml | config | 2.96 | 2025-10-24 | 12 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/payments_high_volume_expectations.yml | config | 5.62 | 2025-10-09 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/casos_uso/payments_high_volume_schema.json | config | 2.42 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_db_only/dataset.yml | config | 2.28 | 2025-10-24 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_db_only/expectations.yml | config | 0.46 | 2025-10-20 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_db_only/schema.yml | config | 0.69 | 2025-10-20 | 12 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_db_only/transforms.yml | config | 1.16 | 2025-10-20 | 17 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_multi/dataset.yml | config | 1.65 | 2025-10-20 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v1/dataset.yml | config | 2.15 | 2025-10-24 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v1/expectations.yml | config | 0.79 | 2025-10-08 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v1/schema.json | config | 1.79 | 2025-10-15 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v1/transforms.yml | config | 0.40 | 2025-10-13 | 17 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v2/dataset.yml | config | 1.69 | 2025-10-24 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v2/expectations.yml | config | 0.79 | 2025-10-06 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v2/schema.json | config | 0.47 | 2025-10-08 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v3/dataset.yml | config | 2.82 | 2025-10-24 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v3/dataset_api.yml | config | 4.19 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v3/expectations.yml | config | 0.46 | 2025-10-16 | 19 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v3/schema.yml | config | 0.70 | 2025-10-16 | 12 | KEEP | Referenced or recently modified. | medium | keep |
| config/datasets/finanzas/payments_v3/transforms.yml | config | 1.36 | 2025-10-20 | 17 | KEEP | Referenced or recently modified. | medium | keep |
| config/env.yml | config | 0.65 | 2025-10-24 | 21 | KEEP | Referenced or recently modified. | medium | keep |
| config/schema_mapping.yml | config | 1.89 | 2025-10-08 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| data/casos-uso/multi-format/events_batch_001.json | other | 5709.05 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| data/output/.gitkeep | other | 0.14 | 2025-10-08 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| data/processed/.gitkeep | other | 0.16 | 2025-10-08 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| data/raw/.gitkeep | other | 0.15 | 2025-10-08 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| data/raw/payments/sample.csv | other | 0.64 | 2025-10-15 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| data/s3a-staging/finanzas/payments/raw/sample.jsonl | other | 0.31 | 2025-10-20 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_001.csv | other | 588.43 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_002.csv | other | 588.26 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| data/s3a-staging/raw/payments_v3/sample.csv | other | 0.34 | 2025-10-24 | 13 | KEEP | Referenced or recently modified. | medium | keep |
| docker/spark-pandas-udf/Dockerfile | docker | 0.34 | 2025-10-13 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| docker-compose.yml | config | 3.98 | 2025-10-13 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/CLEANUP_REPORT.md | doc | 69.73 | 2025-10-24 | 10 | KEEP | Governance or generated artifact | low | keep |
| docs/PROJECT_DOCUMENTATION.md | doc | 6.12 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/cleanup.json | doc | 109.96 | 2025-10-24 | 12 | KEEP | Governance or generated artifact | low | keep |
| docs/diagrams/dependencies.md | doc | 1.24 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| docs/diagrams/deps_cleanup.md | doc | 1.36 | 2025-10-24 | 10 | KEEP | Governance or generated artifact | low | keep |
| docs/diagrams/pipeline_flow.md | doc | 1.17 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/policies/DEP-001-legacy-removal.md | doc | 2.08 | 2025-10-24 | 6 | KEEP | Governance or generated artifact | low | keep |
| docs/run/aws.md | doc | 3.11 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/azure.md | doc | 3.28 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/configs.md | doc | 2.69 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/databricks.md | doc | 3.15 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/gcp.md | doc | 3.61 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/jobs/aws_stepfunctions.json | doc | 1.28 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/jobs/databricks_job.json | doc | 2.03 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| docs/tools/list_io.py | code | 2.94 | 2025-10-24 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| legacy/docs/2025-10-25-reports/README.md | doc | 0.56 | - | 12 | KEEP | Punto de entrada documental | low | keep |
| legacy/docs/2025-10-25-reports/REPORT.md | doc | 12.38 | - | 14 | QUARANTINE | Reporte legacy reubicado en cuarentena (2025-10-25). | medium | hold_in_legacy |
| legacy/docs/2025-10-25-reports/report.json | config | 5.92 | - | 12 | QUARANTINE | Export JSON legacy en cuarentena (2025-10-25). | medium | hold_in_legacy |
| legacy/infra/2025-10-25-gcp/README.md | doc | 0.50 | - | 12 | KEEP | Punto de entrada documental | low | keep |
| legacy/infra/2025-10-25-gcp/dataproc_workflow.yaml | config | 1.68 | - | 11 | QUARANTINE | Workflow Dataproc legacy en cuarentena (2025-10-25). | medium | hold_in_legacy |
| legacy/scripts/2025-10-25-generation/README.md | doc | 0.48 | - | 12 | KEEP | Punto de entrada documental | low | keep |
| legacy/scripts/2025-10-25-generation/generate_big_payments.py | code | 1.27 | - | 12 | QUARANTINE | Script de pagos legacy retenido en cuarentena (2025-10-25). | medium | hold_in_legacy |
| pipelines/__init__.py | code | 0.16 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/common.py | code | 4.32 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/config/__init__.py | code | 0.05 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/config/loader.py | code | 0.75 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/__init__.py | code | 0.06 | 2025-10-08 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/db_manager.py | code | 29.79 | 2025-10-15 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/schema_mapper.py | code | 18.59 | 2025-10-09 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/__init__.py | code | 0.06 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/reader.py | code | 1.48 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/s3a.py | code | 0.48 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/writer.py | code | 1.42 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/sources.py | code | 13.96 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/spark_job.py | code | 11.51 | 2025-10-24 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/spark_job_with_db.py | code | 2.11 | 2025-10-24 | 16 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/__init__.py | code | 0.05 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/apply.py | code | 4.82 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/tests/__init__.py | code | 0.03 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/tests/test_apply.py | code | 1.23 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/udf_catalog.py | code | 4.79 | 2025-10-13 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/__init__.py | code | 0.06 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/logger.py | code | 1.16 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/parallel.py | code | 0.95 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/validation/__init__.py | code | 0.08 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/validation/quality.py | code | 4.31 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| pyproject.toml | config | 0.81 | 2025-10-24 | 6 | KEEP | Governance or generated artifact | low | keep |
| requirements.txt | other | 0.68 | 2025-10-24 | 12 | KEEP | Governance or generated artifact | low | keep |
| scripts/db/init.sql | other | 1.67 | 2025-10-08 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/generate_synthetic_data.py | code | 34.54 | 2025-10-24 | 16 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/run_high_volume_case.py | code | 8.63 | 2025-10-24 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/run_multiformat_case.py | code | 15.11 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/runner.sh | other | 4.07 | 2025-10-24 | 12 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/runner_with_db.sh | other | 3.03 | 2025-10-24 | 11 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/__init__.py | code | 0.32 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/cli.py | code | 4.91 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/config/schema.py | code | 8.37 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/context.py | code | 6.29 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/__init__.py | code | 0.31 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/adapters.py | code | 3.37 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/fs.py | code | 19.87 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/bronze/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/bronze/main.py | code | 6.72 | 2025-10-24 | 11 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/gold/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/gold/main.py | code | 6.58 | 2025-10-24 | 11 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/raw/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/raw/main.py | code | 0.62 | 2025-10-24 | 11 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/silver/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/silver/main.py | code | 6.17 | 2025-10-24 | 11 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/pipeline/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/pipeline/utils.py | code | 24.75 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_cli_smoke.py | code | 2.76 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_config_schema.py | code | 2.22 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_io_adapters.py | code | 1.88 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_io_fs.py | code | 7.97 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_layers_config.py | code | 2.24 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_security.py | code | 0.95 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| tools/audit_cleanup.py | code | 24.75 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| tools/list_io.py | code | 3.18 | 2025-10-24 | 9 | KEEP | Referenced or recently modified. | medium | keep |

## Evidencia por elemento

### .env

Referencias detectadas:

- **code**: scripts/run_multiformat_case.py:38, scripts/run_multiformat_case.py:39, scripts/run_multiformat_case.py:40, scripts/run_multiformat_case.py:44, scripts/run_multiformat_case.py:48, scripts/run_multiformat_case.py:55, scripts/run_multiformat_case.py:57, scripts/run_multiformat_case.py:63
- **configs**: legacy/docs/2025-10-25-reports/report.json:174, legacy/docs/2025-10-25-reports/report.json:175, legacy/docs/2025-10-25-reports/report.json:176, legacy/docs/2025-10-25-reports/report.json:177
- **docs**: docs/cleanup.json:16, docs/cleanup.json:57, docs/run/jobs/aws_stepfunctions.json:12, docs/run/jobs/aws_stepfunctions.json:24, docs/run/jobs/aws_stepfunctions.json:36, docs/run/jobs/aws_stepfunctions.json:48, docs/CLEANUP_REPORT.md:19, docs/CLEANUP_REPORT.md:20
- Clasificación: KEEP

### .env.example

Referencias detectadas:

- **docs**: docs/cleanup.json:57, docs/CLEANUP_REPORT.md:20, docs/CLEANUP_REPORT.md:166
- Clasificación: KEEP

### .gitignore

Referencias detectadas:

- **code**: data/processed/.gitkeep:2, data/raw/.gitkeep:2, data/output/.gitkeep:2
- **docs**: docs/cleanup.json:79, docs/CLEANUP_REPORT.md:21, docs/CLEANUP_REPORT.md:173
- Clasificación: KEEP

### .mc/config.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:27, tools/audit_cleanup.py:29
- **docs**: docs/diagrams/deps_cleanup.md:9, docs/diagrams/deps_cleanup.md:20, docs/cleanup.json:105, docs/CLEANUP_REPORT.md:22, docs/CLEANUP_REPORT.md:181
- Clasificación: KEEP

### .mc/share/downloads.json

Referencias detectadas:

- **docs**: docs/cleanup.json:132, docs/CLEANUP_REPORT.md:23, docs/CLEANUP_REPORT.md:189
- Clasificación: KEEP

### .mc/share/uploads.json

Referencias detectadas:

- **docs**: docs/cleanup.json:154, docs/CLEANUP_REPORT.md:24, docs/CLEANUP_REPORT.md:196
- Clasificación: KEEP

### .pre-commit-config.yaml

Referencias detectadas:

- **docs**: docs/cleanup.json:176, docs/PROJECT_DOCUMENTATION.md:91, docs/CLEANUP_REPORT.md:25, docs/CLEANUP_REPORT.md:203
- Clasificación: KEEP

### README.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:105, tools/audit_cleanup.py:106, tools/audit_cleanup.py:107, tools/audit_cleanup.py:258
- **docs**: docs/cleanup.json:199, docs/cleanup.json:1764, docs/cleanup.json:2080, docs/cleanup.json:2091, docs/cleanup.json:2139, docs/cleanup.json:2171, docs/cleanup.json:2189, docs/cleanup.json:2235
- Clasificación: KEEP

### cfg/bronze/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:105, legacy/docs/2025-10-25-reports/REPORT.md:166, docs/cleanup.json:231, docs/cleanup.json:261, docs/cleanup.json:291, docs/cleanup.json:321, docs/cleanup.json:1349, docs/cleanup.json:1350
- Clasificación: KEEP

### cfg/gold/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:105, legacy/docs/2025-10-25-reports/REPORT.md:166, docs/cleanup.json:231, docs/cleanup.json:261, docs/cleanup.json:291, docs/cleanup.json:321, docs/cleanup.json:1349, docs/cleanup.json:1350
- Clasificación: KEEP

### cfg/raw/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:105, legacy/docs/2025-10-25-reports/REPORT.md:166, docs/cleanup.json:231, docs/cleanup.json:261, docs/cleanup.json:291, docs/cleanup.json:321, docs/cleanup.json:1349, docs/cleanup.json:1350
- Clasificación: KEEP

### cfg/silver/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:105, legacy/docs/2025-10-25-reports/REPORT.md:166, docs/cleanup.json:231, docs/cleanup.json:261, docs/cleanup.json:291, docs/cleanup.json:321, docs/cleanup.json:1349, docs/cleanup.json:1350
- Clasificación: KEEP

### ci/build-test.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:351, docs/cleanup.json:2985, docs/CLEANUP_REPORT.md:31, docs/CLEANUP_REPORT.md:250, docs/CLEANUP_REPORT.md:966
- Clasificación: KEEP

### ci/check_config.sh

Referencias detectadas:

- **configs**: ci/lint.yml:18
- **docs**: docs/cleanup.json:375, docs/cleanup.json:984, docs/cleanup.json:1137, docs/CLEANUP_REPORT.md:32, docs/CLEANUP_REPORT.md:257, docs/CLEANUP_REPORT.md:423, docs/CLEANUP_REPORT.md:460, legacy/docs/2025-10-25-reports/REPORT.md:114
- Clasificación: KEEP

### ci/docker-compose.override.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:404, docs/CLEANUP_REPORT.md:33, docs/CLEANUP_REPORT.md:265
- Clasificación: KEEP

### ci/lint.yml

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:114, legacy/docs/2025-10-25-reports/REPORT.md:128, docs/cleanup.json:382, docs/cleanup.json:426, docs/cleanup.json:1759, docs/cleanup.json:1760, docs/cleanup.json:2069, docs/cleanup.json:2986
- Clasificación: KEEP

### ci/test_dataset.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:453, docs/cleanup.json:981, docs/cleanup.json:1134, docs/cleanup.json:1517, docs/cleanup.json:1617, docs/CLEANUP_REPORT.md:35, docs/CLEANUP_REPORT.md:279, docs/CLEANUP_REPORT.md:422
- Clasificación: KEEP

### config/database.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:580, scripts/generate_synthetic_data.py:639, scripts/run_multiformat_case.py:104, scripts/runner.sh:7, scripts/runner_with_db.sh:7, scripts/run_high_volume_case.py:79
- **configs**: config/datasets/finanzas/payments_v1/dataset.yml:49, config/datasets/finanzas/payments_v2/dataset.yml:44, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:78, config/datasets/finanzas/payments_v3/dataset_api.yml:5, config/datasets/finanzas/payments_v3/dataset_api.yml:83, config/datasets/casos_uso/payments_high_volume.yml:69, config/datasets/casos_uso/events_multiformat.yml:71
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:102, legacy/docs/2025-10-25-reports/REPORT.md:146, docs/cleanup.json:480, docs/run/configs.md:22, docs/run/configs.md:57, docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:31, docs/PROJECT_DOCUMENTATION.md:40
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat.yml

Referencias detectadas:

- **code**: scripts/run_multiformat_case.py:102
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:171, docs/cleanup.json:501, docs/cleanup.json:523, docs/cleanup.json:561, docs/cleanup.json:587, docs/cleanup.json:611, docs/cleanup.json:834, docs/cleanup.json:980
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/casos_uso/events_multiformat.yml:42
- **docs**: docs/cleanup.json:552, docs/CLEANUP_REPORT.md:38, docs/CLEANUP_REPORT.md:303
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:621
- **configs**: config/datasets/casos_uso/events_multiformat.yml:46
- **docs**: docs/cleanup.json:578, docs/CLEANUP_REPORT.md:39, docs/CLEANUP_REPORT.md:312
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_transforms.yml

Referencias detectadas:

- **configs**: config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/cleanup.json:604, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:321
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:599, scripts/generate_synthetic_data.py:603, scripts/generate_synthetic_data.py:782, scripts/run_high_volume_case.py:77
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:172, docs/cleanup.json:500, docs/cleanup.json:628, docs/cleanup.json:669, docs/cleanup.json:695, docs/cleanup.json:764, docs/cleanup.json:942, docs/cleanup.json:979
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume_expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562
- **configs**: config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:660, docs/CLEANUP_REPORT.md:42, docs/CLEANUP_REPORT.md:337
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume_schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566
- **configs**: config/datasets/casos_uso/payments_high_volume.yml:49
- **docs**: docs/cleanup.json:686, docs/CLEANUP_REPORT.md:43, docs/CLEANUP_REPORT.md:346
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:127, legacy/docs/2025-10-25-reports/REPORT.md:168, legacy/docs/2025-10-25-reports/REPORT.md:169, legacy/docs/2025-10-25-reports/REPORT.md:170, docs/cleanup.json:453, docs/cleanup.json:494, docs/cleanup.json:495, docs/cleanup.json:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/expectations.yml

Referencias detectadas:

- **code**: tests/test_config_schema.py:30, scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:552, docs/cleanup.json:660, docs/cleanup.json:746, docs/cleanup.json:924, docs/cleanup.json:1077, docs/cleanup.json:1222, docs/run/configs.md:52, docs/CLEANUP_REPORT.md:38
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/schema.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:47, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:56, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:63, config/datasets/finanzas/payments_multi/dataset.yml:34
- **docs**: docs/cleanup.json:786, docs/cleanup.json:1262, docs/CLEANUP_REPORT.md:46, docs/CLEANUP_REPORT.md:59, docs/CLEANUP_REPORT.md:373, docs/CLEANUP_REPORT.md:490
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/cleanup.json:604, docs/cleanup.json:818, docs/cleanup.json:1005, docs/cleanup.json:1294, docs/PROJECT_DOCUMENTATION.md:28, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:47, docs/CLEANUP_REPORT.md:52
- Clasificación: KEEP

### config/datasets/finanzas/payments_multi/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:127, legacy/docs/2025-10-25-reports/REPORT.md:168, legacy/docs/2025-10-25-reports/REPORT.md:169, legacy/docs/2025-10-25-reports/REPORT.md:170, docs/cleanup.json:453, docs/cleanup.json:494, docs/cleanup.json:495, docs/cleanup.json:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:127, legacy/docs/2025-10-25-reports/REPORT.md:168, legacy/docs/2025-10-25-reports/REPORT.md:169, legacy/docs/2025-10-25-reports/REPORT.md:170, docs/cleanup.json:453, docs/cleanup.json:494, docs/cleanup.json:495, docs/cleanup.json:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/expectations.yml

Referencias detectadas:

- **code**: tests/test_config_schema.py:30, scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:552, docs/cleanup.json:660, docs/cleanup.json:746, docs/cleanup.json:924, docs/cleanup.json:1077, docs/cleanup.json:1222, docs/run/configs.md:52, docs/CLEANUP_REPORT.md:38
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566, scripts/generate_synthetic_data.py:621, pipelines/database/schema_mapper.py:428, pipelines/database/db_manager.py:706, pipelines/database/__init__.py:1
- **configs**: config/datasets/finanzas/payments_v1/dataset.yml:34, config/datasets/finanzas/payments_v2/dataset.yml:25, config/datasets/casos_uso/payments_high_volume.yml:49, config/datasets/casos_uso/events_multiformat.yml:46, ci/test_dataset.yml:9
- **ci**: ci/check_config.sh:6
- **docs**: docs/cleanup.json:578, docs/cleanup.json:686, docs/cleanup.json:964, docs/cleanup.json:1117, docs/CLEANUP_REPORT.md:39, docs/CLEANUP_REPORT.md:43, docs/CLEANUP_REPORT.md:51, docs/CLEANUP_REPORT.md:55
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/cleanup.json:604, docs/cleanup.json:818, docs/cleanup.json:1005, docs/cleanup.json:1294, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:47, docs/CLEANUP_REPORT.md:52, docs/CLEANUP_REPORT.md:60
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:127, legacy/docs/2025-10-25-reports/REPORT.md:168, legacy/docs/2025-10-25-reports/REPORT.md:169, legacy/docs/2025-10-25-reports/REPORT.md:170, docs/cleanup.json:453, docs/cleanup.json:494, docs/cleanup.json:495, docs/cleanup.json:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/expectations.yml

Referencias detectadas:

- **code**: tests/test_config_schema.py:30, scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:552, docs/cleanup.json:660, docs/cleanup.json:746, docs/cleanup.json:924, docs/cleanup.json:1077, docs/cleanup.json:1222, docs/run/configs.md:52, docs/CLEANUP_REPORT.md:38
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566, scripts/generate_synthetic_data.py:621, pipelines/database/schema_mapper.py:428, pipelines/database/db_manager.py:706, pipelines/database/__init__.py:1
- **configs**: config/datasets/finanzas/payments_v1/dataset.yml:34, config/datasets/finanzas/payments_v2/dataset.yml:25, config/datasets/casos_uso/payments_high_volume.yml:49, config/datasets/casos_uso/events_multiformat.yml:46, ci/test_dataset.yml:9
- **ci**: ci/check_config.sh:6
- **docs**: docs/cleanup.json:578, docs/cleanup.json:686, docs/cleanup.json:964, docs/cleanup.json:1117, docs/CLEANUP_REPORT.md:39, docs/CLEANUP_REPORT.md:43, docs/CLEANUP_REPORT.md:51, docs/CLEANUP_REPORT.md:55
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:127, legacy/docs/2025-10-25-reports/REPORT.md:168, legacy/docs/2025-10-25-reports/REPORT.md:169, legacy/docs/2025-10-25-reports/REPORT.md:170, docs/cleanup.json:453, docs/cleanup.json:494, docs/cleanup.json:495, docs/cleanup.json:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/dataset_api.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_v3/dataset_api.yml:4, config/datasets/finanzas/payments_v3/dataset_api.yml:5
- **docs**: docs/cleanup.json:498, docs/cleanup.json:499, docs/cleanup.json:761, docs/cleanup.json:762, docs/cleanup.json:796, docs/cleanup.json:797, docs/cleanup.json:831, docs/cleanup.json:832
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/expectations.yml

Referencias detectadas:

- **code**: tests/test_config_schema.py:30, scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:552, docs/cleanup.json:660, docs/cleanup.json:746, docs/cleanup.json:924, docs/cleanup.json:1077, docs/cleanup.json:1222, docs/run/configs.md:52, docs/CLEANUP_REPORT.md:38
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/schema.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:47, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:56, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:63, config/datasets/finanzas/payments_multi/dataset.yml:34
- **docs**: docs/cleanup.json:786, docs/cleanup.json:1262, docs/CLEANUP_REPORT.md:46, docs/CLEANUP_REPORT.md:59, docs/CLEANUP_REPORT.md:373, docs/CLEANUP_REPORT.md:490
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/cleanup.json:604, docs/cleanup.json:818, docs/cleanup.json:1005, docs/cleanup.json:1294, docs/PROJECT_DOCUMENTATION.md:28, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:47, docs/CLEANUP_REPORT.md:52
- Clasificación: KEEP

### config/env.yml

Referencias detectadas:

- **code**: scripts/runner.sh:6, scripts/runner_with_db.sh:6, src/datacore/io/adapters.py:63, pipelines/sources.py:113, pipelines/sources.py:117
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset_api.yml:5, config/datasets/finanzas/payments_v3/dataset_api.yml:15, config/datasets/finanzas/payments_v3/dataset_api.yml:103, cfg/raw/template.yml:7, cfg/gold/template.yml:7, cfg/silver/template.yml:7, cfg/bronze/template.yml:7
- **docs**: docs/cleanup.json:1332, docs/run/configs.md:20, docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:22, docs/PROJECT_DOCUMENTATION.md:40, docs/PROJECT_DOCUMENTATION.md:42, docs/CLEANUP_REPORT.md:61, docs/CLEANUP_REPORT.md:507
- Clasificación: KEEP

### config/schema_mapping.yml

Referencias detectadas:

- **code**: pipelines/database/schema_mapper.py:48
- **docs**: docs/cleanup.json:1374, docs/cleanup.json:2480, docs/CLEANUP_REPORT.md:62, docs/CLEANUP_REPORT.md:516, docs/CLEANUP_REPORT.md:825
- Clasificación: KEEP

### data/casos-uso/multi-format/events_batch_001.json

Referencias detectadas:

- **docs**: docs/cleanup.json:1400, docs/CLEANUP_REPORT.md:63, docs/CLEANUP_REPORT.md:524
- Clasificación: KEEP

### data/output/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:85, docs/cleanup.json:86, docs/cleanup.json:87, docs/cleanup.json:1422, docs/cleanup.json:1449, docs/cleanup.json:1476, docs/CLEANUP_REPORT.md:64, docs/CLEANUP_REPORT.md:65
- Clasificación: KEEP

### data/processed/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:85, docs/cleanup.json:86, docs/cleanup.json:87, docs/cleanup.json:1422, docs/cleanup.json:1449, docs/cleanup.json:1476, docs/CLEANUP_REPORT.md:64, docs/CLEANUP_REPORT.md:65
- Clasificación: KEEP

### data/raw/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:85, docs/cleanup.json:86, docs/cleanup.json:87, docs/cleanup.json:1422, docs/cleanup.json:1449, docs/cleanup.json:1476, docs/CLEANUP_REPORT.md:64, docs/CLEANUP_REPORT.md:65
- Clasificación: KEEP

### data/raw/payments/sample.csv

Referencias detectadas:

- **code**: tests/test_io_fs.py:185, tests/test_io_fs.py:186, tests/test_io_fs.py:187, tests/test_io_fs.py:211, legacy/scripts/2025-10-25-generation/generate_big_payments.py:6
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:5, ci/test_dataset.yml:5
- **docs**: docs/cleanup.json:1503, docs/cleanup.json:1603, docs/CLEANUP_REPORT.md:67, docs/CLEANUP_REPORT.md:71, docs/CLEANUP_REPORT.md:552, docs/CLEANUP_REPORT.md:582
- Clasificación: KEEP

### data/s3a-staging/finanzas/payments/raw/sample.jsonl

Referencias detectadas:

- **docs**: docs/cleanup.json:1537, docs/CLEANUP_REPORT.md:68, docs/CLEANUP_REPORT.md:561
- Clasificación: KEEP

### data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_001.csv

Referencias detectadas:

- **docs**: docs/cleanup.json:1559, docs/CLEANUP_REPORT.md:69, docs/CLEANUP_REPORT.md:568
- Clasificación: KEEP

### data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_002.csv

Referencias detectadas:

- **docs**: docs/cleanup.json:1581, docs/CLEANUP_REPORT.md:70, docs/CLEANUP_REPORT.md:575
- Clasificación: KEEP

### data/s3a-staging/raw/payments_v3/sample.csv

Referencias detectadas:

- **code**: tests/test_io_fs.py:185, tests/test_io_fs.py:186, tests/test_io_fs.py:187, tests/test_io_fs.py:211, legacy/scripts/2025-10-25-generation/generate_big_payments.py:6
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:5, ci/test_dataset.yml:5
- **docs**: docs/cleanup.json:1503, docs/cleanup.json:1603, docs/CLEANUP_REPORT.md:67, docs/CLEANUP_REPORT.md:71, docs/CLEANUP_REPORT.md:552, docs/CLEANUP_REPORT.md:582
- Clasificación: KEEP

### docker/spark-pandas-udf/Dockerfile

Referencias detectadas:

- **code**: tools/audit_cleanup.py:179
- **configs**: docker-compose.yml:49, docker-compose.yml:65, docker-compose.yml:81
- **docs**: docs/cleanup.json:1637, docs/CLEANUP_REPORT.md:72, docs/CLEANUP_REPORT.md:591
- Clasificación: KEEP

### docker-compose.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:1646, docs/cleanup.json:1647, docs/cleanup.json:1648, docs/cleanup.json:1665, docs/cleanup.json:3013, docs/cleanup.json:3131, docs/cleanup.json:3132, docs/CLEANUP_REPORT.md:73
- Clasificación: KEEP

### docs/CLEANUP_REPORT.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:259, tools/audit_cleanup.py:618
- **docs**: docs/cleanup.json:45, docs/cleanup.json:46, docs/cleanup.json:67, docs/cleanup.json:68, docs/cleanup.json:93, docs/cleanup.json:94, docs/cleanup.json:120, docs/cleanup.json:121
- Clasificación: KEEP

### docs/PROJECT_DOCUMENTATION.md

Referencias detectadas:

- **docs**: README.md:33, docs/cleanup.json:186, docs/cleanup.json:510, docs/cleanup.json:511, docs/cleanup.json:512, docs/cleanup.json:842, docs/cleanup.json:1029, docs/cleanup.json:1318
- Clasificación: KEEP

### docs/cleanup.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:260, tools/audit_cleanup.py:617
- **configs**: ci/lint.yml:35, ci/lint.yml:39
- **docs**: docs/policies/DEP-001-legacy-removal.md:17, docs/cleanup.json:39, docs/cleanup.json:40, docs/cleanup.json:66, docs/cleanup.json:92, docs/cleanup.json:119, docs/cleanup.json:141, docs/cleanup.json:165
- Clasificación: KEEP

### docs/diagrams/dependencies.md

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:45, docs/cleanup.json:1782, docs/CLEANUP_REPORT.md:77, docs/CLEANUP_REPORT.md:631, docs/CLEANUP_REPORT.md:1246
- Clasificación: KEEP

### docs/diagrams/deps_cleanup.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:104, tools/audit_cleanup.py:261
- **docs**: docs/cleanup.json:117, docs/cleanup.json:118, docs/cleanup.json:1806, docs/cleanup.json:2140, docs/cleanup.json:2141, docs/cleanup.json:2172, docs/cleanup.json:2173, docs/cleanup.json:2236
- Clasificación: KEEP

### docs/diagrams/pipeline_flow.md

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:46, docs/cleanup.json:1836, docs/cleanup.json:2645, docs/cleanup.json:2681, docs/CLEANUP_REPORT.md:79, docs/CLEANUP_REPORT.md:646, docs/CLEANUP_REPORT.md:870, docs/CLEANUP_REPORT.md:879
- Clasificación: KEEP

### docs/policies/DEP-001-legacy-removal.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:262
- **docs**: docs/cleanup.json:1765, docs/cleanup.json:1863, docs/CLEANUP_REPORT.md:80, docs/CLEANUP_REPORT.md:628, docs/CLEANUP_REPORT.md:653
- Clasificación: KEEP

### docs/run/aws.md

Referencias detectadas:

- **docs**: docs/cleanup.json:1889, docs/PROJECT_DOCUMENTATION.md:49, docs/CLEANUP_REPORT.md:81, docs/CLEANUP_REPORT.md:661
- Clasificación: KEEP

### docs/run/azure.md

Referencias detectadas:

- **docs**: docs/cleanup.json:1912, docs/PROJECT_DOCUMENTATION.md:51, docs/CLEANUP_REPORT.md:82, docs/CLEANUP_REPORT.md:668, docs/CLEANUP_REPORT.md:1244, docs/CLEANUP_REPORT.md:1245
- Clasificación: KEEP

### docs/run/configs.md

Referencias detectadas:

- **docs**: docs/cleanup.json:508, docs/cleanup.json:509, docs/cleanup.json:774, docs/cleanup.json:952, docs/cleanup.json:1105, docs/cleanup.json:1250, docs/cleanup.json:1357, docs/cleanup.json:1937
- Clasificación: KEEP

### docs/run/databricks.md

Referencias detectadas:

- **docs**: docs/cleanup.json:1964, docs/PROJECT_DOCUMENTATION.md:48, docs/CLEANUP_REPORT.md:84, docs/CLEANUP_REPORT.md:682, docs/CLEANUP_REPORT.md:1241, docs/CLEANUP_REPORT.md:1242, docs/CLEANUP_REPORT.md:1243
- Clasificación: KEEP

### docs/run/gcp.md

Referencias detectadas:

- **docs**: docs/cleanup.json:1990, docs/PROJECT_DOCUMENTATION.md:50, docs/CLEANUP_REPORT.md:85, docs/CLEANUP_REPORT.md:689
- Clasificación: KEEP

### docs/run/jobs/aws_stepfunctions.json

Referencias detectadas:

- **docs**: docs/cleanup.json:41, docs/cleanup.json:42, docs/cleanup.json:43, docs/cleanup.json:44, docs/cleanup.json:2013, docs/CLEANUP_REPORT.md:86, docs/CLEANUP_REPORT.md:163, docs/CLEANUP_REPORT.md:696
- Clasificación: KEEP

### docs/run/jobs/databricks_job.json

Referencias detectadas:

- **docs**: docs/cleanup.json:2040, docs/CLEANUP_REPORT.md:87, docs/CLEANUP_REPORT.md:703
- Clasificación: KEEP

### docs/tools/list_io.py

Referencias detectadas:

- **configs**: ci/lint.yml:34
- **docs**: docs/cleanup.json:2062, docs/cleanup.json:3860, docs/PROJECT_DOCUMENTATION.md:115, docs/CLEANUP_REPORT.md:88, docs/CLEANUP_REPORT.md:153, docs/CLEANUP_REPORT.md:710, docs/CLEANUP_REPORT.md:1199, README.md:40
- Clasificación: KEEP

### legacy/docs/2025-10-25-reports/README.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:105, tools/audit_cleanup.py:106, tools/audit_cleanup.py:107, tools/audit_cleanup.py:258
- **docs**: docs/cleanup.json:199, docs/cleanup.json:1764, docs/cleanup.json:2080, docs/cleanup.json:2091, docs/cleanup.json:2139, docs/cleanup.json:2171, docs/cleanup.json:2189, docs/cleanup.json:2235
- Clasificación: KEEP

### legacy/docs/2025-10-25-reports/REPORT.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:34, tools/audit_cleanup.py:40, tools/audit_cleanup.py:66, tools/audit_cleanup.py:97, tools/audit_cleanup.py:259, tools/audit_cleanup.py:618
- **docs**: legacy/docs/2025-10-25-reports/README.md:6, docs/diagrams/deps_cleanup.md:5, docs/diagrams/deps_cleanup.md:16, docs/cleanup.json:45, docs/cleanup.json:46, docs/cleanup.json:67, docs/cleanup.json:68, docs/cleanup.json:93
- Notas: Restaurar solo si gobernanza lo aprueba.
- Clasificación: QUARANTINE

### legacy/docs/2025-10-25-reports/report.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:42, tools/audit_cleanup.py:48, tools/audit_cleanup.py:73, tools/audit_cleanup.py:98
- **docs**: legacy/docs/2025-10-25-reports/README.md:6, docs/diagrams/deps_cleanup.md:6, docs/diagrams/deps_cleanup.md:17, docs/cleanup.json:32, docs/cleanup.json:33, docs/cleanup.json:34, docs/cleanup.json:35, docs/cleanup.json:2157
- Notas: Mantener disponible durante la ventana de 30 días.
- Clasificación: QUARANTINE

### legacy/infra/2025-10-25-gcp/README.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:105, tools/audit_cleanup.py:106, tools/audit_cleanup.py:107, tools/audit_cleanup.py:258
- **docs**: docs/cleanup.json:199, docs/cleanup.json:1764, docs/cleanup.json:2080, docs/cleanup.json:2091, docs/cleanup.json:2139, docs/cleanup.json:2171, docs/cleanup.json:2189, docs/cleanup.json:2235
- Clasificación: KEEP

### legacy/infra/2025-10-25-gcp/dataproc_workflow.yaml

Referencias detectadas:

- **code**: tools/audit_cleanup.py:58, tools/audit_cleanup.py:64, tools/audit_cleanup.py:80, tools/audit_cleanup.py:100
- **docs**: legacy/infra/2025-10-25-gcp/README.md:6, docs/diagrams/deps_cleanup.md:7, docs/diagrams/deps_cleanup.md:18, docs/cleanup.json:2221, docs/PROJECT_DOCUMENTATION.md:113, docs/CLEANUP_REPORT.md:93, docs/CLEANUP_REPORT.md:752
- Notas: Coordinar con Cloud Data Engineering antes de restaurar.
- Clasificación: QUARANTINE

### legacy/scripts/2025-10-25-generation/README.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:105, tools/audit_cleanup.py:106, tools/audit_cleanup.py:107, tools/audit_cleanup.py:258
- **docs**: docs/cleanup.json:199, docs/cleanup.json:1764, docs/cleanup.json:2080, docs/cleanup.json:2091, docs/cleanup.json:2139, docs/cleanup.json:2171, docs/cleanup.json:2189, docs/cleanup.json:2235
- Clasificación: KEEP

### legacy/scripts/2025-10-25-generation/generate_big_payments.py

Referencias detectadas:

- **code**: tools/audit_cleanup.py:50, tools/audit_cleanup.py:56, tools/audit_cleanup.py:87, tools/audit_cleanup.py:99
- **docs**: legacy/scripts/2025-10-25-generation/README.md:6, docs/diagrams/deps_cleanup.md:8, docs/diagrams/deps_cleanup.md:19, docs/cleanup.json:1513, docs/cleanup.json:1613, docs/cleanup.json:2284, docs/PROJECT_DOCUMENTATION.md:114, docs/CLEANUP_REPORT.md:95
- Notas: Reactivar solo con aprobación del owner registrado en README.
- Clasificación: QUARANTINE

### pipelines/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/common.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:128, legacy/docs/2025-10-25-reports/report.json:144
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:115, legacy/docs/2025-10-25-reports/REPORT.md:121, docs/cleanup.json:2343, docs/CLEANUP_REPORT.md:97, docs/CLEANUP_REPORT.md:785
- Clasificación: KEEP

### pipelines/config/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/config/loader.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2397, docs/PROJECT_DOCUMENTATION.md:15, docs/CLEANUP_REPORT.md:99, docs/CLEANUP_REPORT.md:800
- Clasificación: KEEP

### pipelines/database/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/database/db_manager.py

Referencias detectadas:

- **docs**: docs/cleanup.json:973, docs/cleanup.json:1126, docs/cleanup.json:2447, docs/CLEANUP_REPORT.md:101, docs/CLEANUP_REPORT.md:421, docs/CLEANUP_REPORT.md:458, docs/CLEANUP_REPORT.md:814
- Clasificación: KEEP

### pipelines/database/schema_mapper.py

Referencias detectadas:

- **configs**: config/schema_mapping.yml:2
- **docs**: docs/cleanup.json:972, docs/cleanup.json:1125, docs/cleanup.json:1380, docs/cleanup.json:2473, docs/CLEANUP_REPORT.md:102, docs/CLEANUP_REPORT.md:421, docs/CLEANUP_REPORT.md:458, docs/CLEANUP_REPORT.md:520
- Clasificación: KEEP

### pipelines/io/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/io/reader.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2529, docs/PROJECT_DOCUMENTATION.md:16, docs/CLEANUP_REPORT.md:104, docs/CLEANUP_REPORT.md:836
- Clasificación: KEEP

### pipelines/io/s3a.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2552, docs/CLEANUP_REPORT.md:105, docs/CLEANUP_REPORT.md:843, docs/PROJECT_DOCUMENTATION.md:16
- Clasificación: KEEP

### pipelines/io/writer.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2575, docs/PROJECT_DOCUMENTATION.md:16, docs/PROJECT_DOCUMENTATION.md:79, docs/CLEANUP_REPORT.md:106, docs/CLEANUP_REPORT.md:850
- Clasificación: KEEP

### pipelines/sources.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:61, legacy/docs/2025-10-25-reports/report.json:68
- **docs**: docs/cleanup.json:1340, docs/cleanup.json:1341, docs/cleanup.json:2599, docs/PROJECT_DOCUMENTATION.md:18, docs/PROJECT_DOCUMENTATION.md:56, docs/CLEANUP_REPORT.md:107, docs/CLEANUP_REPORT.md:511, docs/CLEANUP_REPORT.md:857
- Clasificación: KEEP

### pipelines/spark_job.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:75
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:22, legacy/docs/2025-10-25-reports/REPORT.md:86, legacy/docs/2025-10-25-reports/REPORT.md:92, legacy/docs/2025-10-25-reports/REPORT.md:134, legacy/docs/2025-10-25-reports/REPORT.md:157, docs/diagrams/pipeline_flow.md:7, docs/cleanup.json:2629, docs/CLEANUP_REPORT.md:108
- Clasificación: KEEP

### pipelines/spark_job_with_db.py

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:782, scripts/run_multiformat_case.py:110, scripts/runner.sh:106, scripts/runner_with_db.sh:61, scripts/run_high_volume_case.py:85, pipelines/spark_job_with_db.py:13
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset_api.yml:5
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:22, legacy/docs/2025-10-25-reports/REPORT.md:86, legacy/docs/2025-10-25-reports/REPORT.md:87, legacy/docs/2025-10-25-reports/REPORT.md:120, docs/diagrams/pipeline_flow.md:8, docs/cleanup.json:2658, docs/cleanup.json:2669, docs/PROJECT_DOCUMENTATION.md:17
- Clasificación: KEEP

### pipelines/transforms/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/transforms/apply.py

Referencias detectadas:

- **docs**: docs/cleanup.json:824, docs/cleanup.json:1011, docs/cleanup.json:1300, docs/cleanup.json:2722, docs/cleanup.json:2776, docs/PROJECT_DOCUMENTATION.md:14, docs/PROJECT_DOCUMENTATION.md:90, docs/CLEANUP_REPORT.md:111
- Clasificación: KEEP

### pipelines/transforms/tests/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/transforms/tests/test_apply.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2776, docs/PROJECT_DOCUMENTATION.md:90, docs/CLEANUP_REPORT.md:113, docs/CLEANUP_REPORT.md:903
- Clasificación: KEEP

### pipelines/udf_catalog.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2799, docs/CLEANUP_REPORT.md:114, docs/CLEANUP_REPORT.md:910
- Clasificación: KEEP

### pipelines/utils/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/utils/logger.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2848, docs/PROJECT_DOCUMENTATION.md:12, docs/CLEANUP_REPORT.md:116, docs/CLEANUP_REPORT.md:924
- Clasificación: KEEP

### pipelines/utils/parallel.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2871, docs/PROJECT_DOCUMENTATION.md:12, docs/CLEANUP_REPORT.md:117, docs/CLEANUP_REPORT.md:931
- Clasificación: KEEP

### pipelines/validation/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### pipelines/validation/quality.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:119
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:124, docs/cleanup.json:2921, docs/PROJECT_DOCUMENTATION.md:13, docs/CLEANUP_REPORT.md:119, docs/CLEANUP_REPORT.md:945
- Clasificación: KEEP

### pyproject.toml

Referencias detectadas:

- **code**: tools/audit_cleanup.py:256
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:84, docs/cleanup.json:2947, docs/PROJECT_DOCUMENTATION.md:91, docs/CLEANUP_REPORT.md:120, docs/CLEANUP_REPORT.md:953
- Clasificación: KEEP

### requirements.txt

Referencias detectadas:

- **code**: scripts/runner.sh:29, scripts/runner.sh:30, scripts/runner_with_db.sh:31, tools/audit_cleanup.py:257
- **configs**: ci/build-test.yml:28, ci/lint.yml:30
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:116, docs/cleanup.json:2973, docs/PROJECT_DOCUMENTATION.md:39, docs/CLEANUP_REPORT.md:121, docs/CLEANUP_REPORT.md:961, README.md:11
- Clasificación: KEEP

### scripts/db/init.sql

Referencias detectadas:

- **configs**: docker-compose.yml:22
- **docs**: docs/cleanup.json:3006, docs/CLEANUP_REPORT.md:122, docs/CLEANUP_REPORT.md:970
- Clasificación: KEEP

### scripts/generate_synthetic_data.py

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:21, scripts/generate_synthetic_data.py:22, scripts/generate_synthetic_data.py:23, scripts/generate_synthetic_data.py:670, scripts/generate_synthetic_data.py:671, scripts/generate_synthetic_data.py:672, scripts/generate_synthetic_data.py:673, scripts/run_multiformat_case.py:74
- **docs**: docs/diagrams/deps_cleanup.md:15, docs/cleanup.json:486, docs/cleanup.json:487, docs/cleanup.json:558, docs/cleanup.json:584, docs/cleanup.json:634, docs/cleanup.json:635, docs/cleanup.json:636
- Clasificación: KEEP

### scripts/run_high_volume_case.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:161
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:100, legacy/docs/2025-10-25-reports/REPORT.md:125, docs/cleanup.json:491, docs/cleanup.json:637, docs/cleanup.json:2668, docs/cleanup.json:3066, docs/CLEANUP_REPORT.md:124, docs/CLEANUP_REPORT.md:290
- Clasificación: KEEP

### scripts/run_multiformat_case.py

Referencias detectadas:

- **docs**: docs/cleanup.json:488, docs/cleanup.json:529, docs/cleanup.json:2665, docs/cleanup.json:3043, docs/cleanup.json:3095, docs/CLEANUP_REPORT.md:125, docs/CLEANUP_REPORT.md:290, docs/CLEANUP_REPORT.md:299
- Clasificación: KEEP

### scripts/runner.sh

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:136, legacy/docs/2025-10-25-reports/report.json:154, docker-compose.yml:85, docker-compose.yml:86
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:87, legacy/docs/2025-10-25-reports/REPORT.md:126, docs/cleanup.json:489, docs/cleanup.json:718, docs/cleanup.json:862, docs/cleanup.json:896, docs/cleanup.json:1049, docs/cleanup.json:1164
- Clasificación: KEEP

### scripts/runner_with_db.sh

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:132, legacy/docs/2025-10-25-reports/report.json:149, legacy/docs/2025-10-25-reports/report.json:166
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:87, legacy/docs/2025-10-25-reports/REPORT.md:100, legacy/docs/2025-10-25-reports/REPORT.md:125, legacy/docs/2025-10-25-reports/REPORT.md:126, docs/cleanup.json:490, docs/cleanup.json:719, docs/cleanup.json:863, docs/cleanup.json:897
- Clasificación: KEEP

### src/datacore/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/cli.py

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:85, legacy/docs/2025-10-25-reports/REPORT.md:119, docs/cleanup.json:3212, docs/CLEANUP_REPORT.md:129, docs/CLEANUP_REPORT.md:1024
- Clasificación: KEEP

### src/datacore/config/schema.py

Referencias detectadas:

- **docs**: docs/cleanup.json:22, docs/cleanup.json:752, docs/cleanup.json:930, docs/cleanup.json:1083, docs/cleanup.json:1228, docs/cleanup.json:3236, docs/cleanup.json:3709, docs/run/configs.md:5
- Clasificación: KEEP

### src/datacore/context.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3263, docs/CLEANUP_REPORT.md:131, docs/CLEANUP_REPORT.md:1038
- Clasificación: KEEP

### src/datacore/io/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/io/adapters.py

Referencias detectadas:

- **docs**: docs/cleanup.json:1342, docs/cleanup.json:3312, docs/cleanup.json:3736, docs/CLEANUP_REPORT.md:133, docs/CLEANUP_REPORT.md:148, docs/CLEANUP_REPORT.md:511, docs/CLEANUP_REPORT.md:1052, docs/CLEANUP_REPORT.md:1162
- Clasificación: KEEP

### src/datacore/io/fs.py

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:115, docs/cleanup.json:23, docs/cleanup.json:24, docs/cleanup.json:25, docs/cleanup.json:26, docs/cleanup.json:27, docs/cleanup.json:28, docs/cleanup.json:29
- Clasificación: KEEP

### src/datacore/layers/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/layers/bronze/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/layers/bronze/main.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:17, legacy/docs/2025-10-25-reports/report.json:31, legacy/docs/2025-10-25-reports/report.json:46
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:51, legacy/docs/2025-10-25-reports/REPORT.md:59, legacy/docs/2025-10-25-reports/REPORT.md:67, docs/cleanup.json:3420, docs/cleanup.json:3478, docs/cleanup.json:3536, docs/cleanup.json:3594, docs/CLEANUP_REPORT.md:137
- Clasificación: KEEP

### src/datacore/layers/gold/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/layers/gold/main.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:17, legacy/docs/2025-10-25-reports/report.json:31, legacy/docs/2025-10-25-reports/report.json:46
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:51, legacy/docs/2025-10-25-reports/REPORT.md:59, legacy/docs/2025-10-25-reports/REPORT.md:67, docs/cleanup.json:3420, docs/cleanup.json:3478, docs/cleanup.json:3536, docs/cleanup.json:3594, docs/CLEANUP_REPORT.md:137
- Clasificación: KEEP

### src/datacore/layers/raw/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/layers/raw/main.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:17, legacy/docs/2025-10-25-reports/report.json:31, legacy/docs/2025-10-25-reports/report.json:46
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:51, legacy/docs/2025-10-25-reports/REPORT.md:59, legacy/docs/2025-10-25-reports/REPORT.md:67, docs/cleanup.json:3420, docs/cleanup.json:3478, docs/cleanup.json:3536, docs/cleanup.json:3594, docs/CLEANUP_REPORT.md:137
- Clasificación: KEEP

### src/datacore/layers/silver/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/layers/silver/main.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:17, legacy/docs/2025-10-25-reports/report.json:31, legacy/docs/2025-10-25-reports/report.json:46
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:51, legacy/docs/2025-10-25-reports/REPORT.md:59, legacy/docs/2025-10-25-reports/REPORT.md:67, docs/cleanup.json:3420, docs/cleanup.json:3478, docs/cleanup.json:3536, docs/cleanup.json:3594, docs/CLEANUP_REPORT.md:137
- Clasificación: KEEP

### src/datacore/pipeline/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:974, docs/cleanup.json:1127, docs/cleanup.json:2316, docs/cleanup.json:2370, docs/cleanup.json:2420, docs/cleanup.json:2502, docs/cleanup.json:2695, docs/cleanup.json:2749
- Clasificación: KEEP

### src/datacore/pipeline/utils.py

Referencias detectadas:

- **configs**: legacy/docs/2025-10-25-reports/report.json:82, legacy/docs/2025-10-25-reports/report.json:91, legacy/docs/2025-10-25-reports/report.json:98, legacy/docs/2025-10-25-reports/report.json:105, legacy/docs/2025-10-25-reports/report.json:112
- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:122, legacy/docs/2025-10-25-reports/REPORT.md:123, docs/cleanup.json:3652, docs/CLEANUP_REPORT.md:145, docs/CLEANUP_REPORT.md:1140
- Clasificación: KEEP

### tests/test_cli_smoke.py

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:115, docs/cleanup.json:237, docs/cleanup.json:238, docs/cleanup.json:267, docs/cleanup.json:268, docs/cleanup.json:297, docs/cleanup.json:298, docs/cleanup.json:327
- Clasificación: KEEP

### tests/test_config_schema.py

Referencias detectadas:

- **docs**: docs/cleanup.json:752, docs/cleanup.json:930, docs/cleanup.json:1083, docs/cleanup.json:1228, docs/cleanup.json:3709, docs/run/configs.md:77, docs/CLEANUP_REPORT.md:147, docs/CLEANUP_REPORT.md:368
- Clasificación: KEEP

### tests/test_io_adapters.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3736, docs/CLEANUP_REPORT.md:148, docs/CLEANUP_REPORT.md:1162
- Clasificación: KEEP

### tests/test_io_fs.py

Referencias detectadas:

- **docs**: legacy/docs/2025-10-25-reports/REPORT.md:115, docs/cleanup.json:1509, docs/cleanup.json:1510, docs/cleanup.json:1511, docs/cleanup.json:1512, docs/cleanup.json:1609, docs/cleanup.json:1610, docs/cleanup.json:1611
- Clasificación: KEEP

### tests/test_layers_config.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3785, docs/CLEANUP_REPORT.md:150, docs/CLEANUP_REPORT.md:1176
- Clasificación: KEEP

### tests/test_security.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3807, docs/CLEANUP_REPORT.md:151, docs/CLEANUP_REPORT.md:1183
- Clasificación: KEEP

### tools/audit_cleanup.py

Referencias detectadas:

- **code**: tools/audit_cleanup.py:108
- **configs**: ci/lint.yml:32
- **docs**: docs/cleanup.json:111, docs/cleanup.json:112, docs/cleanup.json:205, docs/cleanup.json:206, docs/cleanup.json:207, docs/cleanup.json:208, docs/cleanup.json:1643, docs/cleanup.json:1698
- Clasificación: KEEP

### tools/list_io.py

Referencias detectadas:

- **configs**: ci/lint.yml:34
- **docs**: docs/cleanup.json:2062, docs/cleanup.json:3860, docs/PROJECT_DOCUMENTATION.md:115, docs/CLEANUP_REPORT.md:88, docs/CLEANUP_REPORT.md:153, docs/CLEANUP_REPORT.md:710, docs/CLEANUP_REPORT.md:1199, README.md:40
- Clasificación: KEEP

## Impacto estimado

- Archivos candidatos a eliminar inmediatamente: 0
- Archivos candidatos a cuarentena: 4
- Ahorro aproximado: 0.02 MB

## Riesgos y mitigaciones

- Revisar manualmente los elementos marcados como QUARANTINE antes de moverlos a /legacy.
- Confirmar dependencias transversales en CI/CD para los elementos KEEP críticos.
- Establecer un rollback rápido restaurando archivos desde git si un pipeline falla.

## Plan sugerido

1. Crear PRs por lote (infra, pipelines, documentación) para aplicar REMOVE/QUARANTINE.
2. Actualizar dependencias declaradas (según deptry) antes de eliminar código compartido.
3. Mover notebooks huérfanos a /legacy/notebooks con un README que documente su estado.

## Anexos

### Hallazgos de Vulture

```
vulture no disponible
```

### Hallazgos de Deptry

```
deptry no disponible
```

### Links rotos

- docs/run/databricks.md → https://docs.databricks.com/jobs/jobs-parameterization.html (<urlopen error Tunnel connection failed: 403 Forbidden>)
- docs/run/databricks.md → https://docs.databricks.com/security/secrets/index.html (<urlopen error Tunnel connection failed: 403 Forbidden>)
- docs/run/databricks.md → https://docs.databricks.com/workflows/jobs/jobs-notifications.html (<urlopen error Tunnel connection failed: 403 Forbidden>)
- docs/run/azure.md → https://learn.microsoft.com/azure/synapse-analytics/spark/spark-job-definitions (HTTP Error 404: Not Found)
- docs/run/azure.md → https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-monitor-application (HTTP Error 404: Not Found)
- legacy/docs/2025-10-25-reports/REPORT.md → diagrams/dependencies.md (missing file)
- legacy/docs/2025-10-25-reports/REPORT.md → diagrams/pipeline_flow.md (missing file)

### Documentos huérfanos

- Ninguno

### Notebooks huérfanos

- Ninguno