# Cleanup Audit Report

_Generated on 2025-10-25T02:12:43.911203Z_

## Resumen ejecutivo

| Clasificación | Conteo |
| --- | ---: |
| REMOVE | 1 |
| QUARANTINE | 4 |
| KEEP | 128 |

**Ahorro estimado**: 0.02 MB si se aplica REMOVE + QUARANTINE.

## Tabla maestra

| Path | Tipo | Tamaño (KB) | Último commit | # refs | Clasificación | Motivo | Riesgo | Acción |
| --- | --- | ---: | --- | ---: | --- | --- | --- | --- |
| .env | other | 0.48 | 2025-09-28 | 16 | KEEP | Referenced or recently modified. | medium | keep |
| .env.example | other | 0.55 | 2025-10-06 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .gitignore | other | 6.04 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/config.json | config | 0.69 | 2025-09-29 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/config.json.old | other | 0.69 | 2025-09-29 | 5 | REMOVE | Archivo de respaldo antiguo de MinIO; reemplazado por config.json | low | delete |
| .mc/share/downloads.json | config | 0.03 | 2025-09-29 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .mc/share/uploads.json | config | 0.03 | 2025-09-29 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| .pre-commit-config.yaml | config | 0.34 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| README.md | doc | 1.93 | 2025-10-24 | 9 | KEEP | Governance or generated artifact | low | keep |
| cfg/bronze/template.yml | config | 0.52 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/gold/template.yml | config | 0.51 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/raw/template.yml | config | 0.51 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| cfg/silver/template.yml | config | 0.52 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| ci/build-test.yml | config | 0.97 | 2025-10-24 | 5 | KEEP | Referenced or recently modified. | medium | keep |
| ci/check_config.sh | ci | 0.25 | 2025-09-28 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| ci/docker-compose.override.yml | config | 0.10 | 2025-10-08 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| ci/lint.yml | config | 0.34 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
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
| docs/CLEANUP_REPORT.md | doc | 110.83 | - | 10 | KEEP | Governance or generated artifact | low | keep |
| docs/PROJECT_DOCUMENTATION.md | doc | 5.29 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/REPORT.md | doc | 12.38 | 2025-10-24 | 11 | QUARANTINE | Reporte legacy previo a la documentación modular. | medium | move_to_legacy |
| docs/cleanup.json | doc | 135.98 | - | 10 | KEEP | Governance or generated artifact | low | keep |
| docs/diagrams/dependencies.md | doc | 1.24 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| docs/diagrams/deps_cleanup.md | doc | 0.64 | - | 9 | KEEP | Governance or generated artifact | low | keep |
| docs/diagrams/pipeline_flow.md | doc | 1.17 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/policies/DEP-001-legacy-removal.md | doc | 2.08 | - | 4 | KEEP | Governance or generated artifact | low | keep |
| docs/report.json | doc | 5.92 | 2025-10-24 | 9 | QUARANTINE | Export JSON antiguo duplicando reportes actuales. | medium | move_to_legacy |
| docs/run/aws.md | doc | 3.11 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/azure.md | doc | 3.28 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/configs.md | doc | 2.69 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/databricks.md | doc | 3.15 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/gcp.md | doc | 3.61 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/jobs/aws_stepfunctions.json | doc | 1.28 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/jobs/databricks_job.json | doc | 2.03 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| docs/run/jobs/dataproc_workflow.yaml | config | 1.68 | 2025-10-24 | 4 | QUARANTINE | Plantilla Dataproc sin referencias en CI/CD. | medium | move_to_legacy |
| docs/tools/list_io.py | code | 2.94 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/__init__.py | code | 0.16 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/common.py | code | 4.32 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/config/__init__.py | code | 0.05 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/config/loader.py | code | 0.75 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/__init__.py | code | 0.06 | 2025-10-08 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/db_manager.py | code | 29.79 | 2025-10-15 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/database/schema_mapper.py | code | 18.59 | 2025-10-09 | 9 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/__init__.py | code | 0.06 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/reader.py | code | 1.48 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/s3a.py | code | 0.48 | 2025-10-24 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/io/writer.py | code | 1.42 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/sources.py | code | 13.96 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/spark_job.py | code | 11.51 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/spark_job_with_db.py | code | 2.11 | 2025-10-24 | 16 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/__init__.py | code | 0.05 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/apply.py | code | 4.82 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/tests/__init__.py | code | 0.03 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/transforms/tests/test_apply.py | code | 1.23 | 2025-10-21 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/udf_catalog.py | code | 4.79 | 2025-10-13 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/__init__.py | code | 0.06 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/logger.py | code | 1.16 | 2025-10-21 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/utils/parallel.py | code | 0.95 | 2025-10-21 | 6 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/validation/__init__.py | code | 0.08 | 2025-10-21 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pipelines/validation/quality.py | code | 4.31 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| pyproject.toml | config | 0.81 | 2025-10-24 | 6 | KEEP | Governance or generated artifact | low | keep |
| requirements.txt | other | 0.68 | 2025-10-24 | 11 | KEEP | Governance or generated artifact | low | keep |
| scripts/db/init.sql | other | 1.67 | 2025-10-08 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/generate_big_payments.py | code | 1.27 | 2025-09-29 | 9 | QUARANTINE | Script de generación legacy reemplazado por generate_synthetic_data.py. | medium | move_to_legacy |
| scripts/generate_synthetic_data.py | code | 34.54 | 2025-10-24 | 16 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/run_high_volume_case.py | code | 8.63 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/run_multiformat_case.py | code | 15.11 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/runner.sh | other | 4.07 | 2025-10-24 | 10 | KEEP | Referenced or recently modified. | medium | keep |
| scripts/runner_with_db.sh | other | 3.03 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/__init__.py | code | 0.32 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/cli.py | code | 4.91 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/config/schema.py | code | 8.37 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/context.py | code | 6.29 | 2025-10-24 | 7 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/__init__.py | code | 0.31 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/adapters.py | code | 3.37 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/io/fs.py | code | 19.87 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/bronze/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/bronze/main.py | code | 6.72 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/gold/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/gold/main.py | code | 6.58 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/raw/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/raw/main.py | code | 0.62 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/silver/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/layers/silver/main.py | code | 6.17 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/pipeline/__init__.py | code | 0.00 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| src/datacore/pipeline/utils.py | code | 24.75 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_cli_smoke.py | code | 2.76 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_config_schema.py | code | 2.22 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_io_adapters.py | code | 1.88 | 2025-10-24 | 4 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_io_fs.py | code | 7.97 | 2025-10-24 | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_layers_config.py | code | 2.24 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| tests/test_security.py | code | 0.95 | 2025-10-24 | 3 | KEEP | Referenced or recently modified. | medium | keep |
| tools/audit_cleanup.py | code | 20.54 | - | 8 | KEEP | Referenced or recently modified. | medium | keep |
| tools/list_io.py | code | 3.18 | - | 8 | KEEP | Referenced or recently modified. | medium | keep |

## Evidencia por elemento

### .env

Referencias detectadas:

- **code**: scripts/run_high_volume_case.py:29, scripts/run_high_volume_case.py:30, scripts/run_high_volume_case.py:31, scripts/run_high_volume_case.py:39, scripts/run_multiformat_case.py:38, scripts/run_multiformat_case.py:39, scripts/run_multiformat_case.py:40, scripts/run_multiformat_case.py:44
- **docs**: docs/report.json:174, docs/report.json:175, docs/report.json:176, docs/report.json:177, docs/run/jobs/aws_stepfunctions.json:12, docs/run/jobs/aws_stepfunctions.json:24, docs/run/jobs/aws_stepfunctions.json:36, docs/run/jobs/aws_stepfunctions.json:48
- Clasificación: KEEP

### .env.example

Referencias detectadas:

- **docs**: docs/cleanup.json:53, docs/CLEANUP_REPORT.md:20, docs/CLEANUP_REPORT.md:163
- Clasificación: KEEP

### .gitignore

Referencias detectadas:

- **code**: data/output/.gitkeep:2, data/processed/.gitkeep:2, data/raw/.gitkeep:2
- **docs**: docs/cleanup.json:75, docs/CLEANUP_REPORT.md:21, docs/CLEANUP_REPORT.md:170
- Clasificación: KEEP

### .mc/config.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:26, tools/audit_cleanup.py:28
- **docs**: docs/diagrams/deps_cleanup.md:11, docs/cleanup.json:101, docs/cleanup.json:131, docs/cleanup.json:150, docs/CLEANUP_REPORT.md:22, docs/CLEANUP_REPORT.md:23, docs/CLEANUP_REPORT.md:178, docs/CLEANUP_REPORT.md:186
- Clasificación: KEEP

### .mc/config.json.old

Referencias detectadas:

- **code**: tools/audit_cleanup.py:26
- **docs**: docs/diagrams/deps_cleanup.md:11, docs/cleanup.json:131, docs/CLEANUP_REPORT.md:23, docs/CLEANUP_REPORT.md:186
- Notas: No se usa en CI ni en scripts actuales.
- Clasificación: REMOVE

### .mc/share/downloads.json

Referencias detectadas:

- **docs**: docs/cleanup.json:156, docs/CLEANUP_REPORT.md:24, docs/CLEANUP_REPORT.md:195
- Clasificación: KEEP

### .mc/share/uploads.json

Referencias detectadas:

- **docs**: docs/CLEANUP_REPORT.md:25, docs/CLEANUP_REPORT.md:202, docs/cleanup.json:178
- Clasificación: KEEP

### .pre-commit-config.yaml

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:91, docs/cleanup.json:200, docs/CLEANUP_REPORT.md:26, docs/CLEANUP_REPORT.md:209
- Clasificación: KEEP

### README.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:210
- **docs**: docs/PROJECT_DOCUMENTATION.md:107, docs/cleanup.json:223, docs/cleanup.json:525, docs/cleanup.json:1376, docs/cleanup.json:1377, docs/cleanup.json:1378, docs/cleanup.json:1751, docs/cleanup.json:2169
- Clasificación: KEEP

### cfg/bronze/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: docs/cleanup.json:252, docs/cleanup.json:282, docs/cleanup.json:312, docs/cleanup.json:342, docs/cleanup.json:1369, docs/cleanup.json:1370, docs/cleanup.json:1371, docs/cleanup.json:1372
- Clasificación: KEEP

### cfg/gold/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: docs/cleanup.json:252, docs/cleanup.json:282, docs/cleanup.json:312, docs/cleanup.json:342, docs/cleanup.json:1369, docs/cleanup.json:1370, docs/cleanup.json:1371, docs/cleanup.json:1372
- Clasificación: KEEP

### cfg/raw/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: docs/cleanup.json:252, docs/cleanup.json:282, docs/cleanup.json:312, docs/cleanup.json:342, docs/cleanup.json:1369, docs/cleanup.json:1370, docs/cleanup.json:1371, docs/cleanup.json:1372
- Clasificación: KEEP

### cfg/silver/template.yml

Referencias detectadas:

- **code**: tests/test_cli_smoke.py:20, tests/test_cli_smoke.py:27
- **docs**: docs/cleanup.json:252, docs/cleanup.json:282, docs/cleanup.json:312, docs/cleanup.json:342, docs/cleanup.json:1369, docs/cleanup.json:1370, docs/cleanup.json:1371, docs/cleanup.json:1372
- Clasificación: KEEP

### ci/build-test.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:372, docs/cleanup.json:2862, docs/CLEANUP_REPORT.md:32, docs/CLEANUP_REPORT.md:256, docs/CLEANUP_REPORT.md:933
- Clasificación: KEEP

### ci/check_config.sh

Referencias detectadas:

- **configs**: ci/lint.yml:18
- **docs**: docs/cleanup.json:396, docs/cleanup.json:1004, docs/cleanup.json:1157, docs/CLEANUP_REPORT.md:33, docs/CLEANUP_REPORT.md:263, docs/CLEANUP_REPORT.md:429, docs/CLEANUP_REPORT.md:466, docs/REPORT.md:114
- Clasificación: KEEP

### ci/docker-compose.override.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:425, docs/CLEANUP_REPORT.md:34, docs/CLEANUP_REPORT.md:271
- Clasificación: KEEP

### ci/lint.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:403, docs/cleanup.json:447, docs/CLEANUP_REPORT.md:35, docs/CLEANUP_REPORT.md:267, docs/CLEANUP_REPORT.md:278, docs/REPORT.md:114, docs/REPORT.md:128
- Clasificación: KEEP

### ci/test_dataset.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:473, docs/cleanup.json:1001, docs/cleanup.json:1154, docs/cleanup.json:1537, docs/cleanup.json:1637, docs/CLEANUP_REPORT.md:36, docs/CLEANUP_REPORT.md:285, docs/CLEANUP_REPORT.md:428
- Clasificación: KEEP

### config/database.yml

Referencias detectadas:

- **code**: scripts/run_high_volume_case.py:79, scripts/runner.sh:7, scripts/runner_with_db.sh:7, scripts/generate_synthetic_data.py:580, scripts/generate_synthetic_data.py:639, scripts/run_multiformat_case.py:104
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:44, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:78, config/datasets/finanzas/payments_v3/dataset_api.yml:5, config/datasets/finanzas/payments_v3/dataset_api.yml:83, config/datasets/finanzas/payments_v1/dataset.yml:49, config/datasets/casos_uso/payments_high_volume.yml:69, config/datasets/casos_uso/events_multiformat.yml:71
- **docs**: README.md:13, docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:31, docs/PROJECT_DOCUMENTATION.md:40, docs/run/configs.md:22, docs/run/configs.md:57, docs/cleanup.json:500, docs/CLEANUP_REPORT.md:37
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat.yml

Referencias detectadas:

- **code**: scripts/run_multiformat_case.py:102
- **docs**: docs/cleanup.json:521, docs/cleanup.json:543, docs/cleanup.json:581, docs/cleanup.json:607, docs/cleanup.json:631, docs/cleanup.json:854, docs/cleanup.json:1000, docs/cleanup.json:1041
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:624
- **configs**: config/datasets/casos_uso/events_multiformat.yml:42
- **docs**: docs/cleanup.json:572, docs/CLEANUP_REPORT.md:39, docs/CLEANUP_REPORT.md:309
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:621
- **configs**: config/datasets/casos_uso/events_multiformat.yml:46
- **docs**: docs/cleanup.json:598, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:318
- Clasificación: KEEP

### config/datasets/casos_uso/events_multiformat_transforms.yml

Referencias detectadas:

- **configs**: config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/cleanup.json:624, docs/CLEANUP_REPORT.md:41, docs/CLEANUP_REPORT.md:327
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume.yml

Referencias detectadas:

- **code**: scripts/run_high_volume_case.py:77, scripts/generate_synthetic_data.py:599, scripts/generate_synthetic_data.py:603, scripts/generate_synthetic_data.py:782
- **docs**: docs/cleanup.json:520, docs/cleanup.json:648, docs/cleanup.json:689, docs/cleanup.json:715, docs/cleanup.json:784, docs/cleanup.json:962, docs/cleanup.json:999, docs/cleanup.json:1115
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume_expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562
- **configs**: config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/cleanup.json:680, docs/CLEANUP_REPORT.md:43, docs/CLEANUP_REPORT.md:343
- Clasificación: KEEP

### config/datasets/casos_uso/payments_high_volume_schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566
- **configs**: config/datasets/casos_uso/payments_high_volume.yml:49
- **docs**: docs/cleanup.json:706, docs/CLEANUP_REPORT.md:44, docs/CLEANUP_REPORT.md:352
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:24, docs/cleanup.json:473, docs/cleanup.json:514, docs/cleanup.json:515, docs/cleanup.json:516, docs/cleanup.json:519, docs/cleanup.json:732
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624, tests/test_config_schema.py:30
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/run/configs.md:52, docs/cleanup.json:572, docs/cleanup.json:680, docs/cleanup.json:766, docs/cleanup.json:944, docs/cleanup.json:1097, docs/cleanup.json:1242, docs/CLEANUP_REPORT.md:39
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/schema.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:34, config/datasets/finanzas/payments_db_only/dataset.yml:47, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:56, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:63
- **docs**: docs/CLEANUP_REPORT.md:47, docs/CLEANUP_REPORT.md:60, docs/CLEANUP_REPORT.md:379, docs/CLEANUP_REPORT.md:496, docs/cleanup.json:806, docs/cleanup.json:1282
- Clasificación: KEEP

### config/datasets/finanzas/payments_db_only/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/PROJECT_DOCUMENTATION.md:28, docs/cleanup.json:624, docs/cleanup.json:838, docs/cleanup.json:1025, docs/cleanup.json:1314, docs/CLEANUP_REPORT.md:41, docs/CLEANUP_REPORT.md:48, docs/CLEANUP_REPORT.md:53
- Clasificación: KEEP

### config/datasets/finanzas/payments_multi/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:24, docs/cleanup.json:473, docs/cleanup.json:514, docs/cleanup.json:515, docs/cleanup.json:516, docs/cleanup.json:519, docs/cleanup.json:732
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:24, docs/cleanup.json:473, docs/cleanup.json:514, docs/cleanup.json:515, docs/cleanup.json:516, docs/cleanup.json:519, docs/cleanup.json:732
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624, tests/test_config_schema.py:30
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/run/configs.md:52, docs/cleanup.json:572, docs/cleanup.json:680, docs/cleanup.json:766, docs/cleanup.json:944, docs/cleanup.json:1097, docs/cleanup.json:1242, docs/CLEANUP_REPORT.md:39
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566, scripts/generate_synthetic_data.py:621, pipelines/database/db_manager.py:706, pipelines/database/__init__.py:1, pipelines/database/schema_mapper.py:428
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:25, config/datasets/finanzas/payments_v1/dataset.yml:34, config/datasets/casos_uso/payments_high_volume.yml:49, config/datasets/casos_uso/events_multiformat.yml:46, ci/test_dataset.yml:9
- **ci**: ci/check_config.sh:6
- **docs**: docs/cleanup.json:598, docs/cleanup.json:706, docs/cleanup.json:984, docs/cleanup.json:1137, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:44, docs/CLEANUP_REPORT.md:52, docs/CLEANUP_REPORT.md:56
- Clasificación: KEEP

### config/datasets/finanzas/payments_v1/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/PROJECT_DOCUMENTATION.md:28, docs/cleanup.json:624, docs/cleanup.json:838, docs/cleanup.json:1025, docs/cleanup.json:1314, docs/CLEANUP_REPORT.md:41, docs/CLEANUP_REPORT.md:48, docs/CLEANUP_REPORT.md:53
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:24, docs/CLEANUP_REPORT.md:36, docs/CLEANUP_REPORT.md:45, docs/CLEANUP_REPORT.md:49, docs/CLEANUP_REPORT.md:50, docs/CLEANUP_REPORT.md:54, docs/CLEANUP_REPORT.md:57
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624, tests/test_config_schema.py:30
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/run/configs.md:52, docs/cleanup.json:572, docs/cleanup.json:680, docs/cleanup.json:766, docs/cleanup.json:944, docs/cleanup.json:1097, docs/cleanup.json:1242, docs/CLEANUP_REPORT.md:39
- Clasificación: KEEP

### config/datasets/finanzas/payments_v2/schema.json

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:566, scripts/generate_synthetic_data.py:621, pipelines/database/db_manager.py:706, pipelines/database/__init__.py:1, pipelines/database/schema_mapper.py:428
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:25, config/datasets/finanzas/payments_v1/dataset.yml:34, config/datasets/casos_uso/payments_high_volume.yml:49, config/datasets/casos_uso/events_multiformat.yml:46, ci/test_dataset.yml:9
- **ci**: ci/check_config.sh:6
- **docs**: docs/cleanup.json:598, docs/cleanup.json:706, docs/cleanup.json:984, docs/cleanup.json:1137, docs/CLEANUP_REPORT.md:40, docs/CLEANUP_REPORT.md:44, docs/CLEANUP_REPORT.md:52, docs/CLEANUP_REPORT.md:56
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/dataset.yml

Referencias detectadas:

- **code**: scripts/runner.sh:5, scripts/runner_with_db.sh:5
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:5, config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset.yml:90
- **docs**: docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:24, docs/cleanup.json:473, docs/cleanup.json:514, docs/cleanup.json:515, docs/cleanup.json:516, docs/cleanup.json:519, docs/cleanup.json:732
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/dataset_api.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_v3/dataset_api.yml:4, config/datasets/finanzas/payments_v3/dataset_api.yml:5
- **docs**: docs/cleanup.json:517, docs/cleanup.json:518, docs/cleanup.json:781, docs/cleanup.json:782, docs/cleanup.json:817, docs/cleanup.json:818, docs/cleanup.json:851, docs/cleanup.json:852
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/expectations.yml

Referencias detectadas:

- **code**: scripts/generate_synthetic_data.py:562, scripts/generate_synthetic_data.py:624, tests/test_config_schema.py:30
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:74, config/datasets/finanzas/payments_v2/dataset.yml:29, config/datasets/finanzas/payments_db_only/dataset.yml:54, config/datasets/finanzas/payments_v3/dataset.yml:63, config/datasets/finanzas/payments_v3/dataset_api.yml:69, config/datasets/finanzas/payments_v3/dataset_api.yml:150, config/datasets/finanzas/payments_v1/dataset.yml:30, config/datasets/casos_uso/payments_high_volume.yml:45
- **docs**: docs/run/configs.md:52, docs/cleanup.json:572, docs/cleanup.json:680, docs/cleanup.json:766, docs/cleanup.json:944, docs/cleanup.json:1097, docs/cleanup.json:1242, docs/CLEANUP_REPORT.md:39
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/schema.yml

Referencias detectadas:

- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:34, config/datasets/finanzas/payments_db_only/dataset.yml:47, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:56, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:63
- **docs**: docs/cleanup.json:806, docs/cleanup.json:1282, docs/CLEANUP_REPORT.md:47, docs/CLEANUP_REPORT.md:60, docs/CLEANUP_REPORT.md:379, docs/CLEANUP_REPORT.md:496
- Clasificación: KEEP

### config/datasets/finanzas/payments_v3/transforms.yml

Referencias detectadas:

- **code**: pipelines/transforms/apply.py:10
- **configs**: config/datasets/finanzas/payments_multi/dataset.yml:77, config/datasets/finanzas/payments_db_only/dataset.yml:51, config/datasets/finanzas/payments_v3/dataset.yml:2, config/datasets/finanzas/payments_v3/dataset.yml:60, config/datasets/finanzas/payments_v3/dataset_api.yml:2, config/datasets/finanzas/payments_v3/dataset_api.yml:66, config/datasets/finanzas/payments_v1/dataset.yml:11, config/datasets/casos_uso/events_multiformat.yml:50
- **docs**: docs/PROJECT_DOCUMENTATION.md:28, docs/cleanup.json:624, docs/cleanup.json:838, docs/cleanup.json:1025, docs/cleanup.json:1314, docs/CLEANUP_REPORT.md:41, docs/CLEANUP_REPORT.md:48, docs/CLEANUP_REPORT.md:53
- Clasificación: KEEP

### config/env.yml

Referencias detectadas:

- **code**: scripts/runner.sh:6, scripts/runner_with_db.sh:6, pipelines/sources.py:113, pipelines/sources.py:117, src/datacore/io/adapters.py:63
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset_api.yml:5, config/datasets/finanzas/payments_v3/dataset_api.yml:15, config/datasets/finanzas/payments_v3/dataset_api.yml:103, cfg/raw/template.yml:7, cfg/gold/template.yml:7, cfg/silver/template.yml:7, cfg/bronze/template.yml:7
- **docs**: README.md:13, README.md:15, README.md:38, docs/PROJECT_DOCUMENTATION.md:10, docs/PROJECT_DOCUMENTATION.md:22, docs/PROJECT_DOCUMENTATION.md:40, docs/PROJECT_DOCUMENTATION.md:42, docs/run/configs.md:20
- Clasificación: KEEP

### config/schema_mapping.yml

Referencias detectadas:

- **code**: pipelines/database/schema_mapper.py:48
- **docs**: docs/CLEANUP_REPORT.md:63, docs/CLEANUP_REPORT.md:522, docs/CLEANUP_REPORT.md:795, docs/cleanup.json:1394, docs/cleanup.json:2354
- Clasificación: KEEP

### data/casos-uso/multi-format/events_batch_001.json

Referencias detectadas:

- **docs**: docs/CLEANUP_REPORT.md:64, docs/CLEANUP_REPORT.md:530, docs/cleanup.json:1420
- Clasificación: KEEP

### data/output/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:81, docs/cleanup.json:82, docs/cleanup.json:83, docs/cleanup.json:1442, docs/cleanup.json:1469, docs/cleanup.json:1496, docs/CLEANUP_REPORT.md:65, docs/CLEANUP_REPORT.md:66
- Clasificación: KEEP

### data/processed/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:81, docs/cleanup.json:82, docs/cleanup.json:83, docs/cleanup.json:1442, docs/cleanup.json:1469, docs/cleanup.json:1496, docs/CLEANUP_REPORT.md:65, docs/CLEANUP_REPORT.md:66
- Clasificación: KEEP

### data/raw/.gitkeep

Referencias detectadas:

- **docs**: docs/cleanup.json:81, docs/cleanup.json:82, docs/cleanup.json:83, docs/cleanup.json:1442, docs/cleanup.json:1469, docs/cleanup.json:1496, docs/CLEANUP_REPORT.md:65, docs/CLEANUP_REPORT.md:66
- Clasificación: KEEP

### data/raw/payments/sample.csv

Referencias detectadas:

- **code**: scripts/generate_big_payments.py:6, tests/test_io_fs.py:185, tests/test_io_fs.py:186, tests/test_io_fs.py:187, tests/test_io_fs.py:211
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:5, ci/test_dataset.yml:5
- **docs**: docs/CLEANUP_REPORT.md:68, docs/CLEANUP_REPORT.md:72, docs/CLEANUP_REPORT.md:558, docs/CLEANUP_REPORT.md:588, docs/cleanup.json:1523, docs/cleanup.json:1623
- Clasificación: KEEP

### data/s3a-staging/finanzas/payments/raw/sample.jsonl

Referencias detectadas:

- **docs**: docs/cleanup.json:1557, docs/CLEANUP_REPORT.md:69, docs/CLEANUP_REPORT.md:567
- Clasificación: KEEP

### data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_001.csv

Referencias detectadas:

- **docs**: docs/cleanup.json:1579, docs/CLEANUP_REPORT.md:70, docs/CLEANUP_REPORT.md:574
- Clasificación: KEEP

### data/s3a-staging/raw/casos-uso/payments-high-volume/payments-high-volume/payments_batch_002.csv

Referencias detectadas:

- **docs**: docs/cleanup.json:1601, docs/CLEANUP_REPORT.md:71, docs/CLEANUP_REPORT.md:581
- Clasificación: KEEP

### data/s3a-staging/raw/payments_v3/sample.csv

Referencias detectadas:

- **code**: scripts/generate_big_payments.py:6, tests/test_io_fs.py:185, tests/test_io_fs.py:186, tests/test_io_fs.py:187, tests/test_io_fs.py:211
- **configs**: config/datasets/finanzas/payments_v2/dataset.yml:5, ci/test_dataset.yml:5
- **docs**: docs/cleanup.json:1523, docs/cleanup.json:1623, docs/CLEANUP_REPORT.md:68, docs/CLEANUP_REPORT.md:72, docs/CLEANUP_REPORT.md:558, docs/CLEANUP_REPORT.md:588
- Clasificación: KEEP

### docker/spark-pandas-udf/Dockerfile

Referencias detectadas:

- **code**: tools/audit_cleanup.py:131
- **configs**: docker-compose.yml:49, docker-compose.yml:65, docker-compose.yml:81
- **docs**: docs/cleanup.json:1657, docs/CLEANUP_REPORT.md:73, docs/CLEANUP_REPORT.md:597
- Clasificación: KEEP

### docker-compose.yml

Referencias detectadas:

- **docs**: docs/cleanup.json:1666, docs/cleanup.json:1667, docs/cleanup.json:1668, docs/cleanup.json:1685, docs/cleanup.json:2889, docs/cleanup.json:3032, docs/cleanup.json:3033, docs/CLEANUP_REPORT.md:74
- Clasificación: KEEP

### docs/CLEANUP_REPORT.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:211, tools/audit_cleanup.py:527
- **docs**: docs/diagrams/deps_cleanup.md:15, docs/cleanup.json:63, docs/cleanup.json:64, docs/cleanup.json:89, docs/cleanup.json:90, docs/cleanup.json:117, docs/cleanup.json:118, docs/cleanup.json:119
- Clasificación: KEEP

### docs/PROJECT_DOCUMENTATION.md

Referencias detectadas:

- **docs**: README.md:33, docs/cleanup.json:209, docs/cleanup.json:234, docs/cleanup.json:526, docs/cleanup.json:527, docs/cleanup.json:528, docs/cleanup.json:748, docs/cleanup.json:749
- Clasificación: KEEP

### docs/REPORT.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:33, tools/audit_cleanup.py:211, tools/audit_cleanup.py:527
- **docs**: docs/diagrams/deps_cleanup.md:9, docs/diagrams/deps_cleanup.md:15, docs/cleanup.json:63, docs/cleanup.json:64, docs/cleanup.json:89, docs/cleanup.json:90, docs/cleanup.json:117, docs/cleanup.json:118
- Notas: Conservar en /legacy/docs hasta validar que no sea referencia externa.
- Clasificación: QUARANTINE

### docs/cleanup.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:212, tools/audit_cleanup.py:526
- **docs**: docs/cleanup.json:62, docs/cleanup.json:88, docs/cleanup.json:114, docs/cleanup.json:115, docs/cleanup.json:116, docs/cleanup.json:143, docs/cleanup.json:165, docs/cleanup.json:187
- Clasificación: KEEP

### docs/diagrams/dependencies.md

Referencias detectadas:

- **docs**: docs/REPORT.md:45, docs/cleanup.json:1830, docs/CLEANUP_REPORT.md:79, docs/CLEANUP_REPORT.md:645
- Clasificación: KEEP

### docs/diagrams/deps_cleanup.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:213
- **docs**: docs/cleanup.json:113, docs/cleanup.json:142, docs/cleanup.json:1724, docs/cleanup.json:1782, docs/cleanup.json:1783, docs/cleanup.json:1853, docs/cleanup.json:1945, docs/cleanup.json:2917
- Clasificación: KEEP

### docs/diagrams/pipeline_flow.md

Referencias detectadas:

- **docs**: docs/cleanup.json:1881, docs/cleanup.json:2511, docs/cleanup.json:2550, docs/CLEANUP_REPORT.md:81, docs/CLEANUP_REPORT.md:660, docs/CLEANUP_REPORT.md:838, docs/CLEANUP_REPORT.md:847, docs/REPORT.md:46
- Clasificación: KEEP

### docs/policies/DEP-001-legacy-removal.md

Referencias detectadas:

- **code**: tools/audit_cleanup.py:214
- **docs**: docs/cleanup.json:1908, docs/CLEANUP_REPORT.md:82, docs/CLEANUP_REPORT.md:667
- Clasificación: KEEP

### docs/report.json

Referencias detectadas:

- **code**: tools/audit_cleanup.py:40
- **docs**: docs/diagrams/deps_cleanup.md:10, docs/cleanup.json:35, docs/cleanup.json:36, docs/cleanup.json:37, docs/cleanup.json:38, docs/cleanup.json:1934, docs/cleanup.json:2222, docs/cleanup.json:2223
- Notas: Generado por tooling previo; no usado en pipelines.
- Clasificación: QUARANTINE

### docs/run/aws.md

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:49, docs/cleanup.json:1963, docs/CLEANUP_REPORT.md:84, docs/CLEANUP_REPORT.md:684
- Clasificación: KEEP

### docs/run/azure.md

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:51, docs/cleanup.json:1986, docs/CLEANUP_REPORT.md:85, docs/CLEANUP_REPORT.md:691, docs/CLEANUP_REPORT.md:1336, docs/CLEANUP_REPORT.md:1337
- Clasificación: KEEP

### docs/run/configs.md

Referencias detectadas:

- **docs**: docs/cleanup.json:529, docs/cleanup.json:530, docs/cleanup.json:788, docs/cleanup.json:966, docs/cleanup.json:1119, docs/cleanup.json:1264, docs/cleanup.json:1383, docs/cleanup.json:2011
- Clasificación: KEEP

### docs/run/databricks.md

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:48, docs/cleanup.json:2038, docs/CLEANUP_REPORT.md:87, docs/CLEANUP_REPORT.md:705, docs/CLEANUP_REPORT.md:1338, docs/CLEANUP_REPORT.md:1339, docs/CLEANUP_REPORT.md:1340
- Clasificación: KEEP

### docs/run/gcp.md

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:50, docs/cleanup.json:2064, docs/CLEANUP_REPORT.md:88, docs/CLEANUP_REPORT.md:712
- Clasificación: KEEP

### docs/run/jobs/aws_stepfunctions.json

Referencias detectadas:

- **docs**: docs/cleanup.json:39, docs/cleanup.json:40, docs/cleanup.json:41, docs/cleanup.json:42, docs/cleanup.json:2087, docs/CLEANUP_REPORT.md:89, docs/CLEANUP_REPORT.md:160, docs/CLEANUP_REPORT.md:719
- Clasificación: KEEP

### docs/run/jobs/databricks_job.json

Referencias detectadas:

- **docs**: docs/cleanup.json:2114, docs/CLEANUP_REPORT.md:90, docs/CLEANUP_REPORT.md:726
- Clasificación: KEEP

### docs/run/jobs/dataproc_workflow.yaml

Referencias detectadas:

- **code**: tools/audit_cleanup.py:54
- **docs**: docs/cleanup.json:2136, docs/CLEANUP_REPORT.md:91, docs/CLEANUP_REPORT.md:733
- Notas: Considerar mover a legacy/infra antes de eliminar.
- Clasificación: QUARANTINE

### docs/tools/list_io.py

Referencias detectadas:

- **docs**: README.md:40, docs/cleanup.json:2160, docs/cleanup.json:3737, docs/CLEANUP_REPORT.md:92, docs/CLEANUP_REPORT.md:151, docs/CLEANUP_REPORT.md:742, docs/CLEANUP_REPORT.md:1166, docs/CLEANUP_REPORT.md:1259
- Clasificación: KEEP

### pipelines/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/common.py

Referencias detectadas:

- **docs**: docs/report.json:128, docs/report.json:144, docs/cleanup.json:2213, docs/CLEANUP_REPORT.md:94, docs/CLEANUP_REPORT.md:756, docs/CLEANUP_REPORT.md:1196, docs/CLEANUP_REPORT.md:1260, docs/CLEANUP_REPORT.md:1261
- Clasificación: KEEP

### pipelines/config/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/config/loader.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:15, docs/cleanup.json:2267, docs/CLEANUP_REPORT.md:96, docs/CLEANUP_REPORT.md:770, docs/CLEANUP_REPORT.md:1197, docs/CLEANUP_REPORT.md:1198, docs/CLEANUP_REPORT.md:1199, docs/CLEANUP_REPORT.md:1265
- Clasificación: KEEP

### pipelines/database/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/database/db_manager.py

Referencias detectadas:

- **docs**: docs/cleanup.json:992, docs/cleanup.json:1145, docs/cleanup.json:2320, docs/CLEANUP_REPORT.md:98, docs/CLEANUP_REPORT.md:427, docs/CLEANUP_REPORT.md:464, docs/CLEANUP_REPORT.md:784, docs/CLEANUP_REPORT.md:1200
- Clasificación: KEEP

### pipelines/database/schema_mapper.py

Referencias detectadas:

- **configs**: config/schema_mapping.yml:2
- **docs**: docs/cleanup.json:994, docs/cleanup.json:1147, docs/cleanup.json:1400, docs/cleanup.json:2347, docs/CLEANUP_REPORT.md:99, docs/CLEANUP_REPORT.md:427, docs/CLEANUP_REPORT.md:464, docs/CLEANUP_REPORT.md:526
- Clasificación: KEEP

### pipelines/io/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/io/reader.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:16, docs/cleanup.json:2403, docs/CLEANUP_REPORT.md:101, docs/CLEANUP_REPORT.md:806, docs/CLEANUP_REPORT.md:1219, docs/CLEANUP_REPORT.md:1273, docs/CLEANUP_REPORT.md:1274, docs/CLEANUP_REPORT.md:1275
- Clasificación: KEEP

### pipelines/io/s3a.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:16, docs/cleanup.json:2427, docs/CLEANUP_REPORT.md:102, docs/CLEANUP_REPORT.md:813, docs/CLEANUP_REPORT.md:1220, docs/CLEANUP_REPORT.md:1276
- Clasificación: KEEP

### pipelines/io/writer.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:16, docs/PROJECT_DOCUMENTATION.md:79, docs/cleanup.json:2451, docs/CLEANUP_REPORT.md:103, docs/CLEANUP_REPORT.md:820, docs/CLEANUP_REPORT.md:1277, docs/CLEANUP_REPORT.md:1278
- Clasificación: KEEP

### pipelines/sources.py

Referencias detectadas:

- **docs**: docs/report.json:61, docs/report.json:68, docs/PROJECT_DOCUMENTATION.md:18, docs/PROJECT_DOCUMENTATION.md:56, docs/REPORT.md:155, docs/REPORT.md:156, docs/cleanup.json:1360, docs/cleanup.json:1361
- Clasificación: KEEP

### pipelines/spark_job.py

Referencias detectadas:

- **docs**: docs/diagrams/pipeline_flow.md:7, docs/report.json:75, docs/cleanup.json:2502, docs/CLEANUP_REPORT.md:105, docs/CLEANUP_REPORT.md:834, docs/CLEANUP_REPORT.md:1221, docs/CLEANUP_REPORT.md:1282, docs/CLEANUP_REPORT.md:1283
- Clasificación: KEEP

### pipelines/spark_job_with_db.py

Referencias detectadas:

- **code**: scripts/run_high_volume_case.py:85, scripts/runner.sh:106, scripts/runner_with_db.sh:61, scripts/generate_synthetic_data.py:782, scripts/run_multiformat_case.py:110, pipelines/spark_job_with_db.py:13
- **configs**: config/datasets/finanzas/payments_v3/dataset.yml:6, config/datasets/finanzas/payments_v3/dataset_api.yml:5
- **docs**: README.md:13, README.md:29, docs/diagrams/pipeline_flow.md:8, docs/PROJECT_DOCUMENTATION.md:17, docs/PROJECT_DOCUMENTATION.md:40, docs/cleanup.json:2529, docs/cleanup.json:2540, docs/CLEANUP_REPORT.md:106
- Clasificación: KEEP

### pipelines/transforms/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/transforms/apply.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:14, docs/PROJECT_DOCUMENTATION.md:90, docs/cleanup.json:844, docs/cleanup.json:1031, docs/cleanup.json:1320, docs/cleanup.json:2593, docs/cleanup.json:2647, docs/CLEANUP_REPORT.md:108
- Clasificación: KEEP

### pipelines/transforms/tests/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/transforms/tests/test_apply.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:90, docs/cleanup.json:2647, docs/CLEANUP_REPORT.md:110, docs/CLEANUP_REPORT.md:871, docs/CLEANUP_REPORT.md:1293, docs/CLEANUP_REPORT.md:1294, docs/CLEANUP_REPORT.md:1295
- Clasificación: KEEP

### pipelines/udf_catalog.py

Referencias detectadas:

- **docs**: docs/cleanup.json:2670, docs/CLEANUP_REPORT.md:111, docs/CLEANUP_REPORT.md:878, docs/CLEANUP_REPORT.md:1222, docs/CLEANUP_REPORT.md:1223, docs/CLEANUP_REPORT.md:1224, docs/CLEANUP_REPORT.md:1225, docs/CLEANUP_REPORT.md:1226
- Clasificación: KEEP

### pipelines/utils/__init__.py

Referencias detectadas:

- **docs**: docs/CLEANUP_REPORT.md:93, docs/CLEANUP_REPORT.md:95, docs/CLEANUP_REPORT.md:97, docs/CLEANUP_REPORT.md:100, docs/CLEANUP_REPORT.md:107, docs/CLEANUP_REPORT.md:109, docs/CLEANUP_REPORT.md:112, docs/CLEANUP_REPORT.md:115
- Clasificación: KEEP

### pipelines/utils/logger.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:12, docs/cleanup.json:2724, docs/CLEANUP_REPORT.md:113, docs/CLEANUP_REPORT.md:892
- Clasificación: KEEP

### pipelines/utils/parallel.py

Referencias detectadas:

- **docs**: docs/PROJECT_DOCUMENTATION.md:12, docs/cleanup.json:2747, docs/CLEANUP_REPORT.md:114, docs/CLEANUP_REPORT.md:899, docs/CLEANUP_REPORT.md:1230, docs/CLEANUP_REPORT.md:1231
- Clasificación: KEEP

### pipelines/validation/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### pipelines/validation/quality.py

Referencias detectadas:

- **docs**: docs/report.json:119, docs/PROJECT_DOCUMENTATION.md:13, docs/cleanup.json:2799, docs/CLEANUP_REPORT.md:116, docs/CLEANUP_REPORT.md:913, docs/CLEANUP_REPORT.md:1300, docs/CLEANUP_REPORT.md:1301, docs/REPORT.md:124
- Clasificación: KEEP

### pyproject.toml

Referencias detectadas:

- **code**: tools/audit_cleanup.py:208
- **docs**: docs/PROJECT_DOCUMENTATION.md:91, docs/cleanup.json:2824, docs/CLEANUP_REPORT.md:117, docs/CLEANUP_REPORT.md:920, docs/REPORT.md:84
- Clasificación: KEEP

### requirements.txt

Referencias detectadas:

- **code**: scripts/runner.sh:29, scripts/runner.sh:30, scripts/runner_with_db.sh:31, tools/audit_cleanup.py:209
- **configs**: ci/build-test.yml:28
- **docs**: README.md:11, docs/PROJECT_DOCUMENTATION.md:39, docs/cleanup.json:2850, docs/CLEANUP_REPORT.md:118, docs/CLEANUP_REPORT.md:928, docs/REPORT.md:116
- Clasificación: KEEP

### scripts/db/init.sql

Referencias detectadas:

- **configs**: docker-compose.yml:22
- **docs**: docs/cleanup.json:2882, docs/CLEANUP_REPORT.md:119, docs/CLEANUP_REPORT.md:937
- Clasificación: KEEP

### scripts/generate_big_payments.py

Referencias detectadas:

- **code**: tools/audit_cleanup.py:47
- **docs**: docs/diagrams/deps_cleanup.md:8, docs/CLEANUP_REPORT.md:120, docs/CLEANUP_REPORT.md:562, docs/CLEANUP_REPORT.md:592, docs/CLEANUP_REPORT.md:945, docs/cleanup.json:1529, docs/cleanup.json:1629, docs/cleanup.json:2906
- Notas: No referenciado desde CLI actuales.
- Clasificación: QUARANTINE

### scripts/generate_synthetic_data.py

Referencias detectadas:

- **code**: scripts/run_high_volume_case.py:49, scripts/generate_synthetic_data.py:21, scripts/generate_synthetic_data.py:22, scripts/generate_synthetic_data.py:23, scripts/generate_synthetic_data.py:670, scripts/generate_synthetic_data.py:671, scripts/generate_synthetic_data.py:672, scripts/generate_synthetic_data.py:673
- **docs**: docs/diagrams/deps_cleanup.md:7, docs/cleanup.json:509, docs/cleanup.json:510, docs/cleanup.json:578, docs/cleanup.json:604, docs/cleanup.json:655, docs/cleanup.json:656, docs/cleanup.json:657
- Clasificación: KEEP

### scripts/run_high_volume_case.py

Referencias detectadas:

- **docs**: docs/report.json:161, docs/cleanup.json:23, docs/cleanup.json:24, docs/cleanup.json:25, docs/cleanup.json:26, docs/cleanup.json:506, docs/cleanup.json:654, docs/cleanup.json:2535
- Clasificación: KEEP

### scripts/run_multiformat_case.py

Referencias detectadas:

- **docs**: docs/cleanup.json:27, docs/cleanup.json:28, docs/cleanup.json:29, docs/cleanup.json:30, docs/cleanup.json:511, docs/cleanup.json:549, docs/cleanup.json:2539, docs/cleanup.json:2998
- Clasificación: KEEP

### scripts/runner.sh

Referencias detectadas:

- **configs**: docker-compose.yml:85, docker-compose.yml:86
- **docs**: README.md:15, docs/report.json:136, docs/report.json:154, docs/PROJECT_DOCUMENTATION.md:42, docs/cleanup.json:507, docs/cleanup.json:738, docs/cleanup.json:882, docs/cleanup.json:916
- Clasificación: KEEP

### scripts/runner_with_db.sh

Referencias detectadas:

- **docs**: docs/report.json:132, docs/report.json:149, docs/report.json:166, docs/cleanup.json:508, docs/cleanup.json:739, docs/cleanup.json:883, docs/cleanup.json:917, docs/cleanup.json:1070
- Clasificación: KEEP

### src/datacore/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/cli.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3109, docs/CLEANUP_REPORT.md:127, docs/CLEANUP_REPORT.md:998, docs/CLEANUP_REPORT.md:1234, docs/CLEANUP_REPORT.md:1307, docs/CLEANUP_REPORT.md:1308, docs/CLEANUP_REPORT.md:1309, docs/CLEANUP_REPORT.md:1310
- Clasificación: KEEP

### src/datacore/config/schema.py

Referencias detectadas:

- **docs**: docs/run/configs.md:5, docs/run/configs.md:77, docs/cleanup.json:774, docs/cleanup.json:952, docs/cleanup.json:1105, docs/cleanup.json:1250, docs/cleanup.json:3134, docs/cleanup.json:3589
- Clasificación: KEEP

### src/datacore/context.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3161, docs/CLEANUP_REPORT.md:129, docs/CLEANUP_REPORT.md:1012, docs/CLEANUP_REPORT.md:1241, docs/CLEANUP_REPORT.md:1313, docs/CLEANUP_REPORT.md:1314, docs/CLEANUP_REPORT.md:1315
- Clasificación: KEEP

### src/datacore/io/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/io/adapters.py

Referencias detectadas:

- **docs**: docs/cleanup.json:1362, docs/cleanup.json:3211, docs/cleanup.json:3616, docs/CLEANUP_REPORT.md:131, docs/CLEANUP_REPORT.md:146, docs/CLEANUP_REPORT.md:517, docs/CLEANUP_REPORT.md:1026, docs/CLEANUP_REPORT.md:1131
- Clasificación: KEEP

### src/datacore/io/fs.py

Referencias detectadas:

- **docs**: docs/cleanup.json:1530, docs/cleanup.json:1531, docs/cleanup.json:1532, docs/cleanup.json:1533, docs/cleanup.json:1630, docs/cleanup.json:1631, docs/cleanup.json:1632, docs/cleanup.json:1633
- Clasificación: KEEP

### src/datacore/layers/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/layers/bronze/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/layers/bronze/main.py

Referencias detectadas:

- **docs**: docs/report.json:17, docs/report.json:31, docs/report.json:46, docs/cleanup.json:3319, docs/cleanup.json:3373, docs/cleanup.json:3427, docs/cleanup.json:3481, docs/CLEANUP_REPORT.md:135
- Clasificación: KEEP

### src/datacore/layers/gold/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/layers/gold/main.py

Referencias detectadas:

- **docs**: docs/report.json:17, docs/report.json:31, docs/report.json:46, docs/CLEANUP_REPORT.md:135, docs/CLEANUP_REPORT.md:137, docs/CLEANUP_REPORT.md:139, docs/CLEANUP_REPORT.md:141, docs/CLEANUP_REPORT.md:1054
- Clasificación: KEEP

### src/datacore/layers/raw/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/layers/raw/main.py

Referencias detectadas:

- **docs**: docs/report.json:17, docs/report.json:31, docs/report.json:46, docs/cleanup.json:3319, docs/cleanup.json:3373, docs/cleanup.json:3427, docs/cleanup.json:3481, docs/CLEANUP_REPORT.md:135
- Clasificación: KEEP

### src/datacore/layers/silver/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/layers/silver/main.py

Referencias detectadas:

- **docs**: docs/report.json:17, docs/report.json:31, docs/report.json:46, docs/cleanup.json:3319, docs/cleanup.json:3373, docs/cleanup.json:3427, docs/cleanup.json:3481, docs/CLEANUP_REPORT.md:135
- Clasificación: KEEP

### src/datacore/pipeline/__init__.py

Referencias detectadas:

- **docs**: docs/cleanup.json:993, docs/cleanup.json:1146, docs/cleanup.json:2186, docs/cleanup.json:2240, docs/cleanup.json:2293, docs/cleanup.json:2376, docs/cleanup.json:2566, docs/cleanup.json:2620
- Clasificación: KEEP

### src/datacore/pipeline/utils.py

Referencias detectadas:

- **docs**: docs/report.json:82, docs/report.json:91, docs/report.json:98, docs/report.json:105, docs/report.json:112, docs/cleanup.json:3535, docs/CLEANUP_REPORT.md:143, docs/CLEANUP_REPORT.md:1110
- Clasificación: KEEP

### tests/test_cli_smoke.py

Referencias detectadas:

- **docs**: docs/cleanup.json:258, docs/cleanup.json:259, docs/cleanup.json:288, docs/cleanup.json:289, docs/cleanup.json:318, docs/cleanup.json:319, docs/cleanup.json:348, docs/cleanup.json:349
- Clasificación: KEEP

### tests/test_config_schema.py

Referencias detectadas:

- **docs**: docs/run/configs.md:77, docs/cleanup.json:774, docs/cleanup.json:952, docs/cleanup.json:1105, docs/cleanup.json:1250, docs/cleanup.json:3589, docs/CLEANUP_REPORT.md:145, docs/CLEANUP_REPORT.md:374
- Clasificación: KEEP

### tests/test_io_adapters.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3616, docs/CLEANUP_REPORT.md:146, docs/CLEANUP_REPORT.md:1131, docs/CLEANUP_REPORT.md:1249
- Clasificación: KEEP

### tests/test_io_fs.py

Referencias detectadas:

- **docs**: docs/cleanup.json:1530, docs/cleanup.json:1531, docs/cleanup.json:1532, docs/cleanup.json:1533, docs/cleanup.json:1630, docs/cleanup.json:1631, docs/cleanup.json:1632, docs/cleanup.json:1633
- Clasificación: KEEP

### tests/test_layers_config.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3666, docs/CLEANUP_REPORT.md:148, docs/CLEANUP_REPORT.md:1145
- Clasificación: KEEP

### tests/test_security.py

Referencias detectadas:

- **docs**: docs/cleanup.json:3688, docs/CLEANUP_REPORT.md:149, docs/CLEANUP_REPORT.md:1152
- Clasificación: KEEP

### tools/audit_cleanup.py

Referencias detectadas:

- **docs**: docs/cleanup.json:107, docs/cleanup.json:108, docs/cleanup.json:137, docs/cleanup.json:229, docs/cleanup.json:1663, docs/cleanup.json:1718, docs/cleanup.json:1719, docs/cleanup.json:1775
- Clasificación: KEEP

### tools/list_io.py

Referencias detectadas:

- **docs**: README.md:40, docs/cleanup.json:2160, docs/cleanup.json:3737, docs/CLEANUP_REPORT.md:92, docs/CLEANUP_REPORT.md:151, docs/CLEANUP_REPORT.md:742, docs/CLEANUP_REPORT.md:1166, docs/CLEANUP_REPORT.md:1259
- Clasificación: KEEP

## Impacto estimado

- Archivos candidatos a eliminar inmediatamente: 1
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
pipelines/common.py:28: unused function 'norm_type' (60% confidence)
pipelines/config/loader.py:20: unused function 'load_dataset_config' (60% confidence)
pipelines/config/loader.py:24: unused function 'load_env_config' (60% confidence)
pipelines/config/loader.py:28: unused function 'load_db_config' (60% confidence)
pipelines/database/db_manager.py:14: unused import 'sa' (90% confidence)
pipelines/database/db_manager.py:15: unused import 'Column' (90% confidence)
pipelines/database/db_manager.py:15: unused import 'DateTime' (90% confidence)
pipelines/database/db_manager.py:15: unused import 'Integer' (90% confidence)
pipelines/database/db_manager.py:15: unused import 'String' (90% confidence)
pipelines/database/db_manager.py:15: unused import 'Table' (90% confidence)
pipelines/database/db_manager.py:16: unused import 'SQLAlchemyError' (90% confidence)
pipelines/database/db_manager.py:17: unused import 'Engine' (90% confidence)
pipelines/database/db_manager.py:72: unused variable 'created_at' (60% confidence)
pipelines/database/db_manager.py:87: unused method 'get_connection' (60% confidence)
pipelines/database/db_manager.py:190: unused method 'table_exists' (60% confidence)
pipelines/database/db_manager.py:264: unused variable 'connection_props' (60% confidence)
pipelines/database/db_manager.py:437: unused method 'execute_query' (60% confidence)
pipelines/database/db_manager.py:447: unused method 'close' (60% confidence)
pipelines/database/db_manager.py:568: unused method 'get_latest_dataset_version' (60% confidence)
pipelines/database/db_manager.py:587: unused method 'get_pipeline_execution_status' (60% confidence)
pipelines/database/schema_mapper.py:312: unused method 'generate_alter_table_ddl' (60% confidence)
pipelines/database/schema_mapper.py:363: unused method 'validate_engine_support' (60% confidence)
pipelines/database/schema_mapper.py:377: unused method 'validate_schema' (60% confidence)
pipelines/io/reader.py:38: unused function 'read_table_by_jdbc' (60% confidence)
pipelines/io/s3a.py:6: unused function 'configure_s3a' (60% confidence)
pipelines/spark_job.py:7: unused import 'norm_type' (90% confidence)
pipelines/udf_catalog.py:16: unused import 'udf' (90% confidence)
pipelines/udf_catalog.py:21: unused import 'pa' (90% confidence)
pipelines/udf_catalog.py:22: unused variable 'PANDAS_AVAILABLE' (60% confidence)
pipelines/udf_catalog.py:37: unused function 'normalize_id_py' (60% confidence)
pipelines/udf_catalog.py:42: unused function 'sanitize_string_py' (60% confidence)
pipelines/udf_catalog.py:48: unused function 'standardize_name_py' (60% confidence)
pipelines/udf_catalog.py:56: unused function 'calculate_age_py' (60% confidence)
pipelines/udf_catalog.py:79: unused function 'format_address_py' (60% confidence)
pipelines/utils/parallel.py:4: unused function 'run_in_threads' (60% confidence)
pipelines/utils/parallel.py:21: unused function 'run_map_in_threads' (60% confidence)
scripts/generate_synthetic_data.py:35: unused import 'Decimal' (90% confidence)
scripts/generate_synthetic_data.py:42: unused import 'codecs' (90% confidence)
src/datacore/cli.py:90: unused function 'run_layer' (60% confidence)
src/datacore/config/schema.py:12: unused variable 'model_config' (60% confidence)
src/datacore/config/schema.py:35: unused variable 'standardization' (60% confidence)
src/datacore/config/schema.py:39: unused variable 'references' (60% confidence)
src/datacore/config/schema.py:59: unused variable 'id' (60% confidence)
src/datacore/config/schema.py:60: unused variable 'description' (60% confidence)
src/datacore/config/schema.py:85: unused variable 'storage' (60% confidence)
src/datacore/context.py:171: unused function 'context_paths' (60% confidence)
src/datacore/io/fs.py:76: unused function '_load_env_value' (60% confidence)
src/datacore/pipeline/utils.py:23: unused variable 'kwargs' (100% confidence)
src/datacore/pipeline/utils.py:241: unused function 'run_bronze_stage' (60% confidence)
src/datacore/pipeline/utils.py:308: unused function 'run_silver_stage' (60% confidence)
src/datacore/pipeline/utils.py:501: unused function 'apply_gold_transformations' (60% confidence)
src/datacore/pipeline/utils.py:533: unused function 'write_to_gold_database' (60% confidence)
src/datacore/pipeline/utils.py:580: unused function 'write_to_gold_bucket' (60% confidence)
tests/test_io_adapters.py:8: unused function '_clear_env' (60% confidence)
```

### Hallazgos de Deptry

```
Assuming the corresponding module name of package 'typer' is 'typer'. Install the package or configure a package_module_name_map entry to override this behaviour.
Assuming the corresponding module name of package 'pydantic' is 'pydantic'. Install the package or configure a package_module_name_map entry to override this behaviour.
Scanning 49 files...

[1mdocs/tools/list_io.py[m[36m:[m17[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mpipelines/common.py[m[36m:[m7[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/common.py[m[36m:[m8[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/common.py[m[36m:[m9[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/common.py[m[36m:[m54[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/common.py[m[36m:[m99[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/config/loader.py[m[36m:[m3[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m9[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m14[36m:[m8[36m:[m [1m[31mDEP003[m 'sqlalchemy' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m15[36m:[m1[36m:[m [1m[31mDEP003[m 'sqlalchemy' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m16[36m:[m1[36m:[m [1m[31mDEP003[m 'sqlalchemy' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m17[36m:[m1[36m:[m [1m[31mDEP003[m 'sqlalchemy' imported but it is a transitive dependency
[1mpipelines/database/db_manager.py[m[36m:[m18[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/database/schema_mapper.py[m[36m:[m8[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mpipelines/io/reader.py[m[36m:[m3[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/io/reader.py[m[36m:[m4[36m:[m1[36m:[m [1m[31mDEP003[m 'tenacity' imported but it is a transitive dependency
[1mpipelines/io/reader.py[m[36m:[m6[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/io/s3a.py[m[36m:[m3[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/io/writer.py[m[36m:[m3[36m:[m1[36m:[m [1m[31mDEP003[m 'tenacity' imported but it is a transitive dependency
[1mpipelines/io/writer.py[m[36m:[m5[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/sources.py[m[36m:[m4[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/sources.py[m[36m:[m7[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/sources.py[m[36m:[m145[36m:[m20[36m:[m [1m[31mDEP003[m 'requests' imported but it is a transitive dependency
[1mpipelines/spark_job.py[m[36m:[m1[36m:[m17[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mpipelines/spark_job.py[m[36m:[m3[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/spark_job.py[m[36m:[m4[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/spark_job.py[m[36m:[m7[36m:[m1[36m:[m [1m[31mDEP001[m 'common' imported but missing from the dependency definitions
[1mpipelines/spark_job.py[m[36m:[m8[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/spark_job_with_db.py[m[36m:[m6[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/spark_job_with_db.py[m[36m:[m7[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/spark_job_with_db.py[m[36m:[m8[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/spark_job_with_db.py[m[36m:[m9[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/spark_job_with_db.py[m[36m:[m10[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mpipelines/transforms/apply.py[m[36m:[m2[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/transforms/tests/test_apply.py[m[36m:[m1[36m:[m8[36m:[m [1m[31mDEP003[m 'pytest' imported but it is a transitive dependency
[1mpipelines/transforms/tests/test_apply.py[m[36m:[m2[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/transforms/tests/test_apply.py[m[36m:[m3[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/udf_catalog.py[m[36m:[m16[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/udf_catalog.py[m[36m:[m17[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/udf_catalog.py[m[36m:[m20[36m:[m8[36m:[m [1m[31mDEP003[m 'pandas' imported but it is a transitive dependency
[1mpipelines/udf_catalog.py[m[36m:[m21[36m:[m8[36m:[m [1m[31mDEP003[m 'pyarrow' imported but it is a transitive dependency
[1mpipelines/validation/quality.py[m[36m:[m2[36m:[m1[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1mpipelines/validation/quality.py[m[36m:[m4[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1mscripts/generate_synthetic_data.py[m[36m:[m59[36m:[m5[36m:[m [1m[31mDEP001[m 'faker' imported but missing from the dependency definitions
[1mscripts/generate_synthetic_data.py[m[36m:[m600[36m:[m20[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mscripts/generate_synthetic_data.py[m[36m:[m658[36m:[m20[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1mscripts/run_high_volume_case.py[m[36m:[m134[36m:[m16[36m:[m [1m[31mDEP001[m 'psutil' imported but missing from the dependency definitions
[1mscripts/run_multiformat_case.py[m[36m:[m189[36m:[m16[36m:[m [1m[31mDEP001[m 'psutil' imported but missing from the dependency definitions
[1msrc/datacore/cli.py[m[36m:[m8[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1msrc/datacore/cli.py[m[36m:[m10[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/cli.py[m[36m:[m11[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/cli.py[m[36m:[m12[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/cli.py[m[36m:[m13[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/cli.py[m[36m:[m15[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/context.py[m[36m:[m8[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1msrc/datacore/context.py[m[36m:[m11[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/context.py[m[36m:[m17[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/io/fs.py[m[36m:[m21[36m:[m8[36m:[m [1m[31mDEP001[m 'fsspec' imported but missing from the dependency definitions
[1msrc/datacore/io/fs.py[m[36m:[m24[36m:[m12[36m:[m [1m[31mDEP003[m 'pandas' imported but it is a transitive dependency
[1msrc/datacore/io/fs.py[m[36m:[m29[36m:[m12[36m:[m [1m[31mDEP001[m 'polars' imported but missing from the dependency definitions
[1msrc/datacore/io/fs.py[m[36m:[m36[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/layers/bronze/main.py[m[36m:[m7[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/layers/gold/main.py[m[36m:[m7[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/layers/raw/main.py[m[36m:[m5[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/layers/raw/main.py[m[36m:[m6[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1msrc/datacore/layers/silver/main.py[m[36m:[m7[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/pipeline/utils.py[m[36m:[m8[36m:[m8[36m:[m [1m[31mDEP003[m 'yaml' imported but it is a transitive dependency
[1msrc/datacore/pipeline/utils.py[m[36m:[m11[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/pipeline/utils.py[m[36m:[m12[36m:[m5[36m:[m [1m[31mDEP003[m 'pyspark' imported but it is a transitive dependency
[1msrc/datacore/pipeline/utils.py[m[36m:[m36[36m:[m1[36m:[m [1m[31mDEP001[m 'datacore' imported but missing from the dependency definitions
[1m[31mFound 70 dependency issues.[m

For more information, see the documentation: https://deptry.com/
```

### Links rotos

- docs/run/azure.md → https://learn.microsoft.com/azure/synapse-analytics/spark/spark-job-definitions (HTTP Error 404: Not Found)
- docs/run/azure.md → https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-monitor-application (HTTP Error 404: Not Found)
- docs/run/databricks.md → https://docs.databricks.com/jobs/jobs-parameterization.html (<urlopen error Tunnel connection failed: 403 Forbidden>)
- docs/run/databricks.md → https://docs.databricks.com/security/secrets/index.html (<urlopen error Tunnel connection failed: 403 Forbidden>)
- docs/run/databricks.md → https://docs.databricks.com/workflows/jobs/jobs-notifications.html (<urlopen error Tunnel connection failed: 403 Forbidden>)

### Documentos huérfanos

- Ninguno

### Notebooks huérfanos

- Ninguno