# .github/copilot-instructions.md
Responde en español. Mantén compatibilidad hacia atrás: no cambies semántica ni nombres de outputs sin prueba y migrador.
Arquitectura: jobs cortos por capa (raw, bronze, silver, gold), cada uno con su YAML. Core en librería (wheel) con CLI `prodi`.
I/O con fsspec (s3://, abfss://, gs://). Spark como motor; fallback Polars/Pandas solo en tests/local.
No notebooks “gordos”: los notebooks solo demuestran, la lógica vive en la librería.
Seguridad: secretos por variables/identidad; nada hardcodeado.
Si hay dudas, propone PRs pequeños: PR1 scaffold, PR2 CLI, PR3 separar capas, PR4 fsspec, PR5 observabilidad/DQ, PR6 integraciones Databricks/Glue/Dataproc/Synapse.
