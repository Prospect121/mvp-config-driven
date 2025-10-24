# Flujo actual del pipeline monolítico

```mermaid
flowchart LR
  cfg[dataset.yml] --> job[pipelines.spark_job_with_db]
  env[config/env.yml] --> job
  job --> sources_loader[pipelines.sources.load_sources_or_source]
  sources_loader --> standardize[Standardization + dedupe]
  standardize --> transforms[SQL/UDF transforms]
  transforms --> quality_checks[pipelines.validation.quality.apply_quality]
  quality_checks --> silver_write[Silver write (spark.write.save s3a://)]
  quality_checks --> quarantine[Quarantine parquet s3a://]
  silver_write --> gold_branch{Gold enabled?}
  gold_branch -->|Yes| gold_db[DatabaseManager.write_dataframe]
  gold_branch -->|Bucket| gold_bucket[write_to_gold_bucket]
  gold_branch -->|No| finish[[Pipeline completed]]
```
