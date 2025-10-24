# Flujo de ejecución de capas y orquestadores

```mermaid
flowchart LR
    subgraph CLI "CLI & Jobs"
        prodi([prodi run-layer])
        sparkjob([pipelines/spark_job.py])
        sparkjobdb([pipelines/spark_job_with_db.py])
    end

    subgraph Layers "Capas Datacore"
        raw((Raw))
        bronze((Bronze))
        silver((Silver))
        gold((Gold))
    end

    subgraph Outputs "Salidas"
        bronzeStore[[Bronze storage (parquet)]]
        silverStore[[Silver storage (config path)]]
        goldDb[[Gold DB via db_manager]]
        goldBucket[[Gold bucket (fsspec/s3a)]]
    end

    prodi --> raw
    prodi --> bronze
    prodi --> silver
    prodi --> gold

    sparkjob --> raw
    sparkjob --> bronze
    sparkjob --> silver
    sparkjob --> gold

    sparkjobdb --> raw
    sparkjobdb --> bronze
    sparkjobdb --> silver
    sparkjobdb --> gold

    raw --> bronze
    bronze --> silver
    silver --> gold

    bronze --> bronzeStore
    silver --> silverStore
    gold --> goldDb
    gold --> goldBucket
```

> El flujo muestra que tanto la CLI moderna como los scripts heredados disparan la cadena completa de capas, manteniendo dependencias verticales fuertes.
