# Flujo de ejecución de capas y orquestadores

```mermaid
flowchart LR
    subgraph CLI "CLI & Jobs"
        prodi([prodi run-layer])
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

    raw --> bronze
    bronze --> silver
    silver --> gold

    bronze --> bronzeStore
    silver --> silverStore
    gold --> goldDb
    gold --> goldBucket
```

> Las ejecuciones se disparan exclusivamente vía `prodi run-layer`, lo que simplifica la observabilidad y evita monolitos heredados.
