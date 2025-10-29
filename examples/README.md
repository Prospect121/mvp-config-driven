# Ejemplos por plataforma

Cada carpeta contiene pipelines de referencia raw→gold para Azure, AWS y GCP.

## Datos
- `data/customers.csv`: dataset base utilizado en los ejemplos dev.

## Ejecución local (Spark standalone)
```bash
prodi run --layer raw --config configs/envs/dev/layers/raw.yml --platform local
prodi run --layer bronze --config configs/envs/dev/layers/bronze.yml --platform local
prodi run --layer silver --config configs/envs/dev/layers/silver.yml --platform local
prodi run --layer gold --config configs/envs/dev/layers/gold.yml --platform local
```
