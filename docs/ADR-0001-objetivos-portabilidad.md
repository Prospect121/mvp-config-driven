<!--- ADR: Architectural Decision Record -->
# ADR-0001 — Objetivos de portabilidad y corte por capas

Fecha: 2025-10-24

Estado: propuesta

Contexto
--------
El repositorio actual implementa un flujo único que procesa todas las capas (raw→bronze→silver→gold) en un único job. La configuración centralizada está en YAML e incluye conectividad a datalake (S3/ADLS/GCS), base de datos, validaciones y reglas de transformación.

Objetivo
--------
Hacer el proyecto agnóstico de nube y orquestable por capas, con una librería reutilizable (wheel) que ejecuto con un CLI `prodi` y tareas cortas por capa. Mantener compatibilidad hacia atrás con las transformaciones y validaciones existentes.

Requisitos irrenunciables
------------------------
- Portabilidad total entre `s3://`, `abfss://` y `gs://` únicamente cambiando URIs/credenciales.
- Capas separadas: raw, bronze, silver, gold; cada una como job corto reintentable.
- I/O unificado vía `fsspec` (s3fs, adlfs, gcsfs).
- Motor por defecto: Spark; fallback a Polars/Pandas para desarrollo/tests.
- Seguridad: no hardcodear secretos; usar variables/identidad administrada.

Mapa del repo actual (puntos de acoplamiento a nube y riesgos)
-----------------------------------------------------------
- `pipelines/`, `io/`, `transforms/`: contienen lógica de ETL y helpers. Riesgo: llamadas directas a SDKs cloud (boto3, azure.storage) — buscar y migrar a adaptadores fsspec.
- `scripts/` y `docker/`: pueden contener runbooks/imagen optimizada con bindings cloud.
- `config/*.yml` y `config/datasets/*`: contienen convención central de YAML. Riesgo: secretos embebidos.
- Dependencias de runtime: `pyspark` y posibles SDKs cloud en `requirements.txt`.

Plan de corte por capas (mínimo viable)
------------------------------------
Fase 1 (PR1): Auditoría y diseño (este PR)
- Documentar puntos de acoplamiento, riesgos y plan de migración.
- Añadir contrato DataFrame-in/DataFrame-out (`docs/interfaces.md`).

Fase 2 (PR2): Scaffold de librería y CLI
- Crear paquete `datacore` con `pyproject.toml` y CLI `prodi`.

Fase 3 (PR3): Corte por capas
- Extraer la lógica a `layers/<raw|bronze|silver|gold>/main.py` y plantillas `cfg/<capa>/*.yml`.

Fase 4 (PR4): fsspec y SparkSessionFactory
- Sustituir accesos directos a S3/ADLS/GCS por `fsspec`.

Fase 5 (PR5): Observabilidad y DQ
- Añadir logging estructurado y contador de registros; hook de DQ simple.

Fase 6 (PR6): Integraciones de ejecución
- Documentación y plantillas para Databricks, AWS, GCP, Azure.

Riesgos y mitigaciones
----------------------
- Riesgo: romper transformaciones existentes. Mitigación: mantener adaptadores y compatibilidad, tests snapshot por capa.
- Riesgo: secretos en YAML. Mitigación: buscar y reemplazar; documentar migración a variables de entorno.
- Riesgo: dependencias propietarias acopladas al core. Mitigación: mover a adaptadores en `datacore/io` o `datacore/catalog`.

Criterios de aceptación (PR1)
---------------------------
- Documentos añadidos: `docs/ADR-0001-objetivos-portabilidad.md`, `docs/interfaces.md`.
- Lista de issues a crear para seguimiento (en `docs/PR1_issues.md`).

Aceptado por: pipeline-owner (pendiente revisión)
