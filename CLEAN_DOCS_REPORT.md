# CLEAN_DOCS_REPORT

| Archivo | Acción | Justificación |
| ------- | ------ | ------------- |
| `docs/run/databricks.md` | Eliminado | Referenciaba wheel tasks heredados, rutas `dbfs:/cfg/...` sin `dataset_config`/`environment_config` obligatorios y asumía empaquetado monolítico que ya no coincide con `LayerConfig.from_dict`. |
| `docs/run/azure.md` | Eliminado | Documentaba pipelines Synapse/ADF sobre wheels y rutas `abfss://` genéricas sin el contrato de `/dbfs` + ABFSS detallado en las nuevas plantillas; inducía a reutilizar scripts del monolito removido. |
| `docs/run/configs.md` | Eliminado | Describía un esquema legacy basado en `io.source.type`/`io.sink.type` y `options.path`, contradiciendo la normalización actual con `dataset_config`/`environment_config` y `uris.abfss`. |
| `docs/PROJECT_DOCUMENTATION.md` | Movido a `docs/legacy/PROJECT_DOCUMENTATION.md` | Explicaba ejecuciones con `cfg/<layer>/example.yml` y salidas en `data/` locales, lo que contradice el contrato actual basado en `/dbfs/configs/...` y URIs `abfss://`. |
| `docs/STABILIZATION_REPORT.md` | Movido a `docs/legacy/STABILIZATION_REPORT.md` | Referenciaba datasets locales (`config/datasets/examples/...`) y rutas `data/raw/...` como camino productivo; se conserva como histórico bajo `docs/legacy/`. |
| `.docker/*` | Sin hallazgos | Se verificó que no existen artefactos `.docker` o manifest de Docker en el repositorio. |

Los archivos retirados o movidos se sustituyen por `templates/azure/dbfs/`, `docs/azure-databricks.md` y el README actualizado, que documentan el flujo soportado por la CLI `prodi`.
