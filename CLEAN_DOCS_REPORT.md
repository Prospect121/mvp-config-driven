# CLEAN_DOCS_REPORT

| Archivo | Acción | Justificación |
| ------- | ------ | ------------- |
| `docs/run/databricks.md` | Eliminado | Referenciaba wheel tasks heredados, rutas `dbfs:/cfg/...` sin `dataset_config`/`environment_config` obligatorios y asumía empaquetado monolítico que ya no coincide con `LayerConfig.from_dict`. |
| `docs/run/azure.md` | Eliminado | Documentaba pipelines Synapse/ADF sobre wheels y rutas `abfss://` genéricas sin el contrato de `/dbfs` + ABFSS detallado en las nuevas plantillas; inducía a reutilizar scripts del monolito removido. |
| `docs/run/configs.md` | Eliminado | Describía un esquema legacy basado en `io.source.type`/`io.sink.type` y `options.path`, contradiciendo la normalización actual con `dataset_config`/`environment_config` y `uris.abfss`. |
| `.docker/*` | Sin hallazgos | Se verificó que no existen artefactos `.docker` o manifest de Docker en el repositorio. |

Los archivos retirados se reemplazan por `templates/azure/dbfs/` y
`docs/azure-databricks.md`, que describen el flujo soportado por la CLI `prodi`.
