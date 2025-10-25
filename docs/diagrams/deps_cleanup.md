# Impacto de limpieza

| Activo | Ubicación anterior | Ubicación actual | Estado |
| --- | --- | --- | --- |
| Reporte operativo | `docs/REPORT.md` | `legacy/docs/2025-10-25-reports/REPORT.md` | Quarantine |
| Resumen JSON | `docs/report.json` | `legacy/docs/2025-10-25-reports/report.json` | Quarantine |
| Workflow Dataproc | `docs/run/jobs/dataproc_workflow.yaml` | `legacy/infra/2025-10-25-gcp/dataproc_workflow.yaml` | Quarantine |
| Script generación pagos | `scripts/generate_big_payments.py` | `legacy/scripts/2025-10-25-generation/generate_big_payments.py` | Quarantine |
| Config antigua | `.mc/config.json.old` | Eliminado | Remove |

```mermaid
graph TD
    A[Plataforma actual] -->|KEEP| B[src/datacore]
    A -->|KEEP| C[pipelines/]
    A -->|KEEP| D[scripts/generate_synthetic_data.py]
    A -->|QUARANTINE| L1[legacy/docs/2025-10-25-reports/REPORT.md]
    A -->|QUARANTINE| L2[legacy/docs/2025-10-25-reports/report.json]
    A -->|QUARANTINE| L3[legacy/infra/2025-10-25-gcp/dataproc_workflow.yaml]
    A -->|QUARANTINE| L4[legacy/scripts/2025-10-25-generation/generate_big_payments.py]
    A -->|REMOVE| H[.mc/config.json.old]
    C -->|dependencia| I[cfg/]
    C -->|dependencia| J[config/]
```

El diagrama resalta los artefactos trasladados a `legacy/` y la eliminación aplicada. Sigue la política DEP-001 para cuarentena de 30 días con reversibilidad documentada.
