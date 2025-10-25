# Cleanup Report – Purga Final 2025-10-25

Se ejecutó la purga definitiva de artefactos legacy. No existen rutas `legacy`, pipelines heredados ni Docker en el repositorio.

## Resumen

- Monolito pipelines eliminado.
- Artefactos Docker eliminados (archivos de build, manifests compose, `docker/`, `.docker/`).
- Scripts auxiliares Docker (`scripts/runner*.sh`, `scripts/run_*_case.py`) eliminados.
- Reportes y workflows cuarentenados migrados previamente se eliminaron junto con la carpeta legacy.

## Guardas activas

- `python tools/audit_cleanup.py --check` falla si reaparecen rutas legacy, pipelines heredados o Docker.
- `python tools/list_io.py --json` sólo inventaría `src/` y `scripts/`.
- `python tools/check_cross_layer.py` y `pytest` permanecen en CI.

## Próximos pasos

- Mantener documentación y guías (`docs/run/*`, `README.md`) alineadas con la operación por capas.
- Cualquier solicitud de reinstaurar artefactos legacy debe abrirse como propuesta formal y agregar nuevos guardas antes de fusionar.
