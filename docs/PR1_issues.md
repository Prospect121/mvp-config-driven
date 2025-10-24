# PR1 — Issues y checklist de auditoría

Crear los siguientes issues en el tracker para convertirlos en tareas pequeñas y asignables:

1) Buscar y reemplazar SDKs cloud acoplados
- Objetivo: localizar usos de `boto3`, `azure.*`, `google.cloud.*` y cualquier acceso directo a servicios cloud dentro del core.
- Resultado esperado: lista de archivos con llamadas directas y propuesta de adaptador en `datacore/io`.

2) Revisar `config/*.yml` y migración de secretos
- Objetivo: detectar claves/secretos en YAML. Si existen, documentar migración a variables de entorno (ej.: `S3_ACCESS_KEY` / `S3_SECRET_KEY`) o a identidad administrada.

3) Extraer transformaciones de `pipelines/` a `layers/` (plan)
- Objetivo: mapear funciones de `pipelines/` que representan las transformaciones por capa y planear su extracción con adaptadores.

4) Snapshot tests iniciales
- Objetivo: seleccionar datasets pequeños de `data/` como baseline para cada capa y preparar test harness (pytest + fixtures) para snapshots.

5) Preparar `pyproject.toml` y packaging mínimo
- Objetivo: generar `pyproject.toml` compatible con Python 3.10–3.12 y add `prodi` entrypoint.

6) Definir matrix de CI para fsspec mocks
- Objetivo: diseñar pruebas que mockeen `fsspec` para rutas `s3://`, `abfss://` y `gs://`.

Checklist de entrega PR1
- [ ] Añadir ADR (`docs/ADR-0001-objetivos-portabilidad.md`) ✓
- [ ] Añadir `docs/interfaces.md` ✓
- [ ] Añadir `docs/PR1_issues.md` ✓
- [ ] Crear issues en tracker (manual) — listar aquí los números cuando estén creados.

Notas operacionales
- No tocar código en PR1. PR1 es auditoría y diseño: artefactos de documentación y una lista clara de issues para seguir con PR2+.
