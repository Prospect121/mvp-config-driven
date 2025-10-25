# DEP-001: Política de retiro definitivo de artefactos legacy

## Objetivo
Eliminar de forma inmediata los artefactos obsoletos del repositorio evitando zonas de cuarentena o directorios `legacy`.

## Alcance
Aplica a cualquier activo marcado como obsoleto por `tools/audit_cleanup.py`, reportes de seguridad o revisiones de arquitectura: código, configuraciones, notebooks, documentación y assets auxiliares (Docker, scripts).

## Principios
- **Cero cuarentenas**: no se permiten rutas `legacy` ni `attic`. Los artefactos obsoletos se eliminan en el mismo PR.
- **Auditoría automática**: `tools/audit_cleanup.py --check` falla si reaparece un activo retirado.
- **Transparencia**: la documentación (`README.md`, `docs/CLEANUP_REPORT.md`) debe registrar cada purga relevante.

## Flujo de eliminación
1. Abrir PR describiendo el activo y la justificación de retiro.
2. Ejecutar `python tools/audit_cleanup.py --check` y adjuntar evidencia antes y después de la purga.
3. Actualizar documentación y CI para bloquear la reintroducción (tests, scripts, workflows).
4. Fusionar tras aprobación del Tech Lead correspondiente.

## Guardas complementarias
- `tools/list_io.py --json` verifica que no existan runners heredados.
- CI ejecuta scripts de seguridad (`tools/check_cross_layer.py`, tests de TLS/secrets) en cada PR.
- `docs/cleanup.json` y `docs/CLEANUP_REPORT.md` resumen el estado post-purga.

## Revisión
La política se revisa trimestralmente o cuando el owner de plataforma requiera eliminar nuevas familias de artefactos.
