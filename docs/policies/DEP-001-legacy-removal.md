# DEP-001: Legacy Asset Retirement Policy

## Objetivo
Garantizar que los artefactos obsoletos del repositorio se aíslen y retiren de forma controlada, preservando trazabilidad y reduciendo el riesgo operativo.

## Alcance
Esta política aplica a cualquier archivo marcado como **QUARANTINE** o **REMOVE NOW** en los reportes de auditoría, incluyendo código, configuraciones, infraestructura, notebooks, documentación y activos de datos de ejemplo.

## Principios
- **Seguridad primero**: los activos productivos o regulados se mantienen como KEEP hasta contar con aprobación explícita del área de plataformas.
- **Trazabilidad completa**: cualquier movimiento a `/legacy/` o `/attic/` debe acompañarse de un README local con fecha, owner y motivo.
- **Reversibilidad**: toda acción de eliminación definitiva requiere verificar que exista un commit de respaldo y que los pipelines críticos pasen.

## Flujo de cuarentena
1. Crear un subdirectorio bajo `/legacy/<dominio>/YYYY-MM-DD-<slug>`.
2. Mover los archivos marcados como QUARANTINE, agregando un README con:
   - Identificador del hallazgo en `cleanup.json`.
   - Owner responsable y canal de contacto.
   - Fecha objetivo de retiro definitivo.
3. Actualizar documentación para señalar la nueva ubicación.
4. Mantener el asset en cuarentena durante **30 días**. Durante este periodo, monitorear alertas en CI/CD o soporte.

## Eliminación definitiva
- Se ejecuta una vez transcurridos 30 días sin incidentes reportados.
- Requiere aprobación del **Tech Lead** y del **Data Governance Officer**.
- Antes de borrar, realizar `git tag legacy-cleanup-<fecha>` apuntando al último commit donde existía el archivo.
- Registrar la acción en el changelog de plataforma con enlace al PR de remoción.

## Excepciones
- Artefactos normativos (políticas, ADRs, diagramas regulatorios) no pueden moverse a `/legacy/` sin aprobación del área legal.
- Datos de ejemplo con restricciones de uso deben anonimizarse antes de la cuarentena.

## Revisión
La política se revisa cada 6 meses o ante eventos significativos (incidentes operativos, auditorías externas).
