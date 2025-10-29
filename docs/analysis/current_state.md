# Estado actual del repositorio

## Estructura de carpetas y módulos
El repositorio original `mvp-config-driven` no contenía código fuente activo; únicamente existía el directorio `.git`. Esto evidencia que el MVP previo fue desmontado o nunca llegó a versionarse en este repositorio público.

## Dependencias de plataforma
No se identificaron dependencias específicas de Azure, AWS o GCP. La ausencia de código implica que no hay integraciones ni librerías acopladas a un proveedor cloud en el estado inicial.

## Configuración no portable
No se hallaron archivos de configuración (YAML/JSON) que limiten la portabilidad. La falta de configuraciones refuerza la necesidad de diseñar una solución multinube desde cero.

## Mapa de dependencias
Sin código ni manifiestos de paquetes, el repositorio no declaraba dependencias. El nuevo proyecto debe especificar explícitamente bibliotecas para Spark, validaciones y utilidades CLI.

## Bloqueos de portabilidad detectados
- Inexistencia de estructura de proyecto que soporte múltiples nubes.
- Ausencia total de abstracciones para plataformas, conectores o capas de datos.
- Falta de esquemas de configuración y validaciones.

## Acciones de remediación prioritarias
1. **Definir una arquitectura modular** basada en puertos y adaptadores que permita intercambiar proveedores cloud.
2. **Implementar un paquete Python portable** (`datacore`) con CLI, configuraciones y motor de ejecución por capas (raw/bronze/silver/gold).
3. **Estandarizar la configuración** mediante esquemas JSON y archivos YAML por entorno/plataforma.
4. **Crear conectores agnósticos** para storage, APIs y bases de datos con soporte para S3, ABFS, GCS y JDBC.
5. **Documentar y automatizar despliegues** para Azure Databricks, AWS Glue y GCP Dataproc.
6. **Añadir pruebas y CI** que garanticen la portabilidad y calidad de la nueva base de código.
