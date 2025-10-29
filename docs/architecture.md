# Arquitectura Datacore

La solución sigue el patrón hexagonal (puertos y adaptadores) para mantener el núcleo desacoplado de las integraciones con cada proveedor cloud.

```mermaid
graph TD
    CLI[CLI prodi] --> Engine
    Config[Config YAML + JSON Schema] --> Engine
    Engine[Core Engine\n(raw/bronze/silver/gold)] --> Validation
    Engine --> Incremental
    Engine --> Transforms
    Engine --> Readers
    Engine --> Writers
    Readers --> StorageConnectors
    Readers --> APIConnectors
    Readers --> DBConnectors
    Writers --> StorageConnectors
    Writers --> DWConnectors
    StorageConnectors --> Platforms
    APIConnectors --> Platforms
    DBConnectors --> Platforms
    Platforms --> SparkSession
```

## Componentes principales
- **CLI (`datacore.cli`)**: expone comandos `prodi validate|run|plan`.
- **Core (`datacore.core`)**: motor de ejecución por capas, registro de transformaciones y validaciones.
- **Connectores (`datacore.connectors`)**: adaptadores para almacenamiento, APIs y bases de datos.
- **Plataformas (`datacore.platforms`)**: inicialización de Spark y servicios auxiliares según nube.
- **IO (`datacore.io`)**: lectura/escritura batch y streaming.

## Flujo general
1. La CLI carga un archivo YAML y lo valida contra los esquemas JSON.
2. El motor determina las capas a ejecutar y delega en los lectores/escritores adecuados.
3. Los conectores traducen los parámetros a llamadas de Spark o APIs externas.
4. Las reglas de validación e incremental se aplican de forma uniforme sin importar la nube.
