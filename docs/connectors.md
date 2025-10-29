# Conectores disponibles

Los conectores implementan puertos específicos para cada tipo de fuente o destino y se instancian automáticamente desde `datacore.io` según la configuración.

## Storage
- **S3** (`datacore.connectors.storage.s3`)
- **Azure Data Lake Storage Gen2 / ABFS** (`datacore.connectors.storage.abfs`)
- **Google Cloud Storage** (`datacore.connectors.storage.gcs`)
- **Local/MinIO** (`datacore.connectors.storage.local`)

Todos soportan lectura/escritura batch y streaming. La resolución de rutas se delega en la plataforma (`normalize_uri`).

## APIs
- **REST** (`datacore.connectors.api.rest`): soporta autenticación Bearer, paginación y backoff exponencial.
- **GraphQL** (`datacore.connectors.api.graphql`): ejecuta queries parametrizadas con variables dinámicas.

## Bases de datos
- **JDBC** (`datacore.connectors.db.jdbc`): Postgres, SQL Server, MySQL, Redshift, Synapse.
- **NoSQL** (`datacore.connectors.db.nosql`): interfaz genérica con placeholders para DynamoDB/CosmosDB.

Cada conector expone funciones `build_reader_options` y/o `write` para facilitar la integración con Spark.
