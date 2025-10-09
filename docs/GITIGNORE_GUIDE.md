# Guía del .gitignore

## Descripción

Este archivo `.gitignore` está optimizado para el proyecto **MVP Config-Driven** que utiliza:
- **Python** con PySpark
- **Docker** y Docker Compose
- **PostgreSQL** como base de datos
- **MinIO** para almacenamiento de objetos
- **Configuraciones YAML** para datasets y bases de datos

## Estructura del .gitignore

### 1. 🖥️ Archivos del Sistema Operativo
```
.DS_Store, Thumbs.db, Desktop.ini
```
Excluye archivos generados automáticamente por macOS, Windows y Linux.

### 2. 🐍 Python
```
__pycache__/, *.pyc, .pytest_cache/, .coverage
```
Excluye archivos compilados de Python, cache de pytest, reportes de cobertura y entornos virtuales.

### 3. ⚡ PySpark y Big Data
```
spark-warehouse/, metastore_db/, *.parquet, checkpoints/
```
Excluye directorios de Spark, archivos de datos grandes y checkpoints.

### 4. 🐳 Docker
```
docker-compose.override.yml, .docker/
```
Excluye configuraciones locales de Docker y archivos de override.

### 5. 🗄️ Bases de Datos
```
*.sql.backup, *.dump, *.sqlite
```
Excluye backups de bases de datos y archivos de dump.

### 6. 🔐 Configuraciones Sensibles
```
.env, secrets/, *.key, *.pem
```
Excluye variables de entorno, secretos y certificados.

### 7. 💻 IDEs y Editores
```
.vscode/, .idea/, *.sublime-*
```
Excluye configuraciones específicas de editores e IDEs.

### 8. 📦 Archivos de Compilación
```
build/, dist/, target/, *.jar
```
Excluye directorios de build y archivos compilados.

### 9. 📋 Logs y Temporales
```
*.log, *.tmp, logs/, temp/
```
Excluye archivos de log y temporales.

### 10. 📊 Datos y Archivos Grandes
```
data/raw/*, data/processed/*, *.csv, *.json
```
Excluye archivos de datos pero mantiene la estructura de directorios con `.gitkeep`.

### 11. 📚 Dependencias
```
node_modules/, vendor/
```
Excluye directorios de dependencias de diferentes lenguajes.

### 12. 🗂️ Cache y Temporales
```
.cache/, tmp/, temp/
```
Excluye directorios de cache y archivos temporales.

### 13. 🎯 Específicos del Proyecto
```
.mc/, minio-data/, spark-ui/
```
Excluye datos de MinIO, UI de Spark y métricas.

### 14. 📈 Monitoreo
```
prometheus_data/, grafana_data/
```
Excluye datos de herramientas de monitoreo.

### 15. 🔒 Certificados y Seguridad
```
*.crt, *.key, id_rsa, id_ed25519
```
Excluye certificados SSL/TLS y claves SSH.

## Excepciones Importantes

### ✅ Archivos que SÍ se versionan:
- `.env.example` - Plantilla de variables de entorno
- `config/datasets/**/*.yml` - Configuraciones de datasets
- `scripts/**/*.sql` - Scripts SQL de inicialización
- `docs/**/*.md` - Documentación
- `.gitkeep` - Archivos para mantener estructura de directorios

## Uso y Mantenimiento

### Verificar qué archivos están siendo ignorados:
```bash
git status --ignored
```

### Verificar si un archivo específico está siendo ignorado:
```bash
git check-ignore archivo.txt
```

### Forzar agregar un archivo ignorado:
```bash
git add -f archivo.txt
```

### Limpiar archivos ya rastreados que ahora están en .gitignore:
```bash
git rm -r --cached directorio/
git rm --cached archivo.txt
```

## Estructura de Directorios Mantenida

```
data/
├── raw/
│   └── .gitkeep          # Mantiene estructura
├── processed/
│   └── .gitkeep          # Mantiene estructura
└── output/
    └── .gitkeep          # Mantiene estructura
```

## Recomendaciones

1. **Revisar regularmente**: Actualizar el `.gitignore` cuando se agreguen nuevas tecnologías
2. **Archivos sensibles**: Nunca commitear credenciales, usar `.env.example` como plantilla
3. **Datos grandes**: Usar Git LFS para archivos grandes que necesiten versionarse
4. **Configuraciones locales**: Usar archivos `.local.*` para configuraciones específicas del desarrollador

## Comandos Útiles

```bash
# Ver archivos ignorados
git ls-files --others --ignored --exclude-standard

# Limpiar archivos no rastreados (incluyendo ignorados)
git clean -fdX

# Ver el estado incluyendo archivos ignorados
git status --ignored --porcelain
```

## Troubleshooting

### Problema: Archivo aparece en git status aunque esté en .gitignore
**Solución**: El archivo ya estaba siendo rastreado antes de agregarlo al .gitignore
```bash
git rm --cached archivo.txt
```

### Problema: Directorio vacío no aparece en git
**Solución**: Agregar archivo `.gitkeep` en el directorio
```bash
touch directorio/.gitkeep
git add directorio/.gitkeep
```

### Problema: .gitignore no funciona
**Solución**: Verificar que no haya espacios extra y que el archivo esté en la raíz del repositorio
```bash
git check-ignore -v archivo.txt
```