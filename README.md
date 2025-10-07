# MVP Config Driven Data Pipeline

Este proyecto implementa un **pipeline de datos dinámico y flexible**, basado en **Apache Spark + MinIO + configuración YAML/JSON**, diseñado para adaptarse a distintos entornos (local, Docker, WSL2, CI/CD) y soportar cambios en datasets sin necesidad de modificar código.

---

## Objetivos del proyecto

- Procesar datos en **CSV, JSON y Parquet** de forma **configurable**.  
- Definir **esquemas, estándares y reglas de calidad** en archivos de configuración.  
- Adaptarse a cambios de columnas, tipos de datos y flujos mediante **metadata-driven pipelines**.  
- Estandarizar datasets en capas (`raw → silver`).  
- Ser portable y ejecutable en:
  - **Windows** (con WSL2 + Docker Desktop).
  - **Linux** nativo.
  - **Entornos de CI/CD**.

---

## Arquitectura

```
mvp-config-driven/
├─ config/                   
│   ├─ datasets/
│   │   └─ finanzas/
│   │       └─ payments_v1/
│   │           ├─ schema.json           
│   │           ├─ expectations.yml      
│   │           └─ pipeline.yml          
│   └─ envs/
│       ├─ local.yml
│       ├─ dev.yml
│       └─ prod.yml
├─ pipelines/
│   └─ spark_job.py          
├─ scripts/
│   ├─ run_pipeline.ps1      
│   └─ runner.sh             
├─ ci/                       
│   ├─ check_config.sh
│   ├─ lint.yml
│   ├─ test_dataset.yml
│   └─ README.md
├─ docker-compose.yml        
├─ Makefile                  
└─ README.md                 
```

### Servicios principales

| Servicio       | Rol |
|----------------|---------------------------------------------------|
| Spark Master/Worker | Motor de procesamiento distribuido |
| MinIO          | Almacenamiento S3-compatible para raw/silver/quarantine |
| Runner         | Contenedor que ejecuta los pipelines Spark con configs dinámicas |

---

## Instalación y requisitos

### Prerrequisitos

- **Docker** y **Docker Compose** instalados
- **Make** (opcional, pero recomendado)
- **Git** para clonar el repositorio

### Windows (con WSL2 + Docker Desktop)

1. **Instalar Docker Desktop** y habilitar integración con WSL2
2. **Clonar el repositorio**:  

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
```

3. **En WSL2, instalar dependencias**:  

```bash
sudo apt update && sudo apt install make dos2unix -y
```

4. **Convertir scripts a formato Unix** (solo una vez):  

```bash
dos2unix scripts/*.sh
```

5. **Configurar variables de entorno**:

```bash
cp .env.example .env
# Editar .env si es necesario (valores por defecto funcionan para desarrollo local)
```

### Linux nativo

1. **Instalar dependencias**:

```bash
sudo apt update && sudo apt install docker.io docker-compose make git -y
```

2. **Clonar el repositorio**:

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
```

3. **Configurar variables de entorno**:

```bash
cp .env.example .env
# Editar .env si es necesario
```

### macOS

1. **Instalar Docker Desktop** desde [docker.com](https://www.docker.com/products/docker-desktop)
2. **Instalar Homebrew** (si no está instalado):

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

3. **Instalar dependencias**:

```bash
brew install make git
```

4. **Clonar y configurar**:

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
cp .env.example .env
```

---

## Ejecución del proyecto

### Inicio rápido

#### Opción A: Usando Make (Linux/macOS/WSL2)

1. **Levantar infraestructura** (Spark + MinIO):

```bash
make up
```

Esto iniciará:
- **Spark Master** en `http://localhost:4040`
- **Spark Worker**
- **MinIO** (API en puerto 9000, UI en puerto 9001)

2. **Cargar datos de ejemplo** a MinIO:

```bash
make seed
```

Este comando:
- Crea los buckets `raw` y `silver` en MinIO
- Copia `data/raw/payments/sample.csv` a `s3a://raw/payments/2025/09/26/`

3. **Ejecutar el pipeline**:

```bash
make run
```

Por defecto usa:
- Dataset: `config/datasets/finanzas/payments_v1/dataset.yml`
- Entorno: `config/env.yml`

4. **Ver resultados**:

- **MinIO UI**: [http://localhost:9001](http://localhost:9001)  
  - Usuario: `minio`  
  - Password: `minio12345`
  - Los datos procesados estarán en el bucket `silver/payments_v1/`

- **Spark UI**: [http://localhost:4040](http://localhost:4040) (cuando el job esté ejecutándose)

5. **Apagar servicios**:

```bash
make down
```

### Comandos adicionales

- **Ver logs en tiempo real**:
```bash
make logs
```

- **Ver estado de contenedores**:
```bash
make ps
```

- **Ejecutar con dataset grande**:
```bash
make seed-big  # Carga big_sample.csv
make run-big   # Ejecuta con configuración para dataset grande
```

- **Limpiar todo** (contenedores + volúmenes):
```bash
make clean
```

#### Opción B: Usando PowerShell (Windows)

Si no tienes Make instalado en Windows, puedes usar el script de PowerShell:

1. **Levantar infraestructura**:
```powershell
.\scripts\run_pipeline.ps1 up
```

2. **Cargar datos de ejemplo**:
```powershell
.\scripts\run_pipeline.ps1 seed
```

3. **Ejecutar pipeline**:
```powershell
.\scripts\run_pipeline.ps1 run
```

4. **Ver logs**:
```powershell
.\scripts\run_pipeline.ps1 logs
```

5. **Ver estado**:
```powershell
.\scripts\run_pipeline.ps1 ps
```

6. **Apagar servicios**:
```powershell
.\scripts\run_pipeline.ps1 down
```

7. **Limpiar todo**:
```powershell
.\scripts\run_pipeline.ps1 clean
```

8. **Ver ayuda**:
```powershell
.\scripts\run_pipeline.ps1 help
```

---

## Modificación de un pipeline

### Esquema

En `config/datasets/.../schema.json` se define cada columna con nombre, tipo y si es requerida:

```json
{
  "type": "object",
  "properties": {
    "payment_id": { "type": "string" },
    "amount": { "type": "number" },
    "payment_date": { "type": ["string", "null"], "format": "date-time" },
    "updated_at": { "type": ["string", "null"], "format": "date-time" }
  },
  "required": ["payment_id", "amount"]
}
```

### Estándar de columnas

En `pipeline.yml`:

```yaml
standardization:
  timezone: America/Bogota
  rename:
    - { from: customerId, to: customer_id }
  casts:
    - { column: amount, to: "decimal(18,2)", on_error: null }
    - { column: payment_date, to: "timestamp", format_hint: "yyyy-MM-dd[ HH:mm:ss]" }
  defaults:
    - { column: currency, value: "CLP" }
  deduplicate:
    key: [payment_id]
    order_by: [updated_at desc]
```

### Reglas de calidad

En `expectations.yml` se definen validaciones:

```yaml
expectations:
  - { column: amount, rule: ">= 0", action: quarantine }
  - { column: payment_date, rule: "not null", action: reject }
```

---

## Pruebas y CI/CD

- Validación de configuraciones:

```bash
./ci/check_config.sh
```

- Workflow de GitHub Actions (`ci/lint.yml`) valida que todo YAML/JSON sea correcto antes de hacer merge.  
- `ci/test_dataset.yml` permite correr pruebas con un dataset mínimo.

---

## Troubleshooting

### Problemas comunes

#### 1. Error "No such file or directory" en rutas S3

**Problema**: El pipeline no encuentra archivos en la ruta S3 especificada.

**Solución**:
- Verificar que los datos estén cargados: `make seed`
- Comprobar la fecha en `dataset.yml` coincida con la fecha de seed
- Verificar en MinIO UI que los archivos existan en la ruta correcta

#### 2. Contenedores no inician correctamente

**Problema**: `docker compose up` falla o los contenedores se detienen.

**Solución**:
```bash
# Limpiar todo y reiniciar
make clean
make up

# Ver logs para identificar el problema
make logs
```

#### 3. Error de permisos en Windows/WSL2

**Problema**: Scripts no ejecutan por permisos o formato de línea.

**Solución**:
```bash
# Convertir formato de archivos
dos2unix scripts/*.sh

# Dar permisos de ejecución
chmod +x scripts/*.sh
```

#### 4. Puerto ocupado

**Problema**: Error "port already in use" al iniciar servicios.

**Solución**:
```bash
# Verificar qué proceso usa el puerto
netstat -tulpn | grep :9000

# Cambiar puertos en .env si es necesario
MINIO_API_PORT=9010
MINIO_CONSOLE_PORT=9011
```

#### 5. Memoria insuficiente para Spark

**Problema**: Jobs de Spark fallan por falta de memoria.

**Solución**:
- Aumentar memoria disponible para Docker Desktop
- Reducir `SPARK_WORKER_MEMORY` en docker-compose.yml
- Usar datasets más pequeños para pruebas

### Verificación del entorno

Para verificar que todo funciona correctamente:

```bash
# 1. Verificar servicios
make ps

# 2. Verificar conectividad a MinIO
curl http://localhost:9000/minio/health/live

# 3. Ejecutar pipeline de prueba
make seed && make run

# 4. Verificar resultados en MinIO UI
# Ir a http://localhost:9001 y revisar bucket 'silver'
```

---

## Buenas prácticas

- No usar rutas locales. Siempre referenciar `s3a://raw/...` y configurar en `envs/*.yml`.  
- Mantener actualizado `schema.json` al cambiar columnas.  
- Agregar reglas de calidad en `expectations.yml` para prevenir datos incorrectos en silver.  
- Versionar datasets mediante carpetas (`payments_v1`, `payments_v2`, etc.).  

---

## Estado actual

- [x] Ingesta dinámica de CSV/JSON/Parquet  
- [x] Estandarización configurable (renames, casts, defaults, deduplicación)  
- [x] Enriquecimiento automático (timestamp, run_id, particiones por fecha)  
- [x] CI/CD con validación de configs  
- [ ] Futuro: capa `gold` y orquestación con Airflow/n8n  

---

## Contribuciones

1. Crear una nueva rama:

```bash
git checkout -b feature/nueva-funcionalidad
```

2. Hacer cambios en configuración o código.  
3. Validar con `make run` y `./ci/check_config.sh`.  
4. Crear Pull Request (se ejecutan validaciones automáticas).
