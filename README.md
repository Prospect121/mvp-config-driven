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

### Windows (con WSL2 + Docker Desktop)

1. Instalar **Docker Desktop** y habilitar integración con WSL2.  
2. Clonar el repositorio:  

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
```

3. En WSL2, instalar dependencias:  

```bash
sudo apt update && sudo apt install make dos2unix -y
```

4. Convertir scripts a formato Unix (solo una vez):  

```bash
dos2unix scripts/*.sh
```

### Linux nativo

```bash
git clone https://github.com/mi-org/mvp-config-driven.git
cd mvp-config-driven
sudo apt update && sudo apt install docker.io docker-compose make -y
```

---

## Ejecución del proyecto

1. Levantar infraestructura:

```bash
make up
```

2. Ejecutar un pipeline:

```bash
make run
```

Por defecto se usa `config/datasets/finanzas/payments_v1/pipeline.yml` con `config/envs/local.yml`.

3. Ver resultados:

- MinIO UI: [http://localhost:9001](http://localhost:9001)  
  Usuario: `minio`  
  Password: `minio12345`

- Datasets procesados terminan en `s3a://silver/payments_v1/`

4. Apagar servicios:

```bash
make down
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
