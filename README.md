# IPS Analytics — Prueba técnica Ingeniero de Datos

Solución analítica end-to-end para integrar **pacientes**, **citas**, **eventos clínicos** y **facturación** de una IPS, usando arquitectura **medallión** (Bronze → Silver → Gold) en **Databricks**, **PySpark/SQL**, **Git** y **Power BI**.

## Estado del proyecto

| Fase | Estado | Entregables |
|------|--------|-------------|
| **0 — Preparación y baseline** | Completada | Repo, DDL UC, exploración, `docs/arquitectura.md`, `docs/supuestos.md` |
| **1 — Diseño detallado** | Completada | `docs/data_contracts.md`, `docs/calidad_datos.md`, `docs/kpis.md`, ingesta §11 arquitectura |
| **2 — Bronze** | Completada (ejecutar en Databricks) | `01_bronze_ingesta.ipynb`, `src/ips_analytics/bronze/` |
| **3 — Silver** | Completada (ejecutar en Databricks) | `02_silver_transform.ipynb`, `04_data_quality.ipynb`, `sql/silver/` |
| **4 — Gold** | Completada (ejecutar en Databricks) | `03_gold_model.ipynb`, `sql/gold/kpi_queries.sql` |
| **5 — Calidad / incremental** | Completada (ejecutar en Databricks) | `99_orchestration.ipynb`, watermarks |
| 6 — Power BI | Completada | Dashboard |


## Requisitos

- Cuenta [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
- Repositorio Git (local o GitHub)
- Power BI Desktop (fases posteriores)
- Archivos fuente: `pacientes.xlsx`, `citas.xlsx`, `eventos_clinicos.xlsx`, `facturacion.xlsx`

## Configuración inicial (Fase 0)

### 1. Clonar o abrir el repositorio

```bash
git clone <url-del-repo>
cd ips-analytics
```

### 2. Crear objetos en Unity Catalog (Databricks)

Ejecutar en un **SQL Warehouse** o notebook SQL (como usuario con permisos de catálogo):

```text
infra/ddl/00_create_catalog_schema.sql
```

Esto crea el catálogo `ips_analytics`, esquemas `bronze`, `silver`, `gold`, `raw`, `ops` y el Volume `ips_analytics.raw.raw_data`.

### 3. Subir archivos Excel al Volume

En el workspace de Databricks: **Catalog** → `ips_analytics` → `raw` → **Volumes** → `raw_data` → Upload.

Alternativa CLI (con Databricks CLI configurado):

```bash
databricks fs cp pacientes.xlsx dbfs:/Volumes/ips_analytics/raw/raw_data/pacientes.xlsx
```

### 4. Exploración de datos

```text
notebooks/00_exploracion.ipynb
```

### 4b. Ingesta Bronze (Fase 2)

Opcional: `infra/ddl/02_bronze_tables.sql`. Luego:

```text
notebooks/01_bronze_ingesta.ipynb
```

Widgets:

| Widget | Valor típico |
|--------|----------------|
| `repo_root` | Ruta del Repo en Databricks (para importar `src/`) |
| `raw_volume_path` | `/Volumes/ips_analytics/raw/raw_data/` |
| `batch_id` | Vacío (auto) o fijo para reproceso |
| `load_mode` | `full` |

### 4c. Transformación Silver (Fase 3)

Tras Bronze:

```text
notebooks/02_silver_transform.ipynb
notebooks/04_data_quality.ipynb
```

Conteos esperados Silver: pacientes **222** (1 reject por fecha), citas **1001**, eventos **1602**, facturación **1202**.

SQL de referencia: `sql/silver/`.

### 4d. Modelo Gold (Fase 4)

Tras Silver:

```text
notebooks/03_gold_model.ipynb
```

Vistas KPI: `gold.v_kpi_resumen_ips`, `v_kpi_operacion_citas`, `v_kpi_facturacion_mensual`.  
Power BI: usar **`gold.dim_paciente_bi`** (sin PII en claro).

**Delta:** el notebook ejecuta `OPTIMIZE ... ZORDER BY` en tablas de hechos (desactivable con widget `optimize_delta=false`).

Checks Gold en `04_data_quality.ipynb` con widget `include_gold=true`.

### 4e. Orquestación y calidad (Fase 5)

Pipeline completo:

```text
notebooks/99_orchestration.ipynb
```

Widgets: `load_mode` (`full` | `incremental`), `run_gold`, `fail_on_quality`.

Documentación incremental: [docs/incrementalidad.md](./docs/incrementalidad.md).

### 5. Tablas operacionales (opcional hasta Fase 5)

```text
infra/ddl/01_ops_tables.sql
```

### 6. Documentación de diseño

- [docs/arquitectura.md](./docs/arquitectura.md) — medallión, **estrategia de ingesta (§11)**, seguridad  
- [docs/supuestos.md](./docs/supuestos.md) — baseline de datos (Fase 0)  
- [docs/data_contracts.md](./docs/data_contracts.md) — contratos Bronze / Silver / Gold (Fase 1)  
- [docs/calidad_datos.md](./docs/calidad_datos.md) — matriz de reglas (Fase 1)  
- [docs/kpis.md](./docs/kpis.md) — KPIs del dashboard (Fase 1)  

## Estructura del repositorio

```text
├── docs/                 # Arquitectura, supuestos (Fase 0+)
├── infra/ddl/            # Scripts SQL Unity Catalog
├── notebooks/            # Pipelines Databricks
├── sql/                  # SQL Silver y Gold de referencia
├── src/                  # Código PySpark (ips_analytics)
└── README.md
```

## Convención Unity Catalog

| Capa | Esquema | Ejemplo |
|------|---------|---------|
| Landing | `ips_analytics.raw` | Volume `raw_data` |
| Bronze | `ips_analytics.bronze` | `pacientes` |
| Silver | `ips_analytics.silver` | `pacientes` |
| Gold | `ips_analytics.gold` | `fact_facturacion` |
| Operaciones | `ips_analytics.ops` | watermarks, logs |
