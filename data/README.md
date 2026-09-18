# Datos fuente — instrucciones

Los archivos Excel **no se versionan en Git** (contienen PII). Mantén copias locales o en el Volume de Databricks.

## Archivos requeridos

| Archivo | Entidad | Registros (baseline) |
|---------|---------|----------------------|
| `pacientes.xlsx` | Pacientes | 223 |
| `citas.xlsx` | Citas | 1 001 |
| `eventos_clinicos.xlsx` | Eventos clínicos | 1 602 |
| `facturacion.xlsx` | Facturación | 1 202 |

## Destino en Databricks

Subir los cuatro archivos a:

```text
/Volumes/ips_analytics/raw/raw_data/
```

Nombres exactos esperados por el notebook de exploración e ingesta:

- `pacientes.xlsx`
- `citas.xlsx`
- `eventos_clinicos.xlsx`
- `facturacion.xlsx`

## Carpeta local `data/local/` (opcional)

Puedes guardar aquí una copia de trabajo en tu máquina. La carpeta está en `.gitignore`.

## Migración desde ruta anterior

Si ya cargaste archivos en:

```text
/Volumes/workspace/ips_db/raw_data/
```

puedes copiarlos al nuevo Volume o cambiar `RAW_VOLUME_PATH` en `notebooks/00_exploracion.ipynb` hasta completar la migración.
