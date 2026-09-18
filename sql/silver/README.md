# SQL Silver

Scripts **equivalentes en SQL** a las transformaciones PySpark (Fase 3.4).

| Script | Tabla destino demo | Tabla productiva (pipeline) |
|--------|-------------------|----------------------------|
| `01_pacientes.sql` | `silver.pacientes_sql_demo` | `silver.pacientes` |
| `02_citas.sql` | `silver.citas_sql_demo` | `silver.citas` |
| `03_eventos_clinicos.sql` | `silver.eventos_clinicos_sql_demo` | `silver.eventos_clinicos` |
| `04_facturacion.sql` | `silver.facturacion_sql_demo` | `silver.facturacion` |

El pipeline oficial es **`notebooks/02_silver_transform.ipynb`** (PySpark), que además escribe **`silver.rejects`**.

Ejecutar en orden 01 → 04 tras Bronze y Silver PySpark, solo para comparar/demostrar SQL.
