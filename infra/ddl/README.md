# DDL — Unity Catalog

| Script | Cuándo ejecutar |
|--------|-----------------|
| `00_create_catalog_schema.sql` | **Fase 0** — una vez por workspace |
| `01_ops_tables.sql` | **Fase 1** — tablas ops (watermark, runs, quality) |
| `02_bronze_tables.sql` | Fase 2 — tablas Delta Bronze |
| `03_silver_tables.sql` | Fase 3 — referencia Silver / rejects |
| `04_gold_tables.sql` | Fase 4 — referencia Gold / vistas KPI |

Orden: siempre numeración ascendente. Los scripts 01–03 se añadirán en fases posteriores.
