# Incrementalidad — IPS Analytics (Fase 5)

Patrón **watermark + MERGE** para fuentes batch (Excel) con columna `actualizado_en`.

## Componentes

| Componente | Ubicación |
|------------|-----------|
| Watermarks | `ips_analytics.ops.ingestion_watermark` |
| API | `src/ips_analytics/ops/watermark.py` |
| Bronze | `load_mode=incremental` → filtra Excel → MERGE en Delta |
| Silver | Filtra Bronze → transform → MERGE por PK |
| Gold | Tras incremental Silver, ejecutar **full rebuild** Gold (MVP) |

## Flujo `load_mode=full`

1. Bronze: overwrite tablas + `_batch_id`.
2. Silver: overwrite + `sync_watermarks_from_silver()` (MAX `actualizado_en`).
3. Gold: overwrite hechos y dimensiones.
4. Calidad: `run_pipeline_quality_checks()`.

## Flujo `load_mode=incremental`

1. Leer `last_watermark` por entidad (`get_watermark`).
2. Bronze: filas con `to_timestamp(actualizado_en) > watermark` → **MERGE** por PK.
3. Silver: mismo filtro sobre Bronze → **MERGE** por PK; si hay filas, `advance_watermark`.
4. Gold: notebook/orquestador reconstruye Gold completo desde Silver (dataset pequeño).
5. Si **0 filas nuevas** en todas las entidades: pipeline termina sin error (demostración con Excel estático).

## Defensa (Excel estático)

- Tras la primera carga full, watermarks quedan en el máximo `actualizado_en`.
- Una corrida incremental sin cambios en Excel procesa **0 filas** — comportamiento esperado.
- Para simular cambios: actualizar filas en Excel con `actualizado_en` mayor al watermark y re-ejecutar.

## Idempotencia

- Re-ejecutar incremental con los mismos datos no duplica filas (MERGE por PK).
- Si falla Silver antes de `advance_watermark`, la siguiente corrida reintenta las mismas filas.

## Orquestación

Notebook **`99_orchestration.ipynb`** o:

```python
from ips_analytics.pipeline.orchestrate import run_end_to_end_pipeline

run_end_to_end_pipeline(
    spark,
    raw_volume_path="/Volumes/ips_analytics/raw/raw_data/",
    load_mode="incremental",  # o "full"
)
```

## Checks operacionales (Q-O*)

| ID | Regla |
|----|-------|
| Q-O01 | Watermark no retrocede (`advance_watermark` monótono) |
| Q-O02 | Incremental con Excel sin cambios → 0 filas Bronze/Silver (INFO en logs) |

Ver también [arquitectura.md](./arquitectura.md) §11.4–11.5.
