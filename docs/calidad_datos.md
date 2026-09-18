# Matriz de calidad de datos — IPS Analytics (Fase 1)

Reglas aplicables por capa. **Severidad:** `CRIT` (bloquea publicación Gold), `WARN` (registra y continúa), `INFO` (monitoreo).  
Implementación automatizada: Fase 5 (`notebooks/04_data_quality`, `src/ips_analytics/quality/`).

Referencia de columnas: [data_contracts.md](./data_contracts.md).

---

## 1. Resumen por capa

| Capa | Objetivo de calidad | Mecanismo |
|------|---------------------|-----------|
| **Bronze** | Completitud de ingesta, trazabilidad | Conteo filas vs Excel, metadatos obligatorios |
| **Silver** | Validez, unicidad, integridad, reglas negocio | Checks PySpark + tabla rejects |
| **Gold** | Consistencia dimensional, KPIs reproducibles | Conteos reconcile, FK SK |
| **ops** | Auditoría | Persistir resultados en `ops.data_quality_results` |

---

## 2. Matriz de reglas

| ID | Regla | Tabla(s) | Capa | Severidad | Umbral / criterio | Acción si falla |
|----|-------|----------|------|-----------|-------------------|-----------------|
| Q-B01 | Filas ingeridas = filas Excel | bronze.* | Bronze | CRIT | delta = 0 | Abortar pipeline; revisar lectura |
| Q-B02 | `_batch_id`, `_source_file`, `_ingested_at` no nulos | bronze.* | Bronze | CRIT | 100% | Abortar |
| Q-B03 | PK presente (no vacío) | bronze.* | Bronze | CRIT | 0 vacíos | Mover fila a `bronze_quarantine_*` |
| Q-S01 | PK única | silver.* | Silver | CRIT | duplicados = 0 | Deduplicar por PK quedándose con `actualizado_en` max; log WARN |
| Q-S02 | `id_paciente` FK citas → pacientes | silver.citas | Silver | CRIT | huérfanas = 0 | Reject a `silver_rejects_citas` |
| Q-S03 | `id_cita` FK eventos → citas | silver.eventos_clinicos | Silver | CRIT | huérfanas = 0 | Reject |
| Q-S04 | `id_paciente` FK eventos → pacientes | silver.eventos_clinicos | Silver | CRIT | huérfanas = 0 | Reject |
| Q-S05 | Coherencia paciente evento vs cita | silver.eventos_clinicos | Silver | CRIT | inconsistentes = 0 | Reject |
| Q-S06 | `id_cita` FK facturación → citas | silver.facturacion | Silver | CRIT | huérfanas = 0 | Reject |
| Q-S07 | `id_paciente` FK facturación → pacientes | silver.facturacion | Silver | CRIT | huérfanas = 0 | Reject |
| Q-S08 | Ecuación financiera | silver.facturacion | Silver | CRIT | `\|bruto - desc - neto\| > 0.01` = 0 filas | Reject |
| Q-S09 | Montos no negativos | silver.facturacion | Silver | CRIT | bruto, desc, neto ≥ 0 | Reject |
| Q-S10 | `fecha_nacimiento` parseable | silver.pacientes | Silver | CRIT | rejects documentados | Cuarentena (baseline: ≤1 fila anómala) |
| Q-S11 | Dominio `sexo` | silver.pacientes | Silver | WARN | ⊆ {F, M} | Normalizar o reject |
| Q-S12 | Dominio `estado_cita` | silver.citas | Silver | WARN | 4 valores conocidos | Mapear `OTRO` |
| Q-S13 | `motivo_cancelacion` si Cancelada | silver.citas | Silver | WARN | canceladas sin motivo = 0 ideal | Flag `motivo_faltante` |
| Q-S14 | Completitud `ciudad` | silver.pacientes | Silver | INFO | ~98% | Imputar DESCONOCIDO (S4 supuestos) |
| Q-S15 | `tiene_resultado` vs nulls | silver.eventos_clinicos | Silver | INFO | ~50% con valor | Solo monitoreo |
| Q-G01 | Conteo silver.citas = fact_citas | gold.fact_citas | Gold | CRIT | delta = 0 | Abortar Gold refresh |
| Q-G02 | Suma `valor_neto` silver = gold fact | gold.fact_facturacion | Gold | CRIT | delta < 0.01 COP | Investigar |
| Q-G03 | SK paciente resuelto | gold.fact_* | Gold | CRIT | null SK = 0 | Abortar |
| Q-G04 | Sin PII en `dim_paciente_bi` | gold | Gold | CRIT | columnas nombres/doc ausentes | Corregir vista |

---

## 3. Reglas de negocio detalladas

### 3.1 Facturación (Q-S08)

```
round(valor_bruto - valor_descuento, 2) = round(valor_neto, 2)
```

Tolerancia: **0.01** por redondeo float Excel.

### 3.2 Cancelaciones (Q-S13)

Si `estado_cita = 'Cancelada'` entonces se espera `motivo_cancelacion IS NOT NULL`.  
En baseline hay 68 canceladas con motivo — verificar post-limpieza.

### 3.3 Incremental (diseño, check ops)

| ID | Regla | Severidad |
|----|-------|-----------|
| Q-O01 | `last_watermark` monótono | WARN |
| Q-O02 | `actualizado_en >= creado_en` | WARN |
| Q-O03 | Batch idempotente: re-run mismo `_batch_id` no duplica Silver | CRIT |

---

## 4. Tablas de rechazo / cuarentena

| Tabla | Contenido |
|-------|-----------|
| `ips_analytics.bronze.quarantine_pacientes` (u homóloga) | Filas ilegibles en ingesta |
| `ips_analytics.silver.rejects_citas` | FK u otras reglas CRIT |
| `ips_analytics.silver.rejects_eventos_clinicos` | FK, coherencia paciente-cita |
| `ips_analytics.silver.rejects_facturacion` | Finanzas, FK |
| `ips_analytics.silver.rejects` ( `_entity` ) | PK, FK, fecha inválida, reglas financieras |

Columnas mínimas rejects: todas las de negocio + `_reject_reason` + `_batch_id` + `_rejected_at`.

---

## 5. SLAs de calidad (MVP prueba técnica)

| Métrica | Objetivo |
|---------|----------|
| % filas Silver vs Bronze (por tabla) | ≥ 99% (baseline esperado 100%) |
| Reglas CRIT en verde antes de Gold | 100% |
| Tiempo detección fallo | Misma corrida pipeline |
| Documentación de rejects | CSV/log en notebook o tabla ops |

---

## 6. Responsabilidades

| Rol | Acción |
|-----|--------|
| Ingeniero de datos | Implementar checks, revisar rejects |
| Analista / negocio | Validar KPIs vs definiciones en [kpis.md](./kpis.md) |
| Defensa técnica | Mostrar matriz + evidencia `00_exploracion` y Fase 5 |

---

## 7. Trazabilidad

| Versión | Fecha | Cambio |
|---------|-------|--------|
| 1.0 | 2026-09-18 | Matriz inicial Fase 1 |
