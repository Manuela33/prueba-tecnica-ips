# Supuestos y baseline de datos (Fase 0)

Documento derivado del perfilado en `notebooks/00_exploracion.ipynb` y análisis local sobre los Excel entregados. Sirve como contrato de entrada para Bronze/Silver.

**Fecha de baseline:** 2026-09-18 (Fase 0 — perfilado local + notebook Databricks).  
**Volumen de referencia:** ver conteos abajo.

---

## 1. Contexto de negocio

- La IPS opera con cuatro fuentes operacionales exportadas a Excel (snapshot batch, no streaming).
- El objetivo analítico es unificar **demanda clínica** (citas, eventos) con **ingresos** (facturación) a nivel paciente y cita.
- No se dispone de historial de cambios (CDC); las columnas `creado_en` y `actualizado_en` permiten diseñar **incremental futuro** aunque la carga inicial sea full.

---

## 2. Supuestos generales

| ID | Supuesto | Impacto |
|----|----------|---------|
| S1 | Los `id_*` en cada archivo son claves de negocio estables y únicas en el snapshot | PK en Silver |
| S2 | Una cita pertenece a un solo paciente; un evento pertenece a una cita y replica `id_paciente` coherente con la cita | Validación en Silver |
| S3 | `motivo_cancelacion` solo aplica cuando `estado_cita = Cancelada`; el resto de vacíos son esperables | No imputar en Silver |
| S4 | `valor_resultado` / `unidad_resultado` aplican a subconjuntos de eventos (p. ej. laboratorio, signos); vacío ≠ error | Flag `tiene_resultado` en Silver |
| S5 | Montos en facturación están en la misma moneda; `valor_neto = valor_bruto - valor_descuento` | Regla de calidad |
| S6 | Los Excel son la fuente autoritativa del alcance de la prueba (no hay APIs adicionales) | Ingesta desde Volume |
| S7 | Datos sintéticos/demo: se permite enmascarar PII en Gold y en muestras de notebooks | Cumplimiento / defensa |

---

## 3. Volúmenes y granularidad

| Fuente | Filas | Granularidad | Clave natural |
|--------|------:|--------------|---------------|
| pacientes | 223 | 1 fila = 1 paciente | `id_paciente` |
| citas | 1 001 | 1 fila = 1 cita | `id_cita` |
| eventos_clinicos | 1 602 | 1 fila = 1 evento clínico | `id_evento` |
| facturacion | 1 202 | 1 fila = 1 línea de factura / servicio facturado | `id_factura` |

**Relación aproximada:**

- 218 pacientes distintos con al menos una cita (223 pacientes en maestro → 5 sin citas en el periodo).
- ~801 citas con al menos un evento; ~815 citas con al menos una factura (citas **Atendida**: 815).
- Promedio ~4,6 citas por paciente con actividad.

---

## 4. Calidad de datos observada

### 4.1 Completitud (nulos / vacíos)

| Tabla | Campo | Ausentes | Interpretación |
|-------|-------|----------:|----------------|
| pacientes | `ciudad` | 4 | Imputar `DESCONOCIDO` o null explícito en Silver |
| citas | `id_profesional` | 5 | Opcional operativo; flag `profesional_desconocido` |
| citas | `sede` | 3 | Idem sede |
| citas | `motivo_cancelacion` | 933 | Normal (~93% citas no canceladas) |
| eventos | `valor_resultado` | 801 | ~50% sin resultado numérico |
| eventos | `unidad_resultado` | 804 | Alineado con resultado |
| facturacion | `pagador` | 4 | Tratar como desconocido |

### 4.2 Unicidad (PK)

Duplicados en `id_paciente`, `id_cita`, `id_evento`, `id_factura`: **0** en el snapshot.

### 4.3 Integridad referencial

| Regla | Resultado |
|-------|-----------|
| Citas → Pacientes | 0 huérfanas |
| Eventos → Citas | 0 huérfanos |
| Eventos → Pacientes | 0 huérfanos |
| Facturación → Citas | 0 huérfanas |
| `id_paciente` en evento = paciente de la cita | 0 inconsistencias |

### 4.4 Reglas de negocio

| Regla | Resultado |
|-------|-----------|
| `valor_bruto - valor_descuento = valor_neto` (redondeo 2 decimales) | 0 inconsistencias |

### 4.5 Anomalías de tipo (importante para Silver)

| Tabla | Hallazgo | Acción propuesta |
|-------|----------|------------------|
| pacientes | 1 registro con `fecha_nacimiento` tipo hora (`datetime.time`) vs `datetime` en el resto | Parseo defensivo; cuarentena o corrección manual documentada |
| pacientes | `numero_documento` numérico en Excel | Cast a string en Silver (preservar ceros a la izquierda si aparecieran) |
| Todos | Lectura vía pandas → Spark como string en exploración | Bronze puede guardar string; Silver aplica tipos fuertes |

---

## 5. Dominios de valores (cardinalidad)

### Pacientes

- `sexo`: F (121), M (102)
- `aseguradora`: 6 valores (Nueva EPS, SURA, Compensar, Coomeva, Sanitas, Particular)
- `estado`: Activo (164), Inactivo (59)

### Citas

- `estado_cita`: Atendida (815), Programada (74), Cancelada (68), No asistió (44)
- `tipo_cita`: Consulta, Valoración, Control, Procedimiento, Urgencia (5)
- `especialidad`: 9 valores distintos
- `id_profesional`: 40 distintos
- Rango `fecha_hora_cita`: 2026-01-02 — 2026-07-30

### Eventos clínicos

- `tipo_evento`: Signos vitales, Diagnóstico, Procedimiento, Resultado laboratorio, Medicamento (5)
- `estado_evento`: Finalizado (1454), Pendiente (107), Cancelado (41)
- Con resultado numérico: 801 / 1602

### Facturación

- `estado_pago`: Pagado, Pendiente, Pago parcial, Cancelado (4)
- `tipo_servicio`: 6 categorías
- Rango `fecha_factura`: 2026-01-03 — 2026-08-27

---

## 6. Necesidades analíticas inferidas

1. **Operación:** tasa de cancelación / no asistencia, citas por especialidad y sede, backlog Programada.
2. **Clínico:** eventos por tipo y estado, tiempos cita → evento (con timestamps disponibles).
3. **Financiero:** ingreso neto, cartera (Pendiente / Pago parcial), mix por pagador y tipo de servicio.
4. **360° paciente:** actividad por aseguradora y ciudad (con caveat de nulos en ciudad).

---

## 7. Lo que explícitamente no asumimos (Fase 0)

- Retos avanzados opcionales del enunciado (DELETE simulado, schema drift automatizado, etc.).
- Disponibilidad de datos en tiempo real.
- Maestro de profesionales o catálogo CUPS/CIE aparte de lo embebido en eventos.

---

## 8. Trazabilidad

| Artefacto | Ubicación |
|-----------|-----------|
| Notebook de perfilado | `notebooks/00_exploracion.ipynb` |
| DDL catálogo | `infra/ddl/00_create_catalog_schema.sql` |
| Arquitectura objetivo | `docs/arquitectura.md` |
| Contratos de datos | `docs/data_contracts.md` |
| Calidad | `docs/calidad_datos.md` |
| KPIs | `docs/kpis.md` |

Actualizar este documento si cambian los archivos fuente o los conteos tras una nueva ingesta.
