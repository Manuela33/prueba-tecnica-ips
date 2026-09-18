# KPIs y métricas — Dashboard Power BI (Fase 1)

Definiciones de negocio para la **Fase 6 (Power BI)**. Todas las métricas se calculan preferentemente desde **Gold** (`fact_*`, vistas `v_kpi_*`) para consistencia.

Audiencia: dirección IPS, operaciones clínicas, facturación/cartera.

---

## 1. Página: Resumen ejecutivo

| KPI | Definición de negocio | Fórmula (SQL lógica) | Grain | Fuente Gold | Utilidad |
|-----|----------------------|----------------------|-------|-------------|----------|
| **Total pacientes activos** | Pacientes con `estado_paciente = Activo` en maestro | `COUNT(DISTINCT id_paciente)` filtro Activo | Snapshot | `dim_paciente` | Capacidad de cartera afiliada |
| **Total citas (periodo)** | Citas programadas en el rango de fechas del filtro | `COUNT(id_cita)` | Día/mes (filtro) | `fact_citas` | Volumen demanda |
| **Tasa de asistencia** | Proporción de citas atendidas sobre citas cerradas (excl. solo programadas futuras) | `Atendidas / (Atendidas + Canceladas + No asistió)` | Mes | `fact_citas` | Eficiencia operativa y pérdida de capacidad |
| **Tasa de cancelación** | Citas canceladas / total citas | `SUM(es_cancelada) / COUNT(*)` | Mes, sede | `fact_citas` | Gestión de agenda |
| **Tasa de no asistencia** | Citas no asistió / total | `SUM(es_no_asistencia) / COUNT(*)` | Mes, especialidad | `fact_citas` | Riesgo operativo y re-programación |
| **Ingreso neto (periodo)** | Suma facturación neta no anulada | `SUM(valor_neto) WHERE NOT es_factura_anulada` | Mes | `fact_facturacion` | Resultado financiero |
| **Ticket promedio** | Ingreso neto / líneas de factura | `SUM(valor_neto) / COUNT(id_factura)` | Mes | `fact_facturacion` | Mix de servicios |
| **Cartera pendiente** | Monto neto en estados Pendiente o Pago parcial | `SUM(monto_cartera)` | Snapshot / mes | `fact_facturacion` | Flujo de caja esperado |

---

## 2. Página: Operación — Citas

| KPI | Definición | Fórmula | Dimensión principal | Utilidad |
|-----|------------|---------|---------------------|----------|
| **Citas por estado** | Distribución Atendida, Programada, Cancelada, No asistió | `COUNT` por `estado_cita` | Estado, mes | Balance de agenda |
| **Citas por especialidad** | Volumen por especialidad médica | `COUNT` por `especialidad` | Especialidad | Planeación de capacidad |
| **Citas por sede** | Volumen por sede | `COUNT` por `sede` | Sede | Desempeño por ubicación |
| **Citas por tipo** | Consulta, Control, Urgencia, etc. | `COUNT` por `tipo_cita` | Tipo | Perfil de demanda |
| **Ratio citas atendidas / programadas** | Conversión a atención efectiva | Atendidas / total citas | Mes | KPI operativo clave |
| **Pacientes con cita (únicos)** | Pacientes distintos con al menos una cita en periodo | `COUNT(DISTINCT id_paciente)` | Mes | Alcance poblacional |
| **Promedio citas por paciente** | Intensidad de uso | `COUNT citas / COUNT DISTINCT pacientes` | Mes | Frecuencia de contacto |

---

## 3. Página: Clínico — Eventos

| KPI | Definición | Fórmula | Dimensión | Utilidad |
|-----|------------|---------|-----------|----------|
| **Eventos clínicos totales** | Registros clínicos generados | `COUNT(id_evento)` | Mes | Actividad asistencial |
| **Eventos por tipo** | Signos vitales, Diagnóstico, Procedimiento, etc. | `COUNT` por `tipo_evento` | Tipo | Mix clínico |
| **Eventos finalizados vs pendientes** | Cierre de actividades clínicas | `% Finalizado` | Estado | Backlog clínico |
| **Citas con al menos un evento** | Cobertura documentación clínica | `COUNT DISTINCT id_cita` en hechos eventos / citas atendidas | Mes | Calidad de historia clínica |
| **Eventos con resultado numérico** | Proporción con dato medible | `SUM(tiene_resultado) / COUNT(*)` | Tipo evento | Completitud resultados (labs/signos) |
| **Eventos por cita (promedio)** | Intensidad documentación | `COUNT eventos / COUNT DISTINCT id_cita` | Mes | Complejidad por visita |

---

## 4. Página: Finanzas — Facturación

| KPI | Definición | Fórmula | Dimensión | Utilidad |
|-----|------------|---------|-----------|----------|
| **Facturación bruta** | Antes de descuentos | `SUM(valor_bruto)` | Mes, pagador | Gross revenue |
| **Descuentos totales** | Rebates/convenios | `SUM(valor_descuento)` | Mes, aseguradora | Impacto convenios |
| **Facturación neta** | Ingreso reconocido | `SUM(valor_neto)` | Mes | P&L operativo |
| **% descuento sobre bruto** | Intensidad de descuento | `SUM(desc) / SUM(bruto)` | Pagador | Negociación EPS |
| **Facturas por estado de pago** | Pagado, Pendiente, Pago parcial, Cancelado | `COUNT` por `estado_pago` | Estado | Salud de cartera |
| **Ingreso por tipo de servicio** | Mix consulta, procedimiento, imagenología… | `SUM(valor_neto)` por `tipo_servicio` | Tipo servicio | Estrategia servicios |
| **Ingreso por pagador** | EPS vs particular | `SUM(valor_neto)` por `pagador` | Pagador | Concentración de riesgo |
| **Facturación por cita atendida** | Yield por visita atendida | `SUM(neto)` join citas Atendida / COUNT citas atendidas | Mes | Productividad económica |

---

## 5. Página: Pacientes y cobertura

| KPI | Definición | Fórmula | Dimensión | Utilidad |
|-----|------------|---------|-----------|----------|
| **Pacientes por aseguradora** | Distribución afiliación | `COUNT` pacientes | Aseguradora | Perfil aseguramiento |
| **Pacientes por ciudad** | Distribución geográfica | `COUNT` (ciudad ≠ DESCONOCIDO) | Ciudad | Expansión territorial |
| **Pacientes sin citas en periodo** | Inactivos operativamente | Maestro minus pacientes con cita | — | Campañas retención |
| **Edad promedio (atendidos)** | Edad de pacientes con cita | `AVG(edad_anios)` | Especialidad | Perfil demográfico |

---

## 6. KPIs de calidad de datos (opcional en dashboard ops)

| KPI | Definición | Fuente |
|-----|------------|--------|
| **Filas rechazadas Silver** | Total rejects en corrida | `ops.data_quality_results` |
| **Checks CRIT fallidos** | Conteo reglas rojas | ops |
| **Última ingesta exitosa** | Max `_ingested_at` | ops.pipeline_runs |

Útil para defensa técnica; puede ser página oculta o reporte separado.

---

## 7. Filtros globales recomendados (Power BI)

- Rango de fechas (calendario sobre `dim_tiempo`)
- Sede, especialidad, aseguradora, pagador
- Estado cita / estado pago (slicers)

---

## 8. Mapeo vista Gold → KPI

| Vista / tabla | KPIs principales |
|---------------|------------------|
| `gold.v_kpi_resumen_ips` | Ingreso neto, citas, tasas agregadas mensuales |
| `gold.v_kpi_operacion_citas` | Estados, especialidad, sede, no asistencia |
| `gold.v_kpi_facturacion_mensual` | Bruto, descuento, neto, cartera, pagador |
| `fact_citas`, `fact_eventos_clinicos`, `fact_facturacion` | Drill-through detalle |

---

## 9. Priorización MVP (mínimo para entrega)

Para la prueba técnica, implementar al menos **8 visuales** respaldando estos KPIs:

1. Ingreso neto por mes (línea o columnas)  
2. Citas por estado (donut o barras)  
3. Tasa cancelación + no asistencia (tarjetas)  
4. Top 5 especialidades por volumen  
5. Eventos por tipo  
6. Facturación por pagador  
7. Cartera pendiente (tarjeta)  
8. Ticket promedio (tarjeta)  

---

## 10. Trazabilidad

| Versión | Fecha | Cambio |
|---------|-------|--------|
| 1.0 | 2026-09-18 | Definiciones Fase 1 |
