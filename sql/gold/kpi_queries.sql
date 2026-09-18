-- =============================================================================
-- IPS Analytics — Vistas KPI Gold (Fase 4)
-- Ejecutar tras 03_gold_model.ipynb o vía create_kpi_views en PySpark.
-- =============================================================================

USE CATALOG ips_analytics;
USE SCHEMA gold;

-- Operación: citas por día × especialidad × sede × estado
CREATE OR REPLACE VIEW v_kpi_operacion_citas AS
SELECT
  fc.fecha_cita,
  dt.anio,
  dt.mes,
  fc.especialidad,
  fc.sede,
  fc.estado_cita,
  COUNT(*) AS total_citas,
  SUM(fc.es_atendida) AS citas_atendidas,
  SUM(fc.es_cancelada) AS citas_canceladas,
  SUM(fc.es_no_asistencia) AS citas_no_asistencia
FROM ips_analytics.gold.fact_citas fc
LEFT JOIN ips_analytics.gold.dim_tiempo dt ON fc.sk_fecha_cita = dt.sk_fecha
GROUP BY fc.fecha_cita, dt.anio, dt.mes, fc.especialidad, fc.sede, fc.estado_cita;

-- Finanzas: agregado mensual
CREATE OR REPLACE VIEW v_kpi_facturacion_mensual AS
SELECT
  dt.anio,
  dt.mes,
  ff.pagador,
  ff.tipo_servicio,
  SUM(ff.valor_bruto) AS total_bruto,
  SUM(ff.valor_descuento) AS total_descuento,
  SUM(ff.valor_neto) AS total_neto,
  SUM(CASE WHEN ff.es_factura_anulada = 0 THEN ff.valor_neto ELSE 0 END) AS ingreso_neto_vigente,
  SUM(ff.monto_cartera) AS cartera_pendiente,
  COUNT(*) AS lineas_factura
FROM ips_analytics.gold.fact_facturacion ff
LEFT JOIN ips_analytics.gold.dim_tiempo dt ON ff.sk_fecha_factura = dt.sk_fecha
GROUP BY dt.anio, dt.mes, ff.pagador, ff.tipo_servicio;

-- Resumen ejecutivo mensual
CREATE OR REPLACE VIEW v_kpi_resumen_ips AS
WITH citas_mes AS (
  SELECT
    dt.anio,
    dt.mes,
    COUNT(DISTINCT fc.id_cita) AS total_citas,
    SUM(fc.es_atendida) AS citas_atendidas,
    SUM(fc.es_cancelada) AS citas_canceladas,
    SUM(fc.es_no_asistencia) AS citas_no_asistencia
  FROM ips_analytics.gold.fact_citas fc
  INNER JOIN ips_analytics.gold.dim_tiempo dt ON fc.sk_fecha_cita = dt.sk_fecha
  GROUP BY dt.anio, dt.mes
),
fin_mes AS (
  SELECT
    dt.anio,
    dt.mes,
    SUM(CASE WHEN ff.es_factura_anulada = 0 THEN ff.valor_neto ELSE 0 END) AS ingreso_neto,
    SUM(ff.monto_cartera) AS cartera_pendiente,
    COUNT(ff.id_factura) AS lineas_factura
  FROM ips_analytics.gold.fact_facturacion ff
  INNER JOIN ips_analytics.gold.dim_tiempo dt ON ff.sk_fecha_factura = dt.sk_fecha
  GROUP BY dt.anio, dt.mes
)
SELECT
  COALESCE(c.anio, f.anio) AS anio,
  COALESCE(c.mes, f.mes) AS mes,
  COALESCE(c.total_citas, 0) AS total_citas,
  COALESCE(c.citas_atendidas, 0) AS citas_atendidas,
  COALESCE(c.citas_canceladas, 0) AS citas_canceladas,
  COALESCE(c.citas_no_asistencia, 0) AS citas_no_asistencia,
  ROUND(
    COALESCE(c.citas_atendidas, 0)
    / NULLIF(COALESCE(c.citas_atendidas, 0) + COALESCE(c.citas_canceladas, 0) + COALESCE(c.citas_no_asistencia, 0), 0),
    4
  ) AS tasa_asistencia,
  ROUND(COALESCE(c.citas_canceladas, 0) / NULLIF(COALESCE(c.total_citas, 0), 0), 4) AS tasa_cancelacion,
  COALESCE(f.ingreso_neto, 0) AS ingreso_neto,
  COALESCE(f.cartera_pendiente, 0) AS cartera_pendiente,
  ROUND(COALESCE(f.ingreso_neto, 0) / NULLIF(COALESCE(f.lineas_factura, 0), 0), 2) AS ticket_promedio
FROM citas_mes c
FULL OUTER JOIN fin_mes f ON c.anio = f.anio AND c.mes = f.mes;

-- Consumo BI sin PII (nombres / documento en claro)
CREATE OR REPLACE VIEW dim_paciente_bi AS
SELECT
  sk_paciente,
  id_paciente,
  tipo_documento,
  numero_documento_hash,
  sexo,
  aseguradora,
  ciudad,
  estado_paciente,
  fecha_nacimiento,
  edad_anios
FROM ips_analytics.gold.dim_paciente;

-- =============================================================================
-- Ejemplos KPI (< 10 líneas cada uno)
-- =============================================================================

-- Ingreso neto por mes (2026)
-- SELECT anio, mes, ingreso_neto FROM v_kpi_resumen_ips WHERE anio = 2026 ORDER BY mes;

-- Top especialidades por volumen de citas
-- SELECT especialidad, SUM(total_citas) AS citas FROM v_kpi_operacion_citas GROUP BY especialidad ORDER BY citas DESC LIMIT 5;

-- Cartera pendiente total
-- SELECT ROUND(SUM(cartera_pendiente), 2) FROM v_kpi_facturacion_mensual;
