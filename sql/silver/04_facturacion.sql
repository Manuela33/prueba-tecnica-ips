-- Silver facturación (equivalente SQL)

CREATE OR REPLACE TABLE ips_analytics.silver.facturacion_sql_demo AS
SELECT
  TRIM(f.id_factura) AS id_factura,
  TRIM(f.id_paciente) AS id_paciente,
  TRIM(f.id_cita) AS id_cita,
  TRIM(f.tipo_servicio) AS tipo_servicio,
  COALESCE(NULLIF(TRIM(f.pagador), ''), 'DESCONOCIDO') AS pagador,
  TO_TIMESTAMP(f.fecha_factura) AS fecha_factura,
  TO_DATE(TO_TIMESTAMP(f.fecha_factura)) AS fecha_factura_date,
  CAST(f.valor_bruto AS DECIMAL(18, 2)) AS valor_bruto,
  CAST(f.valor_descuento AS DECIMAL(18, 2)) AS valor_descuento,
  CAST(f.valor_neto AS DECIMAL(18, 2)) AS valor_neto,
  TRIM(f.estado_pago) AS estado_pago,
  TO_TIMESTAMP(f.creado_en) AS creado_en,
  TO_TIMESTAMP(f.actualizado_en) AS actualizado_en,
  f._batch_id,
  f._source_file,
  f._ingested_at,
  (TRIM(f.estado_pago) = 'Cancelado') AS es_factura_anulada,
  CASE
    WHEN TRIM(f.estado_pago) IN ('Pendiente', 'Pago parcial') THEN CAST(f.valor_neto AS DECIMAL(18, 2))
    ELSE CAST(0 AS DECIMAL(18, 2))
  END AS monto_cartera,
  CURRENT_TIMESTAMP() AS _silver_processed_at
FROM ips_analytics.bronze.facturacion f
INNER JOIN ips_analytics.silver.pacientes p ON TRIM(f.id_paciente) = p.id_paciente
INNER JOIN ips_analytics.silver.citas c ON TRIM(f.id_cita) = c.id_cita
WHERE TRIM(f.id_factura) IS NOT NULL
  AND TO_TIMESTAMP(f.fecha_factura) IS NOT NULL
  AND CAST(f.valor_bruto AS DECIMAL(18, 2)) >= 0
  AND CAST(f.valor_descuento AS DECIMAL(18, 2)) >= 0
  AND CAST(f.valor_neto AS DECIMAL(18, 2)) >= 0
  AND ROUND(CAST(f.valor_bruto AS DECIMAL(18, 2)) - CAST(f.valor_descuento AS DECIMAL(18, 2)), 2)
      = ROUND(CAST(f.valor_neto AS DECIMAL(18, 2)), 2);
