-- Silver eventos clínicos (equivalente SQL)

CREATE OR REPLACE TABLE ips_analytics.silver.eventos_clinicos_sql_demo AS
SELECT
  TRIM(e.id_evento) AS id_evento,
  TRIM(e.id_cita) AS id_cita,
  TRIM(e.id_paciente) AS id_paciente,
  TRIM(e.tipo_evento) AS tipo_evento,
  TRIM(e.codigo_evento) AS codigo_evento,
  TRIM(e.descripcion_evento) AS descripcion_evento,
  TO_TIMESTAMP(e.fecha_hora_evento) AS fecha_hora_evento,
  CAST(NULLIF(TRIM(e.valor_resultado), '') AS DECIMAL(18, 4)) AS valor_resultado,
  NULLIF(TRIM(e.unidad_resultado), '') AS unidad_resultado,
  TRIM(e.estado_evento) AS estado_evento,
  TO_TIMESTAMP(e.creado_en) AS creado_en,
  TO_TIMESTAMP(e.actualizado_en) AS actualizado_en,
  e._batch_id,
  e._source_file,
  e._ingested_at,
  (CAST(NULLIF(TRIM(e.valor_resultado), '') AS DECIMAL(18, 4)) IS NOT NULL) AS tiene_resultado,
  (TRIM(e.id_paciente) = TRIM(c.id_paciente)) AS paciente_coherente_con_cita,
  CURRENT_TIMESTAMP() AS _silver_processed_at
FROM ips_analytics.bronze.eventos_clinicos e
INNER JOIN ips_analytics.silver.citas c ON TRIM(e.id_cita) = c.id_cita
INNER JOIN ips_analytics.silver.pacientes p ON TRIM(e.id_paciente) = p.id_paciente
WHERE TRIM(e.id_evento) IS NOT NULL
  AND TO_TIMESTAMP(e.fecha_hora_evento) IS NOT NULL
  AND TRIM(e.id_paciente) = TRIM(c.id_paciente);
