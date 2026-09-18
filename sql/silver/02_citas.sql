-- Silver citas (equivalente SQL)
-- Prerrequisito: bronze.citas, silver.pacientes

CREATE OR REPLACE TABLE ips_analytics.silver.citas_sql_demo AS
SELECT
  TRIM(c.id_cita) AS id_cita,
  TRIM(c.id_paciente) AS id_paciente,
  NULLIF(TRIM(c.id_profesional), '') AS id_profesional,
  TRIM(c.especialidad) AS especialidad,
  COALESCE(NULLIF(TRIM(c.sede), ''), 'DESCONOCIDO') AS sede,
  TO_TIMESTAMP(c.fecha_hora_cita) AS fecha_hora_cita,
  TO_DATE(TO_TIMESTAMP(c.fecha_hora_cita)) AS fecha_cita,
  TRIM(c.tipo_cita) AS tipo_cita,
  TRIM(c.estado_cita) AS estado_cita,
  CASE
    WHEN TRIM(c.estado_cita) = 'Cancelada' THEN NULLIF(TRIM(c.motivo_cancelacion), '')
    ELSE NULL
  END AS motivo_cancelacion,
  TO_TIMESTAMP(c.creado_en) AS creado_en,
  TO_TIMESTAMP(c.actualizado_en) AS actualizado_en,
  c._batch_id,
  c._source_file,
  c._ingested_at,
  (NULLIF(TRIM(c.id_profesional), '') IS NULL) AS sin_profesional_asignado,
  (TRIM(c.estado_cita) = 'Cancelada') AS es_cita_cancelada,
  (TRIM(c.estado_cita) = 'No asistió') AS es_no_asistencia,
  (TRIM(c.estado_cita) = 'Atendida') AS es_cita_atendida,
  CURRENT_TIMESTAMP() AS _silver_processed_at
FROM ips_analytics.bronze.citas c
INNER JOIN ips_analytics.silver.pacientes p ON TRIM(c.id_paciente) = p.id_paciente
WHERE TRIM(c.id_cita) IS NOT NULL
  AND TO_TIMESTAMP(c.fecha_hora_cita) IS NOT NULL;
