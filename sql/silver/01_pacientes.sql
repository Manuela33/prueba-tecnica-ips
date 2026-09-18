-- Silver pacientes (equivalente SQL del módulo PySpark)
-- Prerrequisito: ips_analytics.bronze.pacientes
-- Nota: filas con fecha_nacimiento no parseable van a silver.rejects vía pipeline Python.

CREATE OR REPLACE TABLE ips_analytics.silver.pacientes_sql_demo AS
SELECT
  TRIM(id_paciente) AS id_paciente,
  TRIM(tipo_documento) AS tipo_documento,
  TRIM(numero_documento) AS numero_documento,
  TRIM(nombres) AS nombres,
  TRIM(apellidos) AS apellidos,
  TO_DATE(TO_TIMESTAMP(fecha_nacimiento)) AS fecha_nacimiento,
  TRIM(sexo) AS sexo,
  TRIM(aseguradora) AS aseguradora,
  COALESCE(NULLIF(TRIM(ciudad), ''), 'DESCONOCIDO') AS ciudad,
  TRIM(estado) AS estado,
  TO_TIMESTAMP(creado_en) AS creado_en,
  TO_TIMESTAMP(actualizado_en) AS actualizado_en,
  _batch_id,
  _source_file,
  _ingested_at,
  CAST(
    FLOOR(DATEDIFF(CURRENT_DATE(), TO_DATE(TO_TIMESTAMP(fecha_nacimiento))) / 365.25) AS INT
  ) AS edad_anios,
  CURRENT_TIMESTAMP() AS _silver_processed_at
FROM ips_analytics.bronze.pacientes
WHERE id_paciente IS NOT NULL
  AND TRIM(id_paciente) NOT IN ('', 'nan', 'None')
  AND TO_DATE(TO_TIMESTAMP(fecha_nacimiento)) IS NOT NULL
  AND TRIM(sexo) IN ('F', 'M');
