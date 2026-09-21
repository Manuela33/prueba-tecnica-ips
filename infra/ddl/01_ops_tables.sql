-- IPS Analytics — Tablas operacionales (Fase 1 diseño / Fase 5 implementación)


CREATE TABLE IF NOT EXISTS ips_analytics.ops.ingestion_watermark (
  source_table     STRING    NOT NULL COMMENT 'Ej: pacientes, citas, eventos_clinicos, facturacion',
  watermark_column STRING    NOT NULL DEFAULT 'actualizado_en',
  last_watermark   TIMESTAMP COMMENT 'Último valor procesado exitosamente en Silver',
  last_batch_id    STRING,
  updated_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
COMMENT 'Control de ingestas incrementales por entidad';

CREATE TABLE IF NOT EXISTS ips_analytics.ops.pipeline_runs (
  run_id           STRING    NOT NULL,
  batch_id         STRING    NOT NULL,
  stage            STRING    NOT NULL COMMENT 'bronze | silver | gold | quality',
  status           STRING    NOT NULL COMMENT 'success | failed | running',
  started_at       TIMESTAMP NOT NULL,
  ended_at         TIMESTAMP,
  rows_in          BIGINT,
  rows_out         BIGINT,
  rows_rejected    BIGINT,
  error_message    STRING
)
USING DELTA
COMMENT 'Log de ejecuciones del pipeline';

CREATE TABLE IF NOT EXISTS ips_analytics.ops.data_quality_results (
  check_id         STRING    NOT NULL COMMENT 'Referencia docs/calidad_datos.md',
  batch_id         STRING    NOT NULL,
  table_name       STRING    NOT NULL,
  passed           BOOLEAN   NOT NULL,
  metric_value     DOUBLE,
  threshold        DOUBLE,
  evaluated_at     TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Resultados de checks de calidad por corrida';
