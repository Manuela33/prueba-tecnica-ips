-- =============================================================================
-- IPS Analytics — Bronze (Fase 2)
-- Nota: el notebook 01_bronze_ingesta crea/actualiza tablas vía saveAsTable.
-- Este script documenta el contrato y permite crear esquemas vacíos opcionales.
-- Prerrequisito: 00_create_catalog_schema.sql
-- =============================================================================

-- Tablas principales: columnas de negocio STRING + metadatos (ver data_contracts.md)

CREATE TABLE IF NOT EXISTS ips_analytics.bronze.pacientes (
  id_paciente      STRING,
  tipo_documento   STRING,
  numero_documento STRING,
  nombres          STRING,
  apellidos        STRING,
  fecha_nacimiento STRING,
  sexo             STRING,
  aseguradora      STRING,
  ciudad           STRING,
  estado           STRING,
  creado_en        STRING,
  actualizado_en   STRING,
  _ingested_at     TIMESTAMP NOT NULL,
  _source_file     STRING NOT NULL,
  _batch_id        STRING NOT NULL,
  _row_hash        STRING
)
USING DELTA
COMMENT 'Bronze pacientes — crudo desde Excel';

CREATE TABLE IF NOT EXISTS ips_analytics.bronze.citas (
  id_cita            STRING,
  id_paciente        STRING,
  id_profesional     STRING,
  especialidad       STRING,
  sede               STRING,
  fecha_hora_cita    STRING,
  tipo_cita          STRING,
  estado_cita        STRING,
  motivo_cancelacion STRING,
  creado_en          STRING,
  actualizado_en     STRING,
  _ingested_at       TIMESTAMP NOT NULL,
  _source_file       STRING NOT NULL,
  _batch_id          STRING NOT NULL,
  _row_hash          STRING
)
USING DELTA
COMMENT 'Bronze citas';

CREATE TABLE IF NOT EXISTS ips_analytics.bronze.eventos_clinicos (
  id_evento          STRING,
  id_cita            STRING,
  id_paciente        STRING,
  tipo_evento        STRING,
  codigo_evento      STRING,
  descripcion_evento STRING,
  fecha_hora_evento  STRING,
  valor_resultado    STRING,
  unidad_resultado   STRING,
  estado_evento      STRING,
  creado_en          STRING,
  actualizado_en     STRING,
  _ingested_at       TIMESTAMP NOT NULL,
  _source_file       STRING NOT NULL,
  _batch_id          STRING NOT NULL,
  _row_hash          STRING
)
USING DELTA
COMMENT 'Bronze eventos clínicos';

CREATE TABLE IF NOT EXISTS ips_analytics.bronze.facturacion (
  id_factura      STRING,
  id_paciente     STRING,
  id_cita         STRING,
  tipo_servicio   STRING,
  pagador         STRING,
  fecha_factura   STRING,
  valor_bruto     STRING,
  valor_descuento STRING,
  valor_neto      STRING,
  estado_pago     STRING,
  creado_en       STRING,
  actualizado_en  STRING,
  _ingested_at    TIMESTAMP NOT NULL,
  _source_file    STRING NOT NULL,
  _batch_id       STRING NOT NULL,
  _row_hash       STRING
)
USING DELTA
COMMENT 'Bronze facturación';

-- bronze.quarantine: creada en el primer append desde 01_bronze_ingesta (mergeSchema).
-- Incluye columnas de negocio + _reject_reason + _entity + _quarantined_at + metadatos Bronze.
