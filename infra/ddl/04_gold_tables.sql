-- =============================================================================
-- IPS Analytics — Gold (Fase 4)
-- Tablas materializadas por notebooks/03_gold_model.ipynb (saveAsTable).
-- Vistas KPI: sql/gold/kpi_queries.sql o create_kpi_views().
-- =============================================================================

-- Dimensiones: dim_tiempo, dim_paciente, dim_especialidad, dim_sede,
--   dim_tipo_servicio, dim_pagador, dim_tipo_evento
-- Hechos: fact_citas, fact_eventos_clinicos, fact_facturacion
-- Vistas BI: v_kpi_operacion_citas, v_kpi_facturacion_mensual, v_kpi_resumen_ips, dim_paciente_bi
