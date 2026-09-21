-- IPS Analytics — Silver (Fase 3)
-- Las tablas productivas se materializan con saveAsTable desde PySpark.

-- Rejects unificados (append por corrida desde pipeline Python)
-- CREATE via init_rejects_table / mergeSchema en append

-- Referencia de tablas productivas:
-- ips_analytics.silver.pacientes
-- ips_analytics.silver.citas
-- ips_analytics.silver.eventos_clinicos
-- ips_analytics.silver.facturacion
-- ips_analytics.silver.rejects
