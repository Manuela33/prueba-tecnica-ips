-- IPS Analytics — Unity Catalog (Fase 0)


CREATE CATALOG IF NOT EXISTS ips_analytics
COMMENT 'Lakehouse analítico IPS — prueba técnica';

-- Landing / archivos fuente
CREATE SCHEMA IF NOT EXISTS ips_analytics.raw
COMMENT 'Zona de aterrizaje: Volumes y metadatos de fuentes';

CREATE VOLUME IF NOT EXISTS ips_analytics.raw.raw_data
COMMENT 'Excel fuente: pacientes, citas, eventos_clinicos, facturacion';

-- Capas medallión
CREATE SCHEMA IF NOT EXISTS ips_analytics.bronze
COMMENT 'Datos crudos ingeridos + columnas técnicas de trazabilidad';

CREATE SCHEMA IF NOT EXISTS ips_analytics.silver
COMMENT 'Datos limpios, tipados y validados';

CREATE SCHEMA IF NOT EXISTS ips_analytics.gold
COMMENT 'Modelo dimensional y vistas KPI para BI';

-- Metadatos operacionales (watermarks, logs de pipeline — Fase 5+)
CREATE SCHEMA IF NOT EXISTS ips_analytics.ops
COMMENT 'Control de ingestas, calidad y orquestación';


