"""Configuración central del lakehouse IPS Analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "ips_analytics"
    raw_volume_subpath: str = "/Volumes/ips_analytics/raw/raw_data/"

    @property
    def bronze_schema(self) -> str:
        return f"{self.catalog}.bronze"

    @property
    def ops_schema(self) -> str:
        return f"{self.catalog}.ops"


DEFAULT_CONFIG = LakehouseConfig()

# Ejecución local (PySpark sin Unity Catalog): tablas `bronze.pacientes`, etc.
LOCAL_SPARK_CONFIG = LakehouseConfig(catalog="__local__")

# Entidades Excel → tabla Bronze (columnas de negocio en data_contracts.md)
BRONZE_SOURCES: tuple[dict[str, str], ...] = (
    {
        "entity": "pacientes",
        "file_name": "pacientes.xlsx",
        "table": "pacientes",
        "pk": "id_paciente",
    },
    {
        "entity": "citas",
        "file_name": "citas.xlsx",
        "table": "citas",
        "pk": "id_cita",
    },
    {
        "entity": "eventos_clinicos",
        "file_name": "eventos_clinicos.xlsx",
        "table": "eventos_clinicos",
        "pk": "id_evento",
    },
    {
        "entity": "facturacion",
        "file_name": "facturacion.xlsx",
        "table": "facturacion",
        "pk": "id_factura",
    },
)


def generate_batch_id() -> str:
    """Identificador de corrida: timestamp UTC + sufijo único."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{uuid4().hex[:8]}"


def full_table_name(config: LakehouseConfig, layer: str, table: str) -> str:
    if config.catalog == "__local__":
        return f"{layer}.{table}"
    return f"{config.catalog}.{layer}.{table}"
