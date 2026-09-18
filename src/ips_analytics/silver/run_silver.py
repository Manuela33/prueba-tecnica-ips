"""Orquestación Silver: Bronze → Silver en orden de dependencias."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from ips_analytics.bronze.ingest_excel import log_pipeline_run
from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig
from ips_analytics.ops.watermark import init_watermarks_if_empty, sync_watermarks_from_silver
from ips_analytics.silver.citas import process_citas
from ips_analytics.silver.common import init_rejects_table
from ips_analytics.silver.eventos import process_eventos
from ips_analytics.silver.facturacion import process_facturacion
from ips_analytics.silver.pacientes import EntitySilverResult, process_pacientes


@dataclass
class SilverRunResult:
    batch_id: str
    entities: list[EntitySilverResult]

    @property
    def rows_in(self) -> int:
        return sum(e.rows_in for e in self.entities)

    @property
    def rows_silver(self) -> int:
        return sum(e.rows_silver for e in self.entities)

    @property
    def rows_rejected(self) -> int:
        return sum(e.rows_rejected for e in self.entities)


def run_silver_pipeline(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
    run_id: str | None = None,
    log_ops: bool = True,
) -> SilverRunResult:
    from uuid import uuid4

    rid = run_id or str(uuid4())
    started_at = datetime.now(timezone.utc)
    status = "success"
    error_message = None
    results: list[EntitySilverResult] = []
    out: SilverRunResult | None = None

    try:
        init_watermarks_if_empty(spark, config)
        init_rejects_table(spark, config)
        results.append(
            process_pacientes(spark, batch_id=batch_id, config=config, load_mode=load_mode)
        )
        results.append(process_citas(spark, batch_id=batch_id, config=config, load_mode=load_mode))
        results.append(
            process_eventos(spark, batch_id=batch_id, config=config, load_mode=load_mode)
        )
        results.append(
            process_facturacion(spark, batch_id=batch_id, config=config, load_mode=load_mode)
        )
        if load_mode == "full":
            sync_watermarks_from_silver(spark, batch_id, config)
        out = SilverRunResult(batch_id=batch_id, entities=results)
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        raise
    finally:
        if log_ops:
            ended_at = datetime.now(timezone.utc)
            rows_in = sum(r.rows_in for r in results)
            rows_out = sum(r.rows_silver for r in results)
            rows_rej = sum(r.rows_rejected for r in results)
            try:
                log_pipeline_run(
                    spark,
                    run_id=rid,
                    batch_id=batch_id,
                    stage="silver",
                    status=status,
                    started_at=started_at,
                    ended_at=ended_at,
                    rows_in=rows_in,
                    rows_out=rows_out,
                    rows_rejected=rows_rej,
                    error_message=error_message,
                    config=config,
                )
            except Exception:
                pass

    assert out is not None
    return out
