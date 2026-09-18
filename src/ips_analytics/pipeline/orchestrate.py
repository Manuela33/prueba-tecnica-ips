"""Orquestación end-to-end: Bronze → Silver → Gold → Calidad."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession

from ips_analytics.bronze.ingest_excel import log_pipeline_run, run_bronze_ingestion
from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, generate_batch_id
from ips_analytics.gold.build_star_schema import run_gold_pipeline
from ips_analytics.quality.runner import QualityRunReport, assert_quality_passed, run_pipeline_quality_checks
from ips_analytics.silver.run_silver import run_silver_pipeline


@dataclass
class PipelineRunResult:
    batch_id: str
    load_mode: str
    bronze_ok: bool
    silver_ok: bool
    gold_ok: bool
    quality: QualityRunReport | None


def run_end_to_end_pipeline(
    spark: SparkSession,
    *,
    raw_volume_path: str,
    batch_id: str | None = None,
    load_mode: str = "full",
    run_gold: bool = True,
    run_quality: bool = True,
    fail_on_quality: bool = True,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> PipelineRunResult:
    bid = batch_id or generate_batch_id()
    pipeline_run_id = str(uuid4())
    started = datetime.now(timezone.utc)
    bronze_ok = silver_ok = gold_ok = False
    quality_report: QualityRunReport | None = None
    error: str | None = None
    status = "failed"

    try:
        run_bronze_ingestion(
            spark,
            raw_volume_path=raw_volume_path,
            batch_id=bid,
            config=config,
            load_mode=load_mode,
        )
        bronze_ok = True

        run_silver_pipeline(
            spark,
            batch_id=bid,
            config=config,
            load_mode=load_mode,
            run_id=str(uuid4()),
        )
        silver_ok = True

        if run_gold:
            run_gold_pipeline(
                spark,
                batch_id=bid,
                config=config,
                run_id=str(uuid4()),
                optimize=True,
            )
            gold_ok = True

        if run_quality:
            quality_report = run_pipeline_quality_checks(
                spark,
                batch_id=bid,
                include_bronze=True,
                include_silver=True,
                include_gold=run_gold,
                config=config,
            )
            if fail_on_quality:
                assert_quality_passed(quality_report)

        status = "success"
    except Exception as exc:
        status = "failed"
        error = str(exc)
        raise
    finally:
        try:
            log_pipeline_run(
                spark,
                run_id=pipeline_run_id,
                batch_id=bid,
                stage="pipeline",
                status=status,
                started_at=started,
                ended_at=datetime.now(timezone.utc),
                rows_in=None,
                rows_out=None,
                rows_rejected=len(quality_report.failed_ids) if quality_report else None,
                error_message=error,
                config=config,
            )
        except Exception:
            pass

    return PipelineRunResult(
        batch_id=bid,
        load_mode=load_mode,
        bronze_ok=bronze_ok,
        silver_ok=silver_ok,
        gold_ok=gold_ok,
        quality=quality_report,
    )
