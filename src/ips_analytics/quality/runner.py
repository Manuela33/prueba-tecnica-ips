"""Ejecutor unificado de calidad por corrida."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name
from ips_analytics.quality.checks import (
    QualityCheckResult,
    log_quality_results,
    run_gold_quality_checks,
    run_silver_quality_checks,
)


@dataclass
class QualityRunReport:
    batch_id: str
    checks: list[QualityCheckResult]

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_ids(self) -> list[str]:
        return [c.check_id for c in self.checks if not c.passed]

    def summary_lines(self) -> list[str]:
        lines = []
        for c in self.checks:
            tag = "OK" if c.passed else "FAIL"
            lines.append(f"[{tag}] {c.check_id}: {c.detail} (metric={c.metric_value})")
        return lines


def run_bronze_quality_checks(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> list[QualityCheckResult]:
    results: list[QualityCheckResult] = []
    for table in ("pacientes", "citas", "eventos_clinicos", "facturacion"):
        try:
            df = spark.table(full_table_name(config, "bronze", table))
        except Exception:
            results.append(
                QualityCheckResult(f"Q-B00-{table}", False, 0.0, f"Tabla bronze.{table} no existe")
            )
            continue
        null_meta = df.filter(
            F.col("_batch_id").isNull()
            | F.col("_source_file").isNull()
            | F.col("_ingested_at").isNull()
        ).count()
        results.append(
            QualityCheckResult(
                f"Q-B02-{table}",
                null_meta == 0,
                float(null_meta),
                f"Metadatos nulos en bronze.{table}",
            )
        )
    return results


def run_pipeline_quality_checks(
    spark: SparkSession,
    *,
    batch_id: str,
    include_bronze: bool = True,
    include_silver: bool = True,
    include_gold: bool = True,
    config: LakehouseConfig = DEFAULT_CONFIG,
    persist_ops: bool = True,
) -> QualityRunReport:
    checks: list[QualityCheckResult] = []
    if include_bronze:
        checks.extend(run_bronze_quality_checks(spark, config))
    if include_silver:
        checks.extend(run_silver_quality_checks(spark, config))
    if include_gold:
        checks.extend(run_gold_quality_checks(spark, config))

    report = QualityRunReport(batch_id=batch_id, checks=checks)
    if persist_ops and checks:
        log_quality_results(spark, batch_id, checks, config)
    return report


def assert_quality_passed(report: QualityRunReport) -> None:
    if not report.passed:
        raise RuntimeError(f"Controles de calidad fallidos: {report.failed_ids}")
