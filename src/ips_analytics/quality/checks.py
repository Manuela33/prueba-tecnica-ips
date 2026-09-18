"""Checks de calidad post-Silver (matriz docs/calidad_datos.md)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name


@dataclass
class QualityCheckResult:
    check_id: str
    passed: bool
    metric_value: float
    detail: str


def _table(spark: SparkSession, config: LakehouseConfig, layer: str, name: str):
    return spark.table(full_table_name(config, layer, name))


def run_silver_quality_checks(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> list[QualityCheckResult]:
    results: list[QualityCheckResult] = []
    catalog = config.catalog

    expected = {
        "pacientes": 223,
        "citas": 1001,
        "eventos_clinicos": 1602,
        "facturacion": 1202,
    }

    for table, exp in expected.items():
        n = _table(spark, config, "silver", table).count()
        # Pacientes: 1 reject posible por fecha → 222 silver
        low = exp - 2 if table == "pacientes" else exp
        passed = low <= n <= exp
        results.append(
            QualityCheckResult(
                f"Q-COUNT-{table}",
                passed,
                float(n),
                f"silver.{table}={n} (esperado ~{exp}, rejects documentados)",
            )
        )

    pac = _table(spark, config, "silver", "pacientes").select("id_paciente")
    cit = _table(spark, config, "silver", "citas")
    evt = _table(spark, config, "silver", "eventos_clinicos")
    fac = _table(spark, config, "silver", "facturacion")

    h_citas = cit.join(pac, "id_paciente", "left_anti").count()
    results.append(
        QualityCheckResult("Q-S02", h_citas == 0, float(h_citas), "Citas huérfanas paciente")
    )

    h_evt_c = evt.join(cit.select("id_cita"), "id_cita", "left_anti").count()
    results.append(
        QualityCheckResult("Q-S03", h_evt_c == 0, float(h_evt_c), "Eventos sin cita")
    )

    h_fac_c = fac.join(cit.select("id_cita"), "id_cita", "left_anti").count()
    results.append(
        QualityCheckResult("Q-S06", h_fac_c == 0, float(h_fac_c), "Facturas sin cita")
    )

    bad_fin = (
        fac.filter(
            F.round(F.col("valor_bruto") - F.col("valor_descuento"), 2)
            != F.round(F.col("valor_neto"), 2)
        ).count()
    )
    results.append(
        QualityCheckResult("Q-S08", bad_fin == 0, float(bad_fin), "Inconsistencia financiera")
    )

    dup_pac = (
        _table(spark, config, "silver", "pacientes").count()
        - _table(spark, config, "silver", "pacientes").select("id_paciente").distinct().count()
    )
    results.append(
        QualityCheckResult("Q-S01", dup_pac == 0, float(dup_pac), "Duplicados PK pacientes")
    )

    return results


def run_gold_quality_checks(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> list[QualityCheckResult]:
    results: list[QualityCheckResult] = []
    s_citas = _table(spark, config, "silver", "citas").count()
    g_citas = _table(spark, config, "gold", "fact_citas").count()
    results.append(
        QualityCheckResult(
            "Q-G01",
            s_citas == g_citas,
            float(g_citas),
            f"fact_citas={g_citas} vs silver.citas={s_citas}",
        )
    )

    s_neto = (
        _table(spark, config, "silver", "facturacion")
        .agg(F.sum("valor_neto").alias("v"))
        .collect()[0]["v"]
    )
    g_neto = (
        _table(spark, config, "gold", "fact_facturacion")
        .agg(F.sum("valor_neto").alias("v"))
        .collect()[0]["v"]
    )
    diff = abs(float(s_neto or 0) - float(g_neto or 0))
    results.append(
        QualityCheckResult("Q-G02", diff < 0.01, diff, "Reconciliación sum(valor_neto)")
    )

    null_sk = (
        _table(spark, config, "gold", "fact_citas")
        .filter(F.col("sk_paciente").isNull())
        .count()
    )
    results.append(
        QualityCheckResult("Q-G03", null_sk == 0, float(null_sk), "SK paciente nulo en fact_citas")
    )

    bi_cols = _table(spark, config, "gold", "dim_paciente_bi").columns
    pii_absent = not any(c in bi_cols for c in ("nombres", "apellidos", "numero_documento"))
    results.append(
        QualityCheckResult(
            "Q-G04",
            pii_absent,
            1.0 if pii_absent else 0.0,
            "dim_paciente_bi sin columnas PII en claro",
        )
    )

    return results


def log_quality_results(
    spark: SparkSession,
    batch_id: str,
    checks: list[QualityCheckResult],
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> None:
    rows = [
        {
            "check_id": c.check_id,
            "batch_id": batch_id,
            "table_name": "silver",
            "passed": c.passed,
            "metric_value": c.metric_value,
            "threshold": 0.0,
        }
        for c in checks
    ]
    if not rows:
        return
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(
        full_table_name(config, "ops", "data_quality_results")
    )
