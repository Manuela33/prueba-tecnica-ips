"""
Ingesta Bronze: Excel (Volume) → Delta con metadatos y cuarentena.

Diseñado para ejecutarse en Databricks (PySpark + pandas/openpyxl en driver).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from ips_analytics.config import (
    BRONZE_SOURCES,
    DEFAULT_CONFIG,
    LakehouseConfig,
    full_table_name,
)
from ips_analytics.ops.watermark import (
    advance_watermark,
    filter_bronze_incremental,
    get_watermark,
    init_watermarks_if_empty,
)


@dataclass
class BronzeIngestResult:
    entity: str
    table_fqn: str
    batch_id: str
    rows_read: int
    rows_bronze: int
    rows_quarantine: int
    source_file: str


def _null_like(col_name: str):
    c = F.trim(F.col(col_name).cast("string"))
    return (
        F.col(col_name).isNull()
        | (c == "")
        | c.isin("nan", "None", "NaT", "null", "NULL")
    )


def read_excel_as_strings(spark: SparkSession, excel_path: str) -> DataFrame:
    """Lee Excel en el driver y materializa Spark DataFrame (columnas string)."""
    pdf = pd.read_excel(excel_path, engine="openpyxl")
    pdf = pdf.astype(object).where(pd.notna(pdf), None)
    for col in pdf.columns:
        pdf[col] = pdf[col].apply(lambda v: None if v is None else str(v))
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    return spark.createDataFrame(pdf, schema=schema)


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_file: str,
    batch_id: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    ts = ingested_at or datetime.now(timezone.utc)
    business_cols = [c for c in df.columns if not c.startswith("_")]
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols],
    )
    return (
        df.withColumn("_ingested_at", F.lit(ts).cast(TimestampType()))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_row_hash", F.sha2(concat_expr, 256))
    )


def split_valid_and_quarantine(df: DataFrame, pk: str) -> tuple[DataFrame, DataFrame]:
    invalid = df.filter(_null_like(pk))
    valid = df.filter(~_null_like(pk))
    quarantine = invalid.withColumn("_reject_reason", F.lit(f"PK vacía o inválida: {pk}"))
    return valid, quarantine


def write_delta_table(
    df: DataFrame,
    table_fqn: str,
    *,
    mode: str = "overwrite",
) -> None:
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )


def merge_delta_table(df: DataFrame, table_fqn: str, pk: str) -> None:
    from delta.tables import DeltaTable

    if df.head(1) == []:
        return
    try:
        target = DeltaTable.forName(df.sparkSession, table_fqn)
    except Exception:
        write_delta_table(df, table_fqn, mode="overwrite")
        return
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{pk} = s.{pk}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def ingest_entity(
    spark: SparkSession,
    source: dict[str, str],
    *,
    raw_volume_path: str,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
) -> BronzeIngestResult:
    if load_mode not in ("full", "incremental"):
        raise ValueError("load_mode debe ser 'full' o 'incremental'.")

    entity = source["entity"]
    file_name = source["file_name"]
    pk = source["pk"]
    table_fqn = full_table_name(config, "bronze", source["table"])
    quarantine_fqn = full_table_name(config, "bronze", "quarantine")
    excel_path = f"{raw_volume_path.rstrip('/')}/{file_name}"

    init_watermarks_if_empty(spark, config)
    raw_df = read_excel_as_strings(spark, excel_path)
    if load_mode == "incremental":
        raw_df = filter_bronze_incremental(raw_df, get_watermark(spark, entity, config))
    rows_read = raw_df.count()

    enriched = add_bronze_metadata(
        raw_df,
        source_file=file_name,
        batch_id=batch_id,
    )
    valid_df, quarantine_df = split_valid_and_quarantine(enriched, pk)

    if load_mode == "full":
        write_delta_table(valid_df, table_fqn, mode="overwrite")
    else:
        merge_delta_table(valid_df, table_fqn, pk)
        if rows_read > 0:
            max_ts = valid_df.agg(F.max(F.to_timestamp(F.col("actualizado_en"))).alias("m")).collect()[
                0
            ]["m"]
            if max_ts is not None:
                advance_watermark(spark, entity, max_ts, batch_id, config)

    rows_quarantine = quarantine_df.count()
    if rows_quarantine > 0:
        q_out = (
            quarantine_df.withColumn("_entity", F.lit(entity))
            .withColumn("_quarantined_at", F.current_timestamp())
        )
        (
            q_out.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_fqn)
        )
    else:
        rows_quarantine = 0

    rows_bronze = valid_df.count() if rows_read > 0 else 0
    return BronzeIngestResult(
        entity=entity,
        table_fqn=table_fqn,
        batch_id=batch_id,
        rows_read=rows_read,
        rows_bronze=rows_bronze,
        rows_quarantine=rows_quarantine,
        source_file=file_name,
    )


def run_bronze_ingestion(
    spark: SparkSession,
    *,
    raw_volume_path: str,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
    sources: tuple[dict[str, str], ...] | None = None,
) -> list[BronzeIngestResult]:
    src_list = sources or BRONZE_SOURCES
    results: list[BronzeIngestResult] = []
    for source in src_list:
        results.append(
            ingest_entity(
                spark,
                source,
                raw_volume_path=raw_volume_path,
                batch_id=batch_id,
                config=config,
                load_mode=load_mode,
            )
        )
    return results


def log_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    batch_id: str,
    stage: str,
    status: str,
    started_at: datetime,
    ended_at: datetime | None,
    rows_in: int | None,
    rows_out: int | None,
    rows_rejected: int | None,
    error_message: str | None,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> None:
    """Append a ops.pipeline_runs (tabla creada en 01_ops_tables.sql)."""
    table_fqn = full_table_name(config, "ops", "pipeline_runs")
    row: dict[str, Any] = {
        "run_id": run_id,
        "batch_id": batch_id,
        "stage": stage,
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(table_fqn)
