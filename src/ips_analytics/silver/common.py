"""Utilidades compartidas capa Silver."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name


def null_like(col_name: str) -> Column:
    c = F.trim(F.col(col_name).cast("string"))
    return (
        F.col(col_name).isNull()
        | (c == "")
        | c.isin("nan", "None", "NaT", "null", "NULL")
    )


def clean_str(col_name: str) -> Column:
    return F.when(null_like(col_name), F.lit(None)).otherwise(
        F.trim(F.col(col_name).cast("string"))
    )


def coalesce_unknown(col_name: str, unknown: str = "DESCONOCIDO") -> Column:
    return F.coalesce(clean_str(col_name), F.lit(unknown))


def parse_timestamp(col_name: str) -> Column:
    return F.to_timestamp(clean_str(col_name))


def parse_date_from_string(col_name: str) -> Column:
    return F.to_date(parse_timestamp(col_name))


def dedupe_by_pk(df: DataFrame, pk: str, order_col: str = "actualizado_en") -> DataFrame:
    w = Window.partitionBy(pk).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_silver(df: DataFrame, table_fqn: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )


def merge_silver(df: DataFrame, table_fqn: str, pk: str) -> None:
    """MERGE idempotente por PK (modo incremental)."""
    from delta.tables import DeltaTable

    if df.head(1) == []:
        return
    try:
        target = DeltaTable.forName(df.sparkSession, table_fqn)
    except Exception:
        write_silver(df, table_fqn)
        return
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{pk} = s.{pk}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_or_merge_silver(
    df: DataFrame,
    table_fqn: str,
    pk: str,
    load_mode: str,
) -> None:
    if load_mode == "incremental":
        merge_silver(df, table_fqn, pk)
    else:
        write_silver(df, table_fqn)


def append_rejects(
    df: DataFrame,
    *,
    entity: str,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> int:
    if df.head(1) == []:
        return 0
    table_fqn = full_table_name(config, "silver", "rejects")
    out = (
        df.withColumn("_entity", F.lit(entity))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    (
        out.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_fqn)
    )
    return out.count()


def init_rejects_table(spark, config: LakehouseConfig = DEFAULT_CONFIG) -> None:
    """Overwrite vacío al inicio de corrida Silver."""
    table_fqn = full_table_name(config, "silver", "rejects")
    spark.createDataFrame(
        [],
        "_entity STRING, _reject_reason STRING, _batch_id STRING, _silver_processed_at TIMESTAMP",
    ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_fqn
    )


def bronze_table(spark, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.table(full_table_name(config, "bronze", name))


def silver_table(spark, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.table(full_table_name(config, "silver", name))
