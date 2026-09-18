"""Control de watermarks incrementales (ops.ingestion_watermark)."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

WATERMARK_SCHEMA = StructType(
    [
        StructField("source_table", StringType(), False),
        StructField("watermark_column", StringType(), False),
        StructField("last_watermark", TimestampType(), True),
        StructField("last_batch_id", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _spark_timestamp(dt: datetime) -> datetime:
    """PySpark en Windows/local suele fallar con datetime timezone-aware."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

SILVER_ENTITIES: tuple[str, ...] = (
    "pacientes",
    "citas",
    "eventos_clinicos",
    "facturacion",
)


def _watermark_table(spark: SparkSession, config: LakehouseConfig) -> DataFrame:
    return spark.table(full_table_name(config, "ops", "ingestion_watermark"))


def get_watermark(
    spark: SparkSession,
    source_table: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> datetime:
    """Último `actualizado_en` procesado; EPOCH si no hay registro."""
    try:
        df = _watermark_table(spark, config).filter(F.col("source_table") == source_table)
        if df.head(1) == []:
            return EPOCH
        ts = df.select("last_watermark").collect()[0][0]
        return ts if ts is not None else EPOCH
    except Exception:
        return EPOCH


def init_watermarks_if_empty(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> None:
    """Inserta filas iniciales EPOCH por entidad si la tabla está vacía."""
    table_fqn = full_table_name(config, "ops", "ingestion_watermark")
    try:
        if _watermark_table(spark, config).head(1) != []:
            return
    except Exception:
        pass
    rows = [
        {
            "source_table": name,
            "watermark_column": "actualizado_en",
            "last_watermark": _spark_timestamp(EPOCH),
            "last_batch_id": None,
            "updated_at": _spark_timestamp(datetime.now(timezone.utc)),
        }
        for name in SILVER_ENTITIES
    ]
    spark.createDataFrame(rows, schema=WATERMARK_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table_fqn)


def advance_watermark(
    spark: SparkSession,
    source_table: str,
    last_watermark: datetime,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> None:
    table_fqn = full_table_name(config, "ops", "ingestion_watermark")
    row = spark.createDataFrame(
        [
            {
                "source_table": source_table,
                "watermark_column": "actualizado_en",
                "last_watermark": _spark_timestamp(last_watermark),
                "last_batch_id": batch_id,
                "updated_at": _spark_timestamp(datetime.now(timezone.utc)),
            }
        ],
        schema=WATERMARK_SCHEMA,
    )
    try:
        from delta.tables import DeltaTable

        target = DeltaTable.forName(spark, table_fqn)
        (
            target.alias("t")
            .merge(row.alias("s"), "t.source_table = s.source_table")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        row.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            table_fqn
        )


def sync_watermarks_from_silver(
    spark: SparkSession,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> None:
    """Tras carga full: watermark = MAX(actualizado_en) en cada tabla Silver."""
    for name in SILVER_ENTITIES:
        df = spark.table(full_table_name(config, "silver", name))
        if df.head(1) == []:
            continue
        max_ts = df.agg(F.max("actualizado_en").alias("m")).collect()[0]["m"]
        if max_ts is not None:
            advance_watermark(spark, name, max_ts, batch_id, config)


def filter_bronze_incremental(
    bronze_df: DataFrame,
    watermark: datetime,
) -> DataFrame:
    """Filtra filas Bronze (strings) con actualizado_en > watermark."""
    return bronze_df.filter(F.to_timestamp(F.col("actualizado_en")) > F.lit(watermark))
