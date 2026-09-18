"""Silver: pacientes."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name
from ips_analytics.ops.watermark import advance_watermark, get_watermark
from ips_analytics.silver.common import (
    append_rejects,
    bronze_table,
    clean_str,
    coalesce_unknown,
    dedupe_by_pk,
    parse_timestamp,
    write_or_merge_silver,
)
from ips_analytics.ops.watermark import filter_bronze_incremental


@dataclass
class EntitySilverResult:
    entity: str
    rows_in: int
    rows_silver: int
    rows_rejected: int


def transform_pacientes(bronze_df: DataFrame) -> DataFrame:
    raw_fecha = clean_str("fecha_nacimiento")
    fecha = F.to_date(F.to_timestamp(raw_fecha))
    return (
        bronze_df.select(
            clean_str("id_paciente").alias("id_paciente"),
            clean_str("tipo_documento").alias("tipo_documento"),
            clean_str("numero_documento").alias("numero_documento"),
            clean_str("nombres").alias("nombres"),
            clean_str("apellidos").alias("apellidos"),
            raw_fecha.alias("_fecha_nacimiento_raw"),
            fecha.alias("fecha_nacimiento"),
            clean_str("sexo").alias("sexo"),
            clean_str("aseguradora").alias("aseguradora"),
            coalesce_unknown("ciudad").alias("ciudad"),
            clean_str("estado").alias("estado"),
            parse_timestamp("creado_en").alias("creado_en"),
            parse_timestamp("actualizado_en").alias("actualizado_en"),
            F.col("_batch_id"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "_fecha_invalida",
            F.col("_fecha_nacimiento_raw").isNotNull() & F.col("fecha_nacimiento").isNull(),
        )
        .withColumn(
            "edad_anios",
            F.when(
                F.col("fecha_nacimiento").isNotNull(),
                F.floor(F.datediff(F.current_date(), F.col("fecha_nacimiento")) / F.lit(365.25)),
            ),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def split_pacientes_valid_rejects(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid_fecha = F.col("_fecha_invalida")
    invalid_sexo = F.col("sexo").isNotNull() & ~F.col("sexo").isin("F", "M")
    invalid_pk = F.col("id_paciente").isNull()

    reject_cond = invalid_fecha | invalid_sexo | invalid_pk
    rejects = (
        df.filter(reject_cond)
        .withColumn(
            "_reject_reason",
            F.when(invalid_pk, F.lit("PK id_paciente nula"))
            .when(invalid_fecha, F.lit("fecha_nacimiento no parseable"))
            .when(invalid_sexo, F.lit("sexo fuera de dominio F/M"))
            .otherwise(F.lit("rechazo pacientes")),
        )
    )
    valid = df.filter(~reject_cond).drop("_fecha_nacimiento_raw", "_fecha_invalida")
    rejects = rejects.drop("_fecha_nacimiento_raw", "_fecha_invalida")
    return valid, rejects


def process_pacientes(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
) -> EntitySilverResult:
    bronze_df = bronze_table(spark, config, "pacientes")
    if load_mode == "incremental":
        wm = get_watermark(spark, "pacientes", config)
        bronze_df = filter_bronze_incremental(bronze_df, wm)
    rows_in = bronze_df.count()

    if rows_in == 0 and load_mode == "incremental":
        return EntitySilverResult("pacientes", 0, 0, 0)

    transformed = transform_pacientes(bronze_df)
    transformed = dedupe_by_pk(transformed, "id_paciente")
    valid, rejects = split_pacientes_valid_rejects(transformed)

    table_fqn = full_table_name(config, "silver", "pacientes")
    write_or_merge_silver(valid, table_fqn, "id_paciente", load_mode)
    rejected = append_rejects(rejects, entity="pacientes", batch_id=batch_id, config=config)

    rows_silver = valid.count()
    if load_mode == "incremental" and rows_silver > 0:
        max_ts = valid.agg(F.max("actualizado_en").alias("m")).collect()[0]["m"]
        if max_ts is not None:
            advance_watermark(spark, "pacientes", max_ts, batch_id, config)

    return EntitySilverResult("pacientes", rows_in, rows_silver, rejected)
