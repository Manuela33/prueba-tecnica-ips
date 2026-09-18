"""Silver: citas."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name
from ips_analytics.ops.watermark import advance_watermark, filter_bronze_incremental, get_watermark
from ips_analytics.silver.common import (
    append_rejects,
    bronze_table,
    clean_str,
    coalesce_unknown,
    dedupe_by_pk,
    parse_timestamp,
    silver_table,
    write_or_merge_silver,
)
from ips_analytics.silver.pacientes import EntitySilverResult


def transform_citas(bronze_df: DataFrame) -> DataFrame:
    id_prof = clean_str("id_profesional")
    return (
        bronze_df.select(
            clean_str("id_cita").alias("id_cita"),
            clean_str("id_paciente").alias("id_paciente"),
            id_prof.alias("id_profesional"),
            clean_str("especialidad").alias("especialidad"),
            coalesce_unknown("sede").alias("sede"),
            parse_timestamp("fecha_hora_cita").alias("fecha_hora_cita"),
            clean_str("tipo_cita").alias("tipo_cita"),
            clean_str("estado_cita").alias("estado_cita"),
            clean_str("motivo_cancelacion").alias("motivo_cancelacion"),
            parse_timestamp("creado_en").alias("creado_en"),
            parse_timestamp("actualizado_en").alias("actualizado_en"),
            F.col("_batch_id"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "motivo_cancelacion",
            F.when(
                F.col("estado_cita") == F.lit("Cancelada"),
                F.col("motivo_cancelacion"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("fecha_cita", F.to_date(F.col("fecha_hora_cita")))
        .withColumn("sin_profesional_asignado", F.col("id_profesional").isNull())
        .withColumn("es_cita_cancelada", F.col("estado_cita") == F.lit("Cancelada"))
        .withColumn("es_no_asistencia", F.col("estado_cita") == F.lit("No asistió"))
        .withColumn("es_cita_atendida", F.col("estado_cita") == F.lit("Atendida"))
        .withColumn(
            "motivo_cancelacion_faltante",
            F.col("es_cita_cancelada") & F.col("motivo_cancelacion").isNull(),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def split_citas_valid_rejects(
    df: DataFrame, pacientes_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    pac_ids = pacientes_df.select("id_paciente").distinct()
    huerfanas = df.join(pac_ids, "id_paciente", "left_anti")

    base_valid = df.join(pac_ids, "id_paciente", "inner")
    invalid_pk = F.col("id_cita").isNull() | F.col("fecha_hora_cita").isNull()
    rejects_h = huerfanas.withColumn("_reject_reason", F.lit("FK id_paciente inexistente"))
    rejects_i = base_valid.filter(invalid_pk).withColumn(
        "_reject_reason", F.lit("PK o fecha_hora_cita inválida")
    )
    valid = base_valid.filter(~invalid_pk)
    rejects = rejects_h.unionByName(rejects_i, allowMissingColumns=True)
    return valid, rejects


def process_citas(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
) -> EntitySilverResult:
    bronze_df = bronze_table(spark, config, "citas")
    if load_mode == "incremental":
        bronze_df = filter_bronze_incremental(bronze_df, get_watermark(spark, "citas", config))
    pacientes_df = silver_table(spark, config, "pacientes")
    rows_in = bronze_df.count()
    if rows_in == 0 and load_mode == "incremental":
        return EntitySilverResult("citas", 0, 0, 0)

    transformed = transform_citas(bronze_df)
    transformed = dedupe_by_pk(transformed, "id_cita")
    valid, rejects = split_citas_valid_rejects(transformed, pacientes_df)

    write_or_merge_silver(valid, full_table_name(config, "silver", "citas"), "id_cita", load_mode)
    rejected = append_rejects(rejects, entity="citas", batch_id=batch_id, config=config)
    rows_silver = valid.count()
    if load_mode == "incremental" and rows_silver > 0:
        from pyspark.sql import functions as F

        max_ts = valid.agg(F.max("actualizado_en").alias("m")).collect()[0]["m"]
        if max_ts is not None:
            advance_watermark(spark, "citas", max_ts, batch_id, config)

    return EntitySilverResult("citas", rows_in, rows_silver, rejected)
