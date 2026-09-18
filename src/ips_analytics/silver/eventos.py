"""Silver: eventos clínicos."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name
from ips_analytics.ops.watermark import advance_watermark, filter_bronze_incremental, get_watermark
from ips_analytics.silver.common import (
    append_rejects,
    bronze_table,
    clean_str,
    dedupe_by_pk,
    null_like,
    parse_timestamp,
    silver_table,
    write_or_merge_silver,
)
from ips_analytics.silver.pacientes import EntitySilverResult


def transform_eventos(bronze_df: DataFrame) -> DataFrame:
    valor = F.when(null_like("valor_resultado"), F.lit(None)).otherwise(
        F.col("valor_resultado").cast("decimal(18,4)")
    )
    return (
        bronze_df.select(
            clean_str("id_evento").alias("id_evento"),
            clean_str("id_cita").alias("id_cita"),
            clean_str("id_paciente").alias("id_paciente"),
            clean_str("tipo_evento").alias("tipo_evento"),
            clean_str("codigo_evento").alias("codigo_evento"),
            clean_str("descripcion_evento").alias("descripcion_evento"),
            parse_timestamp("fecha_hora_evento").alias("fecha_hora_evento"),
            valor.alias("valor_resultado"),
            clean_str("unidad_resultado").alias("unidad_resultado"),
            clean_str("estado_evento").alias("estado_evento"),
            parse_timestamp("creado_en").alias("creado_en"),
            parse_timestamp("actualizado_en").alias("actualizado_en"),
            F.col("_batch_id"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn("tiene_resultado", F.col("valor_resultado").isNotNull())
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def enrich_coherencia_cita(df: DataFrame, citas_df: DataFrame) -> DataFrame:
    c = citas_df.select(
        F.col("id_cita").alias("_cid"),
        F.col("id_paciente").alias("_pac_cita"),
    )
    return (
        df.join(c, F.col("id_cita") == F.col("_cid"), "left")
        .withColumn(
            "paciente_coherente_con_cita",
            F.col("id_paciente") == F.col("_pac_cita"),
        )
        .drop("_cid", "_pac_cita")
    )


def split_eventos_valid_rejects(
    df: DataFrame, pacientes_df: DataFrame, citas_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    enriched = enrich_coherencia_cita(df, citas_df)
    pac_ids = pacientes_df.select("id_paciente").distinct()
    cita_ids = citas_df.select("id_cita").distinct()

    sin_cita = enriched.join(cita_ids, "id_cita", "left_anti").withColumn(
        "_reject_reason", F.lit("FK id_cita inexistente")
    )
    con_cita = enriched.join(cita_ids, "id_cita", "inner")
    sin_pac = con_cita.join(pac_ids, "id_paciente", "left_anti").withColumn(
        "_reject_reason", F.lit("FK id_paciente inexistente")
    )
    resto = con_cita.join(pac_ids, "id_paciente", "inner")
    incoherente = resto.filter(~F.col("paciente_coherente_con_cita")).withColumn(
        "_reject_reason", F.lit("id_paciente no coincide con cita")
    )
    invalid_pk = F.col("id_evento").isNull() | F.col("fecha_hora_evento").isNull()
    valid = resto.filter(F.col("paciente_coherente_con_cita") & ~invalid_pk)
    rejects_pk = resto.filter(F.col("paciente_coherente_con_cita") & invalid_pk).withColumn(
        "_reject_reason", F.lit("PK o fecha_hora_evento inválida")
    )
    rejects = sin_cita.unionByName(sin_pac, allowMissingColumns=True)
    rejects = rejects.unionByName(incoherente, allowMissingColumns=True)
    rejects = rejects.unionByName(rejects_pk, allowMissingColumns=True)
    return valid, rejects


def process_eventos(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
) -> EntitySilverResult:
    bronze_df = bronze_table(spark, config, "eventos_clinicos")
    if load_mode == "incremental":
        bronze_df = filter_bronze_incremental(
            bronze_df, get_watermark(spark, "eventos_clinicos", config)
        )
    pacientes_df = silver_table(spark, config, "pacientes")
    citas_df = silver_table(spark, config, "citas")
    rows_in = bronze_df.count()
    if rows_in == 0 and load_mode == "incremental":
        return EntitySilverResult("eventos_clinicos", 0, 0, 0)

    transformed = transform_eventos(bronze_df)
    transformed = dedupe_by_pk(transformed, "id_evento")
    valid, rejects = split_eventos_valid_rejects(transformed, pacientes_df, citas_df)

    write_or_merge_silver(
        valid,
        full_table_name(config, "silver", "eventos_clinicos"),
        "id_evento",
        load_mode,
    )
    rejected = append_rejects(rejects, entity="eventos_clinicos", batch_id=batch_id, config=config)
    rows_silver = valid.count()
    if load_mode == "incremental" and rows_silver > 0:
        max_ts = valid.agg(F.max("actualizado_en").alias("m")).collect()[0]["m"]
        if max_ts is not None:
            advance_watermark(spark, "eventos_clinicos", max_ts, batch_id, config)

    return EntitySilverResult("eventos_clinicos", rows_in, rows_silver, rejected)
