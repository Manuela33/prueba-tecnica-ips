"""Silver: facturación."""

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


def transform_facturacion(bronze_df: DataFrame) -> DataFrame:
    bruto = F.col("valor_bruto").cast("decimal(18,2)")
    desc = F.col("valor_descuento").cast("decimal(18,2)")
    neto = F.col("valor_neto").cast("decimal(18,2)")
    estado = clean_str("estado_pago")
    return (
        bronze_df.select(
            clean_str("id_factura").alias("id_factura"),
            clean_str("id_paciente").alias("id_paciente"),
            clean_str("id_cita").alias("id_cita"),
            clean_str("tipo_servicio").alias("tipo_servicio"),
            coalesce_unknown("pagador").alias("pagador"),
            parse_timestamp("fecha_factura").alias("fecha_factura"),
            bruto.alias("valor_bruto"),
            desc.alias("valor_descuento"),
            neto.alias("valor_neto"),
            estado.alias("estado_pago"),
            parse_timestamp("creado_en").alias("creado_en"),
            parse_timestamp("actualizado_en").alias("actualizado_en"),
            F.col("_batch_id"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn("fecha_factura_date", F.to_date(F.col("fecha_factura")))
        .withColumn("es_factura_anulada", F.col("estado_pago") == F.lit("Cancelado"))
        .withColumn(
            "monto_cartera",
            F.when(
                F.col("estado_pago").isin("Pendiente", "Pago parcial"),
                F.col("valor_neto"),
            ).otherwise(F.lit(0).cast("decimal(18,2)")),
        )
        .withColumn(
            "ecuacion_financiera_ok",
            F.round(F.col("valor_bruto") - F.col("valor_descuento"), 2)
            == F.round(F.col("valor_neto"), 2),
        )
        .withColumn(
            "montos_no_negativos",
            (F.col("valor_bruto") >= 0)
            & (F.col("valor_descuento") >= 0)
            & (F.col("valor_neto") >= 0),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def split_facturacion_valid_rejects(
    df: DataFrame, pacientes_df: DataFrame, citas_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    pac_ids = pacientes_df.select("id_paciente").distinct()
    cita_ids = citas_df.select("id_cita").distinct()

    r1 = df.join(pac_ids, "id_paciente", "left_anti").withColumn(
        "_reject_reason", F.lit("FK id_paciente inexistente")
    )
    d1 = df.join(pac_ids, "id_paciente", "inner")
    r2 = d1.join(cita_ids, "id_cita", "left_anti").withColumn(
        "_reject_reason", F.lit("FK id_cita inexistente")
    )
    d2 = d1.join(cita_ids, "id_cita", "inner")

    bad_fin = (~F.col("ecuacion_financiera_ok")) | (~F.col("montos_no_negativos"))
    r3 = d2.filter(bad_fin).withColumn(
        "_reject_reason", F.lit("Regla financiera o montos negativos")
    )
    invalid_pk = F.col("id_factura").isNull() | F.col("fecha_factura").isNull()
    r4 = d2.filter(~bad_fin & invalid_pk).withColumn(
        "_reject_reason", F.lit("PK o fecha_factura inválida")
    )
    valid = d2.filter(~bad_fin & ~invalid_pk)
    rejects = r1.unionByName(r2, allowMissingColumns=True)
    rejects = rejects.unionByName(r3, allowMissingColumns=True)
    rejects = rejects.unionByName(r4, allowMissingColumns=True)
    return valid, rejects


def process_facturacion(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    load_mode: str = "full",
) -> EntitySilverResult:
    bronze_df = bronze_table(spark, config, "facturacion")
    if load_mode == "incremental":
        bronze_df = filter_bronze_incremental(
            bronze_df, get_watermark(spark, "facturacion", config)
        )
    pacientes_df = silver_table(spark, config, "pacientes")
    citas_df = silver_table(spark, config, "citas")
    rows_in = bronze_df.count()
    if rows_in == 0 and load_mode == "incremental":
        return EntitySilverResult("facturacion", 0, 0, 0)

    transformed = transform_facturacion(bronze_df)
    transformed = dedupe_by_pk(transformed, "id_factura")
    valid, rejects = split_facturacion_valid_rejects(transformed, pacientes_df, citas_df)

    write_or_merge_silver(
        valid,
        full_table_name(config, "silver", "facturacion"),
        "id_factura",
        load_mode,
    )
    rejected = append_rejects(rejects, entity="facturacion", batch_id=batch_id, config=config)
    rows_silver = valid.count()
    if load_mode == "incremental" and rows_silver > 0:
        max_ts = valid.agg(F.max("actualizado_en").alias("m")).collect()[0]["m"]
        if max_ts is not None:
            advance_watermark(spark, "facturacion", max_ts, batch_id, config)

    return EntitySilverResult("facturacion", rows_in, rows_silver, rejected)
