"""Gold: modelo estrella desde Silver."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ips_analytics.bronze.ingest_excel import log_pipeline_run
from ips_analytics.config import DEFAULT_CONFIG, LakehouseConfig, full_table_name  


def _silver(spark: SparkSession, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.table(full_table_name(config, "silver", name))


def _write_gold(df: DataFrame, table_fqn: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )


def build_dim_tiempo(spark: SparkSession, config: LakehouseConfig) -> DataFrame:
    """Calendario 2025-01-01 .. 2027-12-31 (cubre rango de datos demo)."""
    return spark.sql(
        """
        SELECT
          CAST(DATE_FORMAT(fecha, 'yyyyMMdd') AS INT) AS sk_fecha,
          fecha,
          YEAR(fecha) AS anio,
          MONTH(fecha) AS mes,
          QUARTER(fecha) AS trimestre,
          DATE_FORMAT(fecha, 'MMMM') AS nombre_mes,
          CASE WHEN DAYOFWEEK(fecha) IN (1, 7) THEN true ELSE false END AS es_fin_semana
        FROM (
          SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) AS fecha
        )
        """
    )


def build_dim_paciente(pacientes: DataFrame) -> DataFrame:
    w = Window.orderBy("id_paciente")
    return pacientes.select(
        F.row_number().over(w).alias("sk_paciente"),
        F.col("id_paciente"),
        F.col("tipo_documento"),
        F.sha2(F.col("numero_documento").cast("string"), 256).alias("numero_documento_hash"),
        F.col("sexo"),
        F.col("aseguradora"),
        F.col("ciudad"),
        F.col("estado").alias("estado_paciente"),
        F.col("fecha_nacimiento"),
        F.col("edad_anios"),
        F.current_timestamp().alias("_valid_from"),
    )


def build_dim_lookup(df: DataFrame, value_col: str, sk_name: str) -> DataFrame:
    distinct = df.select(F.col(value_col).alias("nombre")).distinct().orderBy("nombre")
    w = Window.orderBy("nombre")
    return distinct.withColumn(sk_name, F.row_number().over(w))


def _sk_fecha(date_col: F.Column) -> F.Column:
    return F.date_format(date_col, "yyyyMMdd").cast("int")


def build_fact_citas(
    citas: DataFrame,
    dim_paciente: DataFrame,
    dim_esp: DataFrame,
    dim_sede: DataFrame,
) -> DataFrame:
    dp = dim_paciente.select("sk_paciente", "id_paciente")
    de = dim_esp.select(F.col("nombre").alias("especialidad"), F.col("sk_especialidad"))
    ds = dim_sede.select(F.col("nombre").alias("sede"), F.col("sk_sede"))
    return (
        citas.join(dp, "id_paciente", "inner")
        .join(de, "especialidad", "left")
        .join(ds, "sede", "left")
        .select(
            F.col("id_cita"),
            _sk_fecha(F.col("fecha_cita")).alias("sk_fecha_cita"),
            F.col("sk_paciente"),
            F.col("sk_especialidad"),
            F.col("sk_sede"),
            F.col("id_paciente"),
            F.col("especialidad"),
            F.col("sede"),
            F.col("tipo_cita"),
            F.col("estado_cita"),
            F.col("fecha_hora_cita"),
            F.col("fecha_cita"),
            F.col("es_cita_atendida").cast("int").alias("es_atendida"),
            F.col("es_cita_cancelada").cast("int").alias("es_cancelada"),
            F.col("es_no_asistencia").cast("int").alias("es_no_asistencia"),
            F.current_timestamp().alias("_gold_loaded_at"),
        )
    )


def build_fact_eventos(
    eventos: DataFrame,
    dim_paciente: DataFrame,
    dim_tipo_evento: DataFrame,
) -> DataFrame:
    dp = dim_paciente.select("sk_paciente", "id_paciente")
    dt = dim_tipo_evento.select(F.col("nombre").alias("tipo_evento"), F.col("sk_tipo_evento"))
    return (
        eventos.join(dp, "id_paciente", "inner")
        .join(dt, "tipo_evento", "left")
        .select(
            F.col("id_evento"),
            F.col("id_cita"),
            F.col("sk_paciente"),
            F.col("sk_tipo_evento"),
            _sk_fecha(F.to_date(F.col("fecha_hora_evento"))).alias("sk_fecha_evento"),
            F.col("tipo_evento"),
            F.col("estado_evento"),
            F.col("fecha_hora_evento"),
            F.col("valor_resultado"),
            F.col("tiene_resultado").cast("int").alias("tiene_resultado"),
            F.current_timestamp().alias("_gold_loaded_at"),
        )
    )


def build_fact_facturacion(
    fact: DataFrame,
    dim_paciente: DataFrame,
    dim_tipo_servicio: DataFrame,
    dim_pagador: DataFrame,
) -> DataFrame:
    dp = dim_paciente.select("sk_paciente", "id_paciente")
    dts = dim_tipo_servicio.select(
        F.col("nombre").alias("tipo_servicio"), F.col("sk_tipo_servicio")
    )
    dpg = dim_pagador.select(F.col("nombre").alias("pagador"), F.col("sk_pagador"))
    return (
        fact.join(dp, "id_paciente", "inner")
        .join(dts, "tipo_servicio", "left")
        .join(dpg, "pagador", "left")
        .select(
            F.col("id_factura"),
            F.col("id_cita"),
            F.col("sk_paciente"),
            F.col("sk_tipo_servicio"),
            F.col("sk_pagador"),
            _sk_fecha(F.col("fecha_factura_date")).alias("sk_fecha_factura"),
            F.col("tipo_servicio"),
            F.col("pagador"),
            F.col("fecha_factura"),
            F.col("fecha_factura_date"),
            F.col("valor_bruto"),
            F.col("valor_descuento"),
            F.col("valor_neto"),
            F.col("monto_cartera"),
            F.col("estado_pago"),
            F.col("es_factura_anulada").cast("int").alias("es_factura_anulada"),
            F.current_timestamp().alias("_gold_loaded_at"),
        )
    )


def _view_ref(config: LakehouseConfig, layer: str, table: str) -> str:
    return full_table_name(config, layer, table)


def create_kpi_views(spark: SparkSession, config: LakehouseConfig) -> None:
    if config.catalog == "__local__":
        spark.sql("USE gold")
    else:
        spark.sql(f"USE CATALOG {config.catalog}")
        spark.sql("USE SCHEMA gold")

    fc = _view_ref(config, "gold", "fact_citas")
    ff = _view_ref(config, "gold", "fact_facturacion")
    dt = _view_ref(config, "gold", "dim_tiempo")
    dp = _view_ref(config, "gold", "dim_paciente")

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW v_kpi_operacion_citas AS
        SELECT
          fc.fecha_cita,
          dt.anio,
          dt.mes,
          fc.especialidad,
          fc.sede,
          fc.estado_cita,
          COUNT(*) AS total_citas,
          SUM(fc.es_atendida) AS citas_atendidas,
          SUM(fc.es_cancelada) AS citas_canceladas,
          SUM(fc.es_no_asistencia) AS citas_no_asistencia
        FROM {fc} fc
        LEFT JOIN {dt} dt ON fc.sk_fecha_cita = dt.sk_fecha
        GROUP BY fc.fecha_cita, dt.anio, dt.mes, fc.especialidad, fc.sede, fc.estado_cita
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW v_kpi_facturacion_mensual AS
        SELECT
          dt.anio,
          dt.mes,
          ff.pagador,
          ff.tipo_servicio,
          SUM(ff.valor_bruto) AS total_bruto,
          SUM(ff.valor_descuento) AS total_descuento,
          SUM(ff.valor_neto) AS total_neto,
          SUM(CASE WHEN ff.es_factura_anulada = 0 THEN ff.valor_neto ELSE 0 END) AS ingreso_neto_vigente,
          SUM(ff.monto_cartera) AS cartera_pendiente,
          COUNT(*) AS lineas_factura
        FROM {ff} ff
        LEFT JOIN {dt} dt ON ff.sk_fecha_factura = dt.sk_fecha
        GROUP BY dt.anio, dt.mes, ff.pagador, ff.tipo_servicio
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW v_kpi_resumen_ips AS
        WITH citas_mes AS (
          SELECT
            dt.anio,
            dt.mes,
            COUNT(DISTINCT fc.id_cita) AS total_citas,
            SUM(fc.es_atendida) AS citas_atendidas,
            SUM(fc.es_cancelada) AS citas_canceladas,
            SUM(fc.es_no_asistencia) AS citas_no_asistencia
          FROM {fc} fc
          INNER JOIN {dt} dt ON fc.sk_fecha_cita = dt.sk_fecha
          GROUP BY dt.anio, dt.mes
        ),
        fin_mes AS (
          SELECT
            dt.anio,
            dt.mes,
            SUM(CASE WHEN ff.es_factura_anulada = 0 THEN ff.valor_neto ELSE 0 END) AS ingreso_neto,
            SUM(ff.monto_cartera) AS cartera_pendiente,
            COUNT(ff.id_factura) AS lineas_factura
          FROM {ff} ff
          INNER JOIN {dt} dt ON ff.sk_fecha_factura = dt.sk_fecha
          GROUP BY dt.anio, dt.mes
        )
        SELECT
          COALESCE(c.anio, f.anio) AS anio,
          COALESCE(c.mes, f.mes) AS mes,
          COALESCE(c.total_citas, 0) AS total_citas,
          COALESCE(c.citas_atendidas, 0) AS citas_atendidas,
          COALESCE(c.citas_canceladas, 0) AS citas_canceladas,
          COALESCE(c.citas_no_asistencia, 0) AS citas_no_asistencia,
          ROUND(
            COALESCE(c.citas_atendidas, 0)
            / NULLIF(COALESCE(c.citas_atendidas, 0) + COALESCE(c.citas_canceladas, 0) + COALESCE(c.citas_no_asistencia, 0), 0),
            4
          ) AS tasa_asistencia,
          ROUND(COALESCE(c.citas_canceladas, 0) / NULLIF(COALESCE(c.total_citas, 0), 0), 4) AS tasa_cancelacion,
          COALESCE(f.ingreso_neto, 0) AS ingreso_neto,
          COALESCE(f.cartera_pendiente, 0) AS cartera_pendiente,
          ROUND(COALESCE(f.ingreso_neto, 0) / NULLIF(COALESCE(f.lineas_factura, 0), 0), 2) AS ticket_promedio
        FROM citas_mes c
        FULL OUTER JOIN fin_mes f ON c.anio = f.anio AND c.mes = f.mes
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW dim_paciente_bi AS
        SELECT
          sk_paciente,
          id_paciente,
          tipo_documento,
          numero_documento_hash,
          sexo,
          aseguradora,
          ciudad,
          estado_paciente,
          fecha_nacimiento,
          edad_anios
        FROM {dp}
        """
    )


def optimize_gold_tables(spark: SparkSession, config: LakehouseConfig) -> None:
    specs = [
        ("fact_citas", "sk_paciente, sk_fecha_cita"),
        ("fact_eventos_clinicos", "sk_paciente, sk_fecha_evento"),
        ("fact_facturacion", "sk_paciente, sk_fecha_factura"),
    ]
    for table, zorder in specs:
        fqn = full_table_name(config, "gold", table)
        spark.sql(f"OPTIMIZE {fqn} ZORDER BY ({zorder})")


@dataclass
class GoldRunResult:
    batch_id: str
    row_counts: dict[str, int]


def run_gold_pipeline(
    spark: SparkSession,
    *,
    batch_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    run_id: str | None = None,
    log_ops: bool = True,
    optimize: bool = True,
) -> GoldRunResult:
    from uuid import uuid4

    rid = run_id or str(uuid4())
    started_at = datetime.now(timezone.utc)
    status = "success"
    error_message = None
    counts: dict[str, int] = {}
    result: GoldRunResult | None = None

    try:
        pac = _silver(spark, config, "pacientes")
        cit = _silver(spark, config, "citas")
        evt = _silver(spark, config, "eventos_clinicos")
        fac = _silver(spark, config, "facturacion")

        dim_tiempo = build_dim_tiempo(spark, config)
        dim_paciente = build_dim_paciente(pac)
        dim_especialidad = build_dim_lookup(cit, "especialidad", "sk_especialidad")
        dim_sede = build_dim_lookup(cit, "sede", "sk_sede")
        dim_tipo_servicio = build_dim_lookup(fac, "tipo_servicio", "sk_tipo_servicio")
        dim_pagador = build_dim_lookup(fac, "pagador", "sk_pagador")
        dim_tipo_evento = build_dim_lookup(evt, "tipo_evento", "sk_tipo_evento")

        fact_citas = build_fact_citas(cit, dim_paciente, dim_especialidad, dim_sede)
        fact_eventos = build_fact_eventos(evt, dim_paciente, dim_tipo_evento)
        fact_facturacion = build_fact_facturacion(
            fac, dim_paciente, dim_tipo_servicio, dim_pagador
        )

        gold_tables = {
            "dim_tiempo": dim_tiempo,
            "dim_paciente": dim_paciente,
            "dim_especialidad": dim_especialidad,
            "dim_sede": dim_sede,
            "dim_tipo_servicio": dim_tipo_servicio,
            "dim_pagador": dim_pagador,
            "dim_tipo_evento": dim_tipo_evento,
            "fact_citas": fact_citas,
            "fact_eventos_clinicos": fact_eventos,
            "fact_facturacion": fact_facturacion,
        }

        for name, df in gold_tables.items():
            _write_gold(df, full_table_name(config, "gold", name))
            counts[name] = df.count()

        create_kpi_views(spark, config)

        if optimize:
            optimize_gold_tables(spark, config)

        result = GoldRunResult(batch_id=batch_id, row_counts=counts)
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        raise
    finally:
        if log_ops:
            try:
                log_pipeline_run(
                    spark,
                    run_id=rid,
                    batch_id=batch_id,
                    stage="gold",
                    status=status,
                    started_at=started_at,
                    ended_at=datetime.now(timezone.utc),
                    rows_in=sum(counts.values()) if counts else None,
                    rows_out=sum(counts.values()) if counts else None,
                    rows_rejected=0,
                    error_message=error_message,
                    config=config,
                )
            except Exception:
                pass

    assert result is not None
    return result
