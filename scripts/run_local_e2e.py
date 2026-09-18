"""
Ejecuta pipeline E2E en local cuando PySpark+Delta están disponibles.
En Windows sin winutils usa modo Parquet (full) equivalente funcional.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

_HADOOP_HOME = ROOT / "tools" / "hadoop"


def _configure_windows_hadoop() -> None:
    if os.name != "nt":
        return
    hadoop = _HADOOP_HOME.resolve()
    winutils = hadoop / "bin" / "winutils.exe"
    if winutils.is_file():
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["hadoop.home.dir"] = str(hadoop)
        bin_dir = str(hadoop / "bin")
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def _patch_delta_format_to_parquet() -> None:
    """Redirige .format('delta') → parquet (cuarentena, ops, calidad, etc.)."""
    from pyspark.sql.readwriter import DataFrameWriter

    _orig_format = DataFrameWriter.format

    def _format(self, source: str):
        if source == "delta":
            source = "parquet"
        return _orig_format(self, source)

    DataFrameWriter.format = _format  # type: ignore[method-assign]


def _patch_parquet_writes() -> None:
    """Evita Delta en local: tablas Hive Parquet (solo load_mode=full)."""
    _patch_delta_format_to_parquet()
    import ips_analytics.bronze.ingest_excel as bronze_mod
    import ips_analytics.gold.build_star_schema as gold_mod
    import ips_analytics.silver.common as silver_mod

    def parquet_write(df, table_fqn, mode="overwrite"):
        (
            df.write.format("parquet")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_fqn)
        )

    def noop_optimize(spark, config):
        return None

    bronze_mod.write_delta_table = lambda df, t, mode="overwrite": parquet_write(df, t, mode)
    bronze_mod.merge_delta_table = lambda df, t, pk: parquet_write(df, t, "overwrite")
    silver_mod.write_silver = lambda df, t: parquet_write(df, t, "overwrite")
    silver_mod.merge_silver = lambda df, t, pk: parquet_write(df, t, "overwrite")
    silver_mod.write_or_merge_silver = lambda df, t, pk, load_mode: parquet_write(
        df, t, "overwrite" if load_mode == "full" else "append"
    )
    gold_mod.optimize_gold_tables = noop_optimize
    gold_mod._write_gold = lambda df, table_fqn: parquet_write(df, table_fqn, "overwrite")


def build_spark(warehouse: str):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # local[1] evita resets de socket Python worker en Windows.
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ips-analytics-local-e2e")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.python.worker.reuse", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    return builder.getOrCreate()


def ensure_databases(spark) -> None:
    for db in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def _run_pytest() -> int:
    import subprocess

    r = subprocess.run(
        [sys.executable, "-m", "pytest", str(ROOT / "src" / "tests"), "-q"],
        cwd=str(ROOT),
    )
    return int(r.returncode)


def _spark_python_worker_ok(spark) -> bool:
    """En algunos Windows el worker Python de Spark falla; solo JVM (spark.range) funciona."""
    from pyspark.sql.types import StructField, StringType, StructType

    try:
        spark.range(1).count()
        schema = StructType([StructField("x", StringType(), True)])
        spark.createDataFrame([("ok",)], schema=schema).limit(1).count()
        return True
    except Exception:
        return False


def _run_pandas_validate() -> int:
    import importlib.util

    path = ROOT / "scripts" / "validate_pipeline_pandas.py"
    spec = importlib.util.spec_from_file_location("validate_pipeline_pandas", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return int(mod.main())


def main() -> int:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    _configure_windows_hadoop()

    print(">>> 1/4 Validación pandas (reglas de negocio)")
    if _run_pandas_validate() != 0:
        return 1

    print("\n>>> 2/4 pytest (contratos y reglas unitarias)")
    if _run_pytest() != 0:
        return 1

    data_dir = ROOT
    required = ["pacientes.xlsx", "citas.xlsx", "eventos_clinicos.xlsx", "facturacion.xlsx"]
    for name in required:
        if not (data_dir / name).exists():
            print(f"ERROR: falta {name}")
            return 1

    print("\n>>> 3/4 Comprobación entorno PySpark")
    _patch_parquet_writes()
    warehouse = tempfile.mkdtemp(prefix="ips_spark_warehouse_", dir=tempfile.gettempdir())
    spark = build_spark(warehouse)
    spark.sparkContext.setLogLevel("WARN")

    if not _spark_python_worker_ok(spark):
        spark.stop()
        print(
            "\nAVISO: PySpark local no puede ejecutar el pipeline (worker Python en este equipo).\n"
            "Validación local OK con pandas + pytest.\n"
            "Ejecuta en Databricks: notebooks/99_orchestration.ipynb (Delta, load_mode=full).\n"
            "Opcional Windows: scripts/setup_windows_spark.ps1 y Python 3.12; reintenta este script."
        )
        return 0

    print("\n>>> 4/4 Pipeline PySpark local (Parquet, full)")
    ensure_databases(spark)

    from ips_analytics.config import LOCAL_SPARK_CONFIG, generate_batch_id
    from ips_analytics.pipeline.orchestrate import run_end_to_end_pipeline
    from ips_analytics.quality.runner import assert_quality_passed

    raw_path = str(data_dir).replace("\\", "/") + "/"
    batch_id = generate_batch_id()

    try:
        result = run_end_to_end_pipeline(
            spark,
            raw_volume_path=raw_path,
            batch_id=batch_id,
            load_mode="full",
            run_gold=True,
            run_quality=True,
            fail_on_quality=True,
            config=LOCAL_SPARK_CONFIG,
        )
    except Exception as exc:
        print(f"ERROR PySpark E2E: {exc}")
        spark.stop()
        return 1

    print("\n=== Pipeline PySpark ===")
    print(f"batch_id={result.batch_id}")
    if result.quality:
        for line in result.quality.summary_lines():
            print(line)
        try:
            assert_quality_passed(result.quality)
        except Exception as exc:
            print(f"QC falló: {exc}")
            spark.stop()
            return 1

    counts = {
        "bronze.pacientes": spark.table("bronze.pacientes").count(),
        "silver.pacientes": spark.table("silver.pacientes").count(),
        "silver.citas": spark.table("silver.citas").count(),
        "gold.fact_citas": spark.table("gold.fact_citas").count(),
        "silver.rejects": spark.table("silver.rejects").count(),
    }
    spark.stop()

    print("\n--- Conteos PySpark ---")
    for k, v in counts.items():
        print(f"  {k}: {v}")

    expected = {
        "bronze.pacientes": 223,
        "silver.pacientes": 222,
        "silver.citas": 1001,
        "gold.fact_citas": 1001,
        "silver.rejects": 1,
    }
    errors = [f"{k}: {counts[k]} != {exp}" for k, exp in expected.items() if counts[k] != exp]
    if errors:
        print("\nFALLOS conteos:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("\nOK — Validación local completa (pandas + PySpark).")
    print("En Databricks ejecutar notebooks/99_orchestration.ipynb con Delta.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
