"""
Validación E2E en pandas (sin Databricks): reglas Silver/Gold y conteos esperados.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _invalid_pk(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return series.isna() | s.eq("") | s.str.lower().isin(["nan", "none", "nat", "null"])


def main() -> int:
    pac = pd.read_excel(ROOT / "pacientes.xlsx", engine="openpyxl")
    cit = pd.read_excel(ROOT / "citas.xlsx", engine="openpyxl")
    evt = pd.read_excel(ROOT / "eventos_clinicos.xlsx", engine="openpyxl")
    fac = pd.read_excel(ROOT / "facturacion.xlsx", engine="openpyxl")

    errors: list[str] = []

    # Bronze expectations
    for name, df, pk in [
        ("pacientes", pac, "id_paciente"),
        ("citas", cit, "id_cita"),
        ("eventos", evt, "id_evento"),
        ("facturacion", fac, "id_factura"),
    ]:
        if len(df) == 0:
            errors.append(f"{name} vacío")
        if _invalid_pk(df[pk]).any():
            errors.append(f"{name} PK inválida")

    # Silver-like rules
    from datetime import time as dt_time

    bad_fecha = pac["fecha_nacimiento"].apply(lambda x: isinstance(x, dt_time)).sum()
    if bad_fecha != 1:
        errors.append(f"pacientes fecha inválida: expected 1 got {bad_fecha}")

    silver_pac = len(pac) - bad_fecha
    if cit["id_paciente"].isin(pac["id_paciente"]).all() is False:
        errors.append("citas FK paciente")
    if not cit["id_paciente"].isin(pac["id_paciente"]).all():
        errors.append("citas huérfanas paciente")
    if not evt["id_cita"].isin(cit["id_cita"]).all():
        errors.append("eventos huérfanos cita")
    if not fac["id_cita"].isin(cit["id_cita"]).all():
        errors.append("facturas huérfanas cita")

    m = evt.merge(cit[["id_cita", "id_paciente"]], on="id_cita", suffixes=("_e", "_c"))
    if (m["id_paciente_e"] != m["id_paciente_c"]).any():
        errors.append("evento paciente incoherente con cita")

    for _, row in fac.iterrows():
        if round(Decimal(str(row["valor_bruto"])) - Decimal(str(row["valor_descuento"])), 2) != round(
            Decimal(str(row["valor_neto"])), 2
        ):
            errors.append("facturación ecuación")
            break

    expected = {
        "bronze_pacientes": 223,
        "silver_pacientes": silver_pac,
        "silver_citas": 1001,
        "silver_eventos": 1602,
        "silver_facturacion": 1202,
        "gold_fact_citas": 1001,
    }
    actual = {
        "bronze_pacientes": len(pac),
        "silver_pacientes": silver_pac,
        "silver_citas": len(cit),
        "silver_eventos": len(evt),
        "silver_facturacion": len(fac),
        "gold_fact_citas": len(cit),
    }
    for k, exp in expected.items():
        if actual[k] != exp:
            errors.append(f"{k}: {actual[k]} != {exp}")

    print("=== Validación pandas E2E ===")
    for k, v in actual.items():
        print(f"  {k}: {v}")

    if errors:
        print("\nFALLOS:")
        for e in sorted(set(errors)):
            print(f"  - {e}")
        return 1

    print("\nOK — Reglas de negocio y conteos alineados con pipeline esperado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
