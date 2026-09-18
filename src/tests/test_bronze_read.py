"""Tests de lectura Bronze (sin Spark)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PK_BY_FILE = {
    "pacientes.xlsx": "id_paciente",
    "citas.xlsx": "id_cita",
    "eventos_clinicos.xlsx": "id_evento",
    "facturacion.xlsx": "id_factura",
}


def _is_invalid_pk(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return series.isna() | s.eq("") | s.str.lower().isin(["nan", "none", "nat", "null"])


def test_excel_pk_completeness():
    """Baseline: ninguna fila con PK inválida (quarantine vacía en full load)."""
    for file_name, pk in PK_BY_FILE.items():
        path = ROOT / file_name
        if not path.exists():
            continue
        df = pd.read_excel(path, engine="openpyxl")
        invalid = _is_invalid_pk(df[pk]).sum()
        assert invalid == 0, f"{file_name}: {invalid} PK inválidas"
