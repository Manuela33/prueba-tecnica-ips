"""Validación lógica fecha_nacimiento (1 anomalía esperada en baseline)."""

from __future__ import annotations

from datetime import datetime, time
from pathlib import Path

import pandas as pd


def _fecha_invalida(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, time):
        return True
    if isinstance(value, datetime):
        return False
    s = str(value).strip()
    if s.lower() in ("", "nan", "none", "nat"):
        return False
    try:
        pd.to_datetime(s)
        return False
    except Exception:
        return True


def test_pacientes_fecha_anomaly_count():
    path = Path(__file__).resolve().parents[2] / "pacientes.xlsx"
    if not path.exists():
        return
    df = pd.read_excel(path, engine="openpyxl")
    invalid = df["fecha_nacimiento"].map(_fecha_invalida).sum()
    assert invalid == 1, f"Se esperaba 1 fecha inválida, encontrado {invalid}"
