"""Reglas financieras — lógica pura (Q-S08)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd


def _ecuacion_ok(bruto, desc, neto, tol: Decimal = Decimal("0.01")) -> bool:
    return abs(Decimal(str(bruto)) - Decimal(str(desc)) - Decimal(str(neto))) <= tol


def test_facturacion_ecuacion_en_excel():
    path = Path(__file__).resolve().parents[2] / "facturacion.xlsx"
    if not path.exists():
        return
    df = pd.read_excel(path, engine="openpyxl")
    bad = [
        i
        for i, row in df.iterrows()
        if not _ecuacion_ok(row["valor_bruto"], row["valor_descuento"], row["valor_neto"])
    ]
    assert bad == [], f"Filas con ecuación inválida: {len(bad)}"


def test_facturacion_montos_no_negativos_en_excel():
    path = Path(__file__).resolve().parents[2] / "facturacion.xlsx"
    if not path.exists():
        return
    df = pd.read_excel(path, engine="openpyxl")
    for col in ("valor_bruto", "valor_descuento", "valor_neto"):
        assert (df[col] >= 0).all(), f"Montos negativos en {col}"


def test_pk_unica_facturacion():
    path = Path(__file__).resolve().parents[2] / "facturacion.xlsx"
    if not path.exists():
        return
    df = pd.read_excel(path, engine="openpyxl")
    assert df["id_factura"].is_unique
