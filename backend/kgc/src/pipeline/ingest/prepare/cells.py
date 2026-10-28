"""Parsing for the JSON-valued cells of the delivery CSVs and the KG snapshot."""

from __future__ import annotations

import ast
import json

import pandas as pd


def as_dict(cell: object) -> dict:
    if isinstance(cell, dict):
        return cell
    if isinstance(cell, str) and cell.strip():
        value = json.loads(cell)
        return value if isinstance(value, dict) else {}
    return {}


def as_list(cell: object) -> list:
    """A JSON array, or the Python-literal list older snapshots wrote."""
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            value = json.loads(cell)
        except json.JSONDecodeError:
            value = ast.literal_eval(cell)
        return value if isinstance(value, list) else []
    return []


def first(values: list) -> str:
    return str(values[0]) if values else ""


def int_str(value: object) -> str:
    """A numeric id as a plain integer string (``75142.0`` → ``75142``)."""
    if value is None or value == "" or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return str(int(float(str(value))))


def blank_if_na(value: object) -> str:
    return "" if pd.isna(value) else str(value)
