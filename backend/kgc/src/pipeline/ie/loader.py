"""Load and standardize IE raw TSV into MetadataContains-compatible rows."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from tqdm import tqdm

from ...stores.schema import FILE_IE_CONC_ERRORS, FILE_IE_PARSE_ERRORS
from .conc_parser import convert_conc, parse_conc
from .constants import GREEK_LETTERS, PUNCTUATIONS

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_RAW_COLUMNS = ["pmcid", "section", "matched_query", "sentence", "prob", "response"]


def standardize_name(name: str) -> str:
    """Normalize Greek Unicode → English and clean punctuation."""
    for eng, variants in GREEK_LETTERS.items():
        for ch in variants:
            name = name.replace(ch, eng)
    for canonical, variants in PUNCTUATIONS.items():
        for ch in variants:
            name = name.replace(ch, canonical)
    return name.strip().lower()


def _parse_tuple(line: str) -> tuple[str, str, str, str] | None:
    """Extract (food, food_part, chemical, quantity) from a response line.

    Chemical names may contain commas (e.g. ``3,4-dihydroxyphenylethanol``),
    so we split on the first two commas and the last comma.
    """
    inner = line.strip().lstrip("(").rstrip(")")
    if not inner:
        return None

    parts = inner.split(",", 2)
    if len(parts) < 3:
        return None

    food = parts[0].strip()
    food_part = parts[1].strip()
    rest = parts[2]

    last_comma = rest.rfind(",")
    if last_comma == -1:
        chemical = rest.strip()
        quantity = ""
    else:
        chemical = rest[:last_comma].strip()
        quantity = rest[last_comma + 1 :].strip()

    # Take first pipe-separated alias as the primary name.
    if "|" in chemical:
        chemical = chemical.split("|")[0].strip()

    if not food or not chemical:
        return None

    return food, food_part, chemical, quantity


def _process_line(
    line: str,
    rec: pd.Series,
    rows: list[dict],
    parse_errors: list[dict],
    conc_errors: list[dict],
    *,
    method: str,
) -> None:
    """Parse one response line into a row dict, or log an error."""
    parsed = _parse_tuple(line)
    if parsed is None:
        parse_errors.append(
            {"pmcid": rec["pmcid"], "line": line.strip(), "reason": "bad_tuple"}
        )
        return
    food, food_part, chemical, quantity = parsed
    conc_value_raw, conc_unit_raw = _parse_quantity(quantity, rec, parse_errors)
    conc_value_converted, conc_unit_converted = _convert_quantity(
        conc_value_raw, conc_unit_raw, rec, conc_errors
    )
    ref = json.dumps({"pmcid": rec["pmcid"], "text": rec.get("sentence", "")})
    rows.append(
        {
            "source_type": "pubmed",
            "reference": ref,
            "source": f"lit2kg:{method}",
            "head_name_raw": standardize_name(food),
            "tail_name_raw": standardize_name(chemical),
            "conc_value": conc_value_converted,
            "conc_unit": conc_unit_converted,
            "conc_value_raw": conc_value_raw,
            "conc_unit_raw": conc_unit_raw,
            "food_part": food_part.strip().lower(),
            "food_processing": "",
            "filter_score": float(rec["prob"]),
            "_food_name": standardize_name(food),
            "_chemical_name": standardize_name(chemical),
        }
    )


def _parse_quantity(
    quantity: str, rec: pd.Series, parse_errors: list[dict]
) -> tuple[str, str]:
    """Parse a raw quantity string, logging errors."""
    if not quantity:
        return "", ""
    conc_result = parse_conc(quantity)
    if conc_result is None:
        parse_errors.append(
            {"pmcid": rec["pmcid"], "line": quantity, "reason": "bad_conc"}
        )
        return "", ""
    return conc_result


def _convert_quantity(
    value_raw: str,
    unit_raw: str,
    rec: pd.Series,
    conc_errors: list[dict],
) -> tuple[float | None, str]:
    """Convert parsed concentration to mg/100g, logging failures."""
    if not value_raw or not unit_raw:
        return None, ""
    converted = convert_conc(value_raw, unit_raw)
    if converted is not None:
        return converted
    conc_errors.append(
        {
            "pmcid": rec["pmcid"],
            "value_raw": value_raw,
            "unit_raw": unit_raw,
        }
    )
    return None, ""


def _load_json(path: Path) -> pd.DataFrame:
    """Load extraction JSON into a DataFrame with standard columns."""
    with path.open(encoding="utf-8") as f:
        data: dict[str, dict[str, object]] = json.load(f)
    rows: list[dict[str, object]] = []
    for entry in data.values():
        rows.append(
            {
                "pmcid": str(entry["pmcid"]),
                "section": entry.get("section", ""),
                "matched_query": entry.get("matched_query", ""),
                "sentence": entry.get("text", ""),
                "prob": entry.get("prob", 0.0) or 0.0,
                "response": entry.get("response", ""),
            }
        )
    return pd.DataFrame(rows, columns=_RAW_COLUMNS)


def load_ie_raw(path: Path, output_dir: Path, *, method: str) -> pd.DataFrame:
    """Parse raw IE file (JSON, TSV, or parquet) into a DataFrame.

    Args:
        path: Path to the raw IE file.
        output_dir: Directory for diagnostics output.
        method: IE method name (e.g. "gpt-4"), used in the attestation source field.

    Returns:
        DataFrame with evidence + extraction columns.
    """
    suffix = path.suffix.lower()
    if suffix == ".json":
        raw = _load_json(path)
    elif suffix == ".parquet":
        raw = pd.read_parquet(path)
    else:
        raw = pd.read_csv(path, sep="\t", dtype={"pmcid": str})
    missing = set(_RAW_COLUMNS) - set(raw.columns)
    if missing:
        msg = f"IE file missing columns: {missing}"
        raise ValueError(msg)

    logger.info("IE raw: %d rows from %s.", len(raw), path)

    rows: list[dict] = []
    parse_errors: list[dict] = []
    conc_errors: list[dict] = []
    for _, rec in tqdm(raw.iterrows(), total=len(raw), desc="parsing IE", leave=True):
        response = rec["response"]
        if not isinstance(response, str):
            parse_errors.append(
                {"pmcid": rec["pmcid"], "line": str(response), "reason": "not_string"}
            )
            continue
        for line in response.split("\n"):
            _process_line(line, rec, rows, parse_errors, conc_errors, method=method)

    if parse_errors:
        errors_path = output_dir / FILE_IE_PARSE_ERRORS
        errors_path.parent.mkdir(exist_ok=True)
        pd.DataFrame(parse_errors).to_csv(errors_path, sep="\t", index=False)
        logger.warning("%d parse errors written to %s.", len(parse_errors), errors_path)
    if conc_errors:
        conc_path = output_dir / FILE_IE_CONC_ERRORS
        conc_path.parent.mkdir(exist_ok=True)
        pd.DataFrame(conc_errors).to_csv(conc_path, sep="\t", index=False)
        logger.warning(
            "%d unconverted concentrations written to %s.", len(conc_errors), conc_path
        )
    logger.info("Parsed %d IE tuples from %s.", len(rows), path)

    return pd.DataFrame(rows) if rows else pd.DataFrame()
