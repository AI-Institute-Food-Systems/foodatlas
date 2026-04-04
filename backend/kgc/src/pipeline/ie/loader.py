"""Load and standardize IE raw TSV into MetadataContains-compatible rows."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd
from tqdm import tqdm

from ...stores.schema import FILE_IE_PARSE_ERRORS
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


def load_ie_raw(path: Path, output_dir: Path) -> pd.DataFrame:
    """Parse raw IE file (TSV or pkl) into a MetadataContains-compatible DataFrame.

    Returns:
        DataFrame with evidence + extraction columns.
    """
    if str(path).endswith(".parquet"):
        raw = pd.read_parquet(path)
    else:
        raw = pd.read_csv(path, sep="\t", dtype={"pmcid": str})
    missing = set(_RAW_COLUMNS) - set(raw.columns)
    if missing:
        msg = f"IE TSV missing columns: {missing}"
        raise ValueError(msg)

    logger.info("IE raw: %d rows from %s.", len(raw), path)

    rows: list[dict] = []
    parse_errors: list[dict] = []
    for _, rec in tqdm(raw.iterrows(), total=len(raw), desc="parsing IE", leave=True):
        response = rec["response"]
        if not isinstance(response, str):
            parse_errors.append(
                {"pmcid": rec["pmcid"], "line": str(response), "reason": "not_string"}
            )
            continue
        for line in response.split("\n"):
            parsed = _parse_tuple(line)
            if parsed is None:
                parse_errors.append(
                    {"pmcid": rec["pmcid"], "line": line.strip(), "reason": "bad_tuple"}
                )
                continue
            food, food_part, chemical, _quantity = parsed
            ref = json.dumps({"pmcid": rec["pmcid"], "text": rec.get("sentence", "")})
            rows.append(
                {
                    # Evidence fields
                    "source_type": "pubmed",
                    "reference": ref,
                    # Extraction fields
                    "extractor": "lit2kg",
                    "head_name_raw": standardize_name(food),
                    "tail_name_raw": standardize_name(chemical),
                    "conc_value": None,
                    "conc_unit": "",
                    "food_part": food_part.strip().lower(),
                    "food_processing": "",
                    "quality_score": float(rec["prob"]),
                    # Kept for IE resolver (name lookup)
                    "_food_name": standardize_name(food),
                    "_chemical_name": standardize_name(chemical),
                }
            )

    if parse_errors:
        errors_path = output_dir / FILE_IE_PARSE_ERRORS
        errors_path.parent.mkdir(exist_ok=True)
        pd.DataFrame(parse_errors).to_csv(errors_path, sep="\t", index=False)
        logger.warning("%d parse errors written to %s.", len(parse_errors), errors_path)
    logger.info("Parsed %d IE tuples from %s.", len(rows), path)

    return pd.DataFrame(rows) if rows else pd.DataFrame()
