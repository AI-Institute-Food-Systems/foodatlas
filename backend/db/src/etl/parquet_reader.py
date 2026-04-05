"""Read KGC parquet files into pandas DataFrames."""

import json
from pathlib import Path

import pandas as pd


def _parse_json_col(series: pd.Series) -> pd.Series:
    """Parse a JSON-encoded string column back to Python objects."""
    return series.apply(lambda x: json.loads(x) if isinstance(x, str) else x)


def _ensure_list_col(series: pd.Series) -> pd.Series:
    """Ensure column values are lists (not NaN or strings)."""
    return series.apply(lambda x: x if isinstance(x, list) else [])


def read_entities(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "entities.parquet")
    df["synonyms"] = _ensure_list_col(_parse_json_col(df["synonyms"]))
    df["external_ids"] = _parse_json_col(df["external_ids"]).apply(
        lambda x: x if isinstance(x, dict) else {}
    )
    df["scientific_name"] = df["scientific_name"].fillna("")
    return df


def read_relationships(kg_dir: Path) -> pd.DataFrame:
    return pd.read_parquet(kg_dir / "relationships.parquet")


def read_triplets(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "triplets.parquet")
    df["attestation_ids"] = _ensure_list_col(_parse_json_col(df["attestation_ids"]))
    df["source"] = df["source"].fillna("")
    return df


def read_evidence(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "evidence.parquet")
    df["reference"] = _parse_json_col(df["reference"]).apply(
        lambda x: x if isinstance(x, dict) else {}
    )
    return df


def read_attestations(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "attestations.parquet")
    for col in (
        "head_name_raw",
        "tail_name_raw",
        "conc_unit",
        "conc_value_raw",
        "conc_unit_raw",
        "food_part",
        "food_processing",
    ):
        df[col] = df[col].fillna("")
    df["head_candidates"] = _ensure_list_col(_parse_json_col(df["head_candidates"]))
    df["tail_candidates"] = _ensure_list_col(_parse_json_col(df["tail_candidates"]))
    df["validated"] = df["validated"].fillna(False).astype(bool)
    df["validated_correct"] = df["validated_correct"].fillna(True).astype(bool)
    return df
