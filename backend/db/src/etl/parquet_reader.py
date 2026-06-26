"""Read KGC parquet files into pandas DataFrames."""

import json
from pathlib import Path

import pandas as pd


def _parse_json_col(series: pd.Series) -> pd.Series:
    """Parse a JSON-encoded string column back to Python objects."""
    return series.apply(lambda x: json.loads(x) if isinstance(x, str) else x)


def _ensure_list_col(series: pd.Series) -> pd.Series:
    """Ensure column values are lists (not NaN, strings, or numpy arrays)."""

    def _to_list(x: object) -> list:
        if isinstance(x, list):
            return x
        if hasattr(x, "tolist"):  # numpy array or pandas-compatible sequence
            return list(x.tolist())
        return []

    return series.apply(_to_list)


def read_entities(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "entities.parquet")
    df["synonyms"] = _ensure_list_col(_parse_json_col(df["synonyms"]))
    df["external_ids"] = _parse_json_col(df["external_ids"]).apply(
        lambda x: x if isinstance(x, dict) else {}
    )
    df["scientific_name"] = df["scientific_name"].fillna("")
    if "attributes" in df.columns:
        df["attributes"] = _parse_json_col(df["attributes"]).apply(
            lambda x: x if isinstance(x, dict) else {}
        )
    else:
        df["attributes"] = [{}] * len(df)
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


def read_trust_signals(kg_dir: Path) -> pd.DataFrame | None:
    """Read trust_signals.parquet if present; return None when missing.

    Distinct from the other readers because trust signals are optional (the
    KGC trust stage is opt-in) and accumulate across runs rather than being
    rebuilt from scratch each ``db load``.
    """
    path = kg_dir / "trust_signals.parquet"
    if not path.exists() or path.stat().st_size == 0:
        return None
    df = pd.read_parquet(path)
    df["reason"] = df["reason"].fillna("")
    df["error_text"] = df["error_text"].fillna("")
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
