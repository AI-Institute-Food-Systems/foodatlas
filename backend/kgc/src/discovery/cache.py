"""Shared caching logic for external API query results."""

import logging
from pathlib import Path

import pandas as pd

from ..utils.json_io import read_json, write_json

logger = logging.getLogger(__name__)


def load_cached(
    cache_dir: Path,
    filename: str,
) -> pd.DataFrame:
    """Load a cached JSON file if it exists, otherwise return empty DataFrame."""
    path = cache_dir / filename
    if path.exists():
        return pd.DataFrame(read_json(path))
    return pd.DataFrame()


def save_cached(
    df: pd.DataFrame,
    cache_dir: Path,
    filename: str,
) -> None:
    """Save a DataFrame as a cached JSON file."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    write_json(cache_dir / filename, df.to_dict(orient="records"))


def incremental_save(
    accumulated: pd.DataFrame,
    new_rows: list[dict],
    batch_names: list[str],
    search_term_col: str,
    cache_dir: Path,
    filename: str,
) -> tuple[pd.DataFrame, list[dict]]:
    """Append new rows to accumulated DataFrame and save to cache.

    Returns the updated accumulated DataFrame and an empty list (cleared batch).
    """
    new_df = pd.DataFrame(new_rows)
    new_df[search_term_col] = batch_names
    accumulated = pd.concat([accumulated, new_df], ignore_index=True)
    save_cached(accumulated, cache_dir, filename)
    return accumulated, []
