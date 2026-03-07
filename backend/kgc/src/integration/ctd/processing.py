"""Step 1: Load and filter raw CTD data."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .constants import (
    CTD_CHEMDIS_FILENAME,
    CTD_COLUMNS_WITH_LISTS,
    CTD_DIRECTEVIDENCE,
    CTD_DISEASE_FILENAME,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ...stores.entity_store import EntityStore


def load_ctd_data(
    ctd_dir: Path,
    dataset: str = "chemdis",
) -> pd.DataFrame:
    """Load a CTD CSV file, parsing the ``# Fields:`` header.

    Args:
        ctd_dir: Directory containing CTD CSV files.
        dataset: One of ``"chemdis"`` or ``"disease"``.

    Returns:
        DataFrame with parsed headers and list columns expanded.
    """
    filenames = {
        "chemdis": CTD_CHEMDIS_FILENAME,
        "disease": CTD_DISEASE_FILENAME,
    }
    file_path = ctd_dir / filenames[dataset]

    with file_path.open() as f:
        lines = f.readlines()
        fields_idx = next(
            i for i, line in enumerate(lines) if line.strip() == "# Fields:"
        )
        header_idx = fields_idx + 1
        header = lines[header_idx].strip().replace("# ", "").split(",")

    df = pd.read_csv(
        file_path,
        comment="#",
        skiprows=range(1, header_idx),
        names=header,
    )
    df = df.dropna(how="all").reset_index(drop=True)
    df = change_content_to_list(df)
    return df


def change_content_to_list(
    df: pd.DataFrame,
    splitby: str = "|",
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Split pipe-delimited columns into Python lists.

    Args:
        df: Input DataFrame.
        splitby: Delimiter string.
        columns: Columns to split (defaults to ``CTD_COLUMNS_WITH_LISTS``).

    Returns:
        DataFrame with specified columns converted to lists.
    """
    if columns is None:
        columns = CTD_COLUMNS_WITH_LISTS
    for column in columns:
        if column not in df.columns:
            continue
        df[column] = df[column].apply(
            lambda x: x.split(splitby) if pd.notnull(x) else []
        )
        df[column] = df[column].apply(
            lambda x: [int(i) if isinstance(i, str) and i.isdigit() else i for i in x]
        )
    return df


def filter_ctd_chemdis(
    ctd_chemdis: pd.DataFrame,
    entity_store: EntityStore,
) -> pd.DataFrame:
    """Filter CTD chemical-disease rows to those with direct evidence and in KG.

    Args:
        ctd_chemdis: Raw CTD chemical-disease DataFrame.
        entity_store: EntityStore to check MeSH ID membership.

    Returns:
        Filtered DataFrame.
    """
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True
    )
    mesh_fa: set[str] = set()
    for _, row in entity_store._entities.iterrows():
        if "mesh" in row["external_ids"]:
            mesh_fa.update(str(m) for m in row["external_ids"]["mesh"])
    ctd_chemdis = ctd_chemdis[ctd_chemdis["ChemicalID"].isin(mesh_fa)].reset_index(
        drop=True
    )
    return ctd_chemdis


def extract_pubmed_ids(ctd_chemdis: pd.DataFrame) -> set[int]:
    """Extract all unique PubMed IDs from filtered CTD data.

    Args:
        ctd_chemdis: Filtered CTD DataFrame with ``PubMedIDs`` list column.

    Returns:
        Set of unique integer PubMed IDs.
    """
    pubmed_ids: set[int] = set()
    for pids in ctd_chemdis["PubMedIDs"]:
        pubmed_ids.update(int(p) for p in pids if p)
    return pubmed_ids
