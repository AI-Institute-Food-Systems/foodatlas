"""MeSH-based chemical categorisation using tree numbers."""

import logging
from itertools import chain
from pathlib import Path

import pandas as pd

from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def load_mesh_categories(settings: KGCSettings) -> pd.DataFrame:
    """Load or build MeSH categories from pre-processed descriptor/supplementary data.

    Returns a DataFrame with columns:
        id, name, synonyms, tree_numbers,
        primary_tree_numbers, secondary_tree_numbers,
        primary_category, secondary_category
    """
    dp_dir = Path(settings.data_cleaning_dir)
    cache_path = dp_dir / "mesh_categories.parquet"

    if cache_path.exists():
        result: pd.DataFrame = pd.read_parquet(cache_path)
        return result

    meshd = pd.read_parquet(dp_dir / "mesh_desc_cleaned.parquet")
    meshs = pd.read_parquet(dp_dir / "mesh_supp_cleaned.parquet")

    meshd = _filter_chemical_trees(meshd)
    meshd = _add_tree_levels(meshd)
    meshd_indexed = meshd.set_index("id")

    meshs = _process_supplementary(meshs, meshd_indexed)

    mesh = pd.concat([meshd, meshs], ignore_index=True)
    tree_map = _build_tree_number_to_category(meshd)
    mesh = _assign_categories(mesh, tree_map)

    mesh.to_parquet(cache_path)
    logger.info("Built MeSH categories: %d entries.", len(mesh))
    return mesh


def _filter_chemical_trees(meshd: pd.DataFrame) -> pd.DataFrame:
    """Keep only tree numbers starting with 'D' (chemical/drug branch)."""
    meshd = meshd.copy()
    meshd["tree_numbers"] = meshd["tree_numbers"].apply(
        lambda x: [t for t in (x if isinstance(x, list) else [x]) if t.startswith("D")]
    )
    return meshd[meshd["tree_numbers"].apply(len) > 0].copy()


def _add_tree_levels(meshd: pd.DataFrame) -> pd.DataFrame:
    """Extract primary (level-1) and secondary (level-2) tree numbers."""
    meshd = meshd.copy()

    def _split_levels(tree_numbers: list[str]) -> tuple[list[str], list[str]]:
        primary: set[str] = set()
        secondary: set[str] = set()
        for tn in tree_numbers:
            parts = tn.split(".")
            primary.add(parts[0])
            if len(parts) > 1:
                secondary.add(f"{parts[0]}.{parts[1]}")
        return sorted(primary), sorted(secondary)

    levels = meshd["tree_numbers"].apply(_split_levels).apply(pd.Series)
    meshd[["primary_tree_numbers", "secondary_tree_numbers"]] = levels
    return meshd


def _process_supplementary(
    meshs: pd.DataFrame,
    meshd_indexed: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve supplementary records to descriptor tree numbers."""
    meshs = meshs.copy()
    meshs["mapped_to"] = meshs["mapped_to"].apply(lambda x: [m.lstrip("*") for m in x])
    valid_ids = set(meshd_indexed.index)
    meshs["mapped_to"] = meshs["mapped_to"].apply(
        lambda x: [m for m in x if m in valid_ids]
    )
    meshs = meshs[meshs["mapped_to"].apply(len) > 0].copy()

    def _resolve(mapped_to: list[str]) -> tuple[list[str], list[str]]:
        rows = meshd_indexed.loc[mapped_to]
        primary = sorted(set(chain.from_iterable(rows["primary_tree_numbers"])))
        secondary = sorted(set(chain.from_iterable(rows["secondary_tree_numbers"])))
        return primary, secondary

    levels = meshs["mapped_to"].apply(_resolve).apply(pd.Series)
    meshs[["primary_tree_numbers", "secondary_tree_numbers"]] = levels
    return meshs


def _build_tree_number_to_category(meshd: pd.DataFrame) -> pd.DataFrame:
    """Map tree numbers to MeSH descriptor names."""
    all_tree_numbers = (
        meshd["primary_tree_numbers"].explode().dropna().unique().tolist()
        + meshd["secondary_tree_numbers"].explode().dropna().unique().tolist()
    )
    exploded = meshd.explode("tree_numbers").set_index("tree_numbers")
    return exploded.loc[exploded.index.intersection(all_tree_numbers), ["id", "name"]]


def _assign_categories(
    mesh: pd.DataFrame,
    tree_map: pd.DataFrame,
) -> pd.DataFrame:
    """Assign primary and secondary category names from tree numbers."""
    mesh = mesh.copy()

    def _map_row(row: pd.Series) -> tuple[list[str], list[str]]:
        p_tn = row["primary_tree_numbers"]
        s_tn = row["secondary_tree_numbers"]
        p_cat = tree_map.loc[tree_map.index.intersection(p_tn), "name"].tolist()
        s_cat = tree_map.loc[tree_map.index.intersection(s_tn), "name"].tolist()
        return p_cat, s_cat

    cats = mesh.apply(_map_row, axis=1).apply(pd.Series)
    mesh[["primary_category", "secondary_category"]] = cats
    return mesh


def assign_mesh_groups(
    chemicals: pd.DataFrame,
    mesh: pd.DataFrame,
) -> pd.DataFrame:
    """Assign MeSH tree-number categories to chemical entities.

    Args:
        chemicals: Entity DataFrame filtered to ``entity_type == 'chemical'``.
        mesh: MeSH categories DataFrame (from :func:`load_mesh_categories`).

    Returns:
        DataFrame with columns ``mesh_lvl1`` and ``mesh_lvl2``.
    """
    mesh_indexed = mesh.set_index("id")
    chemicals = chemicals.copy()
    chemicals["mesh_id"] = chemicals["external_ids"].apply(lambda x: x.get("mesh", []))

    def _lookup(mesh_ids: list[str]) -> tuple[list[str], list[str]]:
        if not mesh_ids or mesh_ids[0] not in mesh_indexed.index:
            return [], []
        row = mesh_indexed.loc[mesh_ids[0]]
        return row["primary_category"], row["secondary_category"]

    results = chemicals["mesh_id"].apply(_lookup).apply(pd.Series)
    results.columns = pd.Index(["mesh_lvl1", "mesh_lvl2"])
    results.index = chemicals.index
    return results
