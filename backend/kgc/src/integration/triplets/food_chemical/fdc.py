"""FDC (Food Data Central) integration — merge nutrient data into KG."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ....constructor.knowledge_graph import KnowledgeGraph
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)


def build_fdc_nutrient_id_map(entities: pd.DataFrame) -> dict[int, str]:
    """Map FDC nutrient IDs to FoodAtlas entity IDs.

    Args:
        entities: DataFrame with ``external_ids`` column (indexed by foodatlas_id).

    Returns:
        Mapping from integer FDC nutrient ID to foodatlas entity ID string.

    Raises:
        ValueError: If a duplicate FDC nutrient ID is found.
    """
    fdcn2fa: dict[int, str] = {}
    for entity_id, row in entities.iterrows():
        for fdcn_id in row["external_ids"].get("fdc_nutrient", []):
            key = int(fdcn_id)
            if key in fdcn2fa:
                msg = f"Duplicate FDC nutrient ID: {fdcn_id}"
                raise ValueError(msg)
            fdcn2fa[key] = str(entity_id)
    return fdcn2fa


def build_fdc_id_map(entities: pd.DataFrame) -> dict[int, str]:
    """Map FDC food IDs to FoodAtlas entity IDs.

    Args:
        entities: DataFrame with ``external_ids`` column (indexed by foodatlas_id).

    Returns:
        Mapping from integer FDC ID to foodatlas entity ID string.

    Raises:
        ValueError: If a duplicate FDC ID is found.
    """
    fdc2fa: dict[int, str] = {}
    for entity_id, row in entities.iterrows():
        for fdc_id in row["external_ids"].get("fdc", []):
            key = int(fdc_id)
            if key in fdc2fa:
                msg = f"Duplicate FDC ID: {fdc_id}"
                raise ValueError(msg)
            fdc2fa[key] = str(entity_id)
    return fdc2fa


def load_fdc_nutrients(fdc_dir: Path) -> pd.DataFrame:
    """Load and filter FDC nutrient data to foundation foods only.

    Args:
        fdc_dir: Path to FDC data directory containing CSV files.

    Returns:
        DataFrame with columns ``id``, ``fdc_id``, ``nutrient_id``, ``amount``.
    """
    food_nutrient = pd.read_csv(
        fdc_dir / "food_nutrient.csv",
        usecols=["id", "fdc_id", "nutrient_id", "amount"],
    )
    fdc_ids_ff = pd.read_csv(fdc_dir / "foundation_food.csv")["fdc_id"]
    return food_nutrient[food_nutrient["fdc_id"].isin(fdc_ids_ff)].reset_index(
        drop=True
    )


def build_fdc_metadata(
    fdc_data: pd.DataFrame,
    fdc_nutrients: pd.DataFrame,
    fdc2fa: dict[int, str],
    fdcn2fa: dict[int, str],
) -> pd.DataFrame:
    """Build metadata rows from FDC nutrient data.

    Args:
        fdc_data: Filtered FDC nutrient DataFrame.
        fdc_nutrients: Nutrient reference table (indexed by nutrient id).
        fdc2fa: FDC food ID → FoodAtlas entity ID mapping.
        fdcn2fa: FDC nutrient ID → FoodAtlas entity ID mapping.

    Returns:
        DataFrame with metadata columns ready for MetadataContainsStore.
    """
    rows: list[dict] = []
    for _, row in fdc_data.iterrows():
        fdc_id = int(row["fdc_id"])
        nutrient_id = int(row["nutrient_id"])
        if fdc_id not in fdc2fa or nutrient_id not in fdcn2fa:
            continue
        conc_value = row["amount"]
        unit_name = fdc_nutrients.loc[nutrient_id, "unit_name"].lower()
        conc_unit = f"{unit_name}/100g"
        rows.append(
            {
                "_food_name": f"FDC:{fdc_id}",
                "_chemical_name": f"FDC_NUTRIENT:{nutrient_id}",
                "_conc": f"{conc_value} {conc_unit}",
                "_food_part": "",
                "source": "fdc",
                "reference": [
                    f"https://fdc.nal.usda.gov/fdc-app.html"
                    f"#/food-details/{fdc_id}/nutrients"
                ],
                "entity_linking_method": "id_matching",
            }
        )
    return pd.DataFrame(rows)


def merge_fdc(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Merge FDC nutrient data into the knowledge graph.

    Args:
        kg: Loaded KnowledgeGraph instance.
        settings: KGCSettings with ``data_dir`` pointing to data root.
    """
    fdc_dir = Path(settings.data_dir) / "FDC"
    fdc_subdir = next(fdc_dir.glob("FoodData_Central_*"), None)
    if fdc_subdir is None:
        msg = f"No FDC data directory found in {fdc_dir}"
        raise FileNotFoundError(msg)

    entities = kg.entities._entities
    fdc2fa = build_fdc_id_map(entities)
    fdcn2fa = build_fdc_nutrient_id_map(entities)

    fdc_data = load_fdc_nutrients(fdc_subdir)
    fdc_nutrients = pd.read_csv(
        fdc_subdir / "nutrient.csv",
        usecols=["id", "name", "unit_name"],
    ).set_index("id")

    fdc_data = fdc_data[
        (fdc_data["fdc_id"].isin(fdc2fa)) & (fdc_data["nutrient_id"].isin(fdcn2fa))
    ]

    metadata = build_fdc_metadata(fdc_data, fdc_nutrients, fdc2fa, fdcn2fa)
    if metadata.empty:
        logger.info("No FDC metadata to merge.")
        return

    kg.add_triplets_from_metadata(metadata, relationship_type="contains")
    logger.info("Merged %d FDC metadata rows.", len(metadata))
