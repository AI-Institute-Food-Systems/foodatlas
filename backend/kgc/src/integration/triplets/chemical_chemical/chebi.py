"""Initialize chemical ontology triplets from ChEBI hierarchy."""

import json
import logging
from pathlib import Path

import pandas as pd

from ....models.settings import KGCSettings
from ....stores.entity_store import EntityStore
from ....stores.schema import FILE_CHEMICAL_ONTOLOGY

logger = logging.getLogger(__name__)


def create_chemical_ontology(
    entity_store: EntityStore,
    settings: KGCSettings,
) -> pd.DataFrame:
    """Generate is_a triplets from ChEBI ontology and save to KG directory.

    Returns the ontology DataFrame.
    """
    data_dir = Path(settings.data_dir)
    chebi_relations: pd.DataFrame = pd.read_csv(
        data_dir / "ChEBI" / "relation.tsv",
        sep="\t",
        encoding="unicode_escape",
    )

    chebi2fa = _build_chebi_to_fa_map(entity_store)

    is_a_rows: list[dict[str, str | None]] = []
    for _, row in chebi_relations.iterrows():
        if row["TYPE"] != "is_a":
            continue
        if not (row["INIT_ID"] in chebi2fa and row["FINAL_ID"] in chebi2fa):
            continue
        is_a_rows.append(
            {
                "foodatlas_id": None,
                "head_id": chebi2fa[row["FINAL_ID"]],
                "relationship_id": "r2",
                "tail_id": chebi2fa[row["INIT_ID"]],
                "source": "chebi",
            }
        )

    is_a = pd.DataFrame(is_a_rows)
    is_a["foodatlas_id"] = [f"co{i}" for i in range(1, len(is_a) + 1)]

    kg_dir = Path(settings.kg_dir)
    records = is_a.to_dict(orient="records")
    with (kg_dir / FILE_CHEMICAL_ONTOLOGY).open("w") as f:
        json.dump(records, f, ensure_ascii=False)
    logger.info("Created %d chemical ontology triplets.", len(is_a))

    return is_a


def _build_chebi_to_fa_map(entity_store: EntityStore) -> dict[int, str]:
    """Map ChEBI IDs to FoodAtlas entity IDs."""
    chebi2fa: dict[int, str] = {}
    for faid, row in entity_store._entities.iterrows():
        if "chebi" not in row["external_ids"]:
            continue
        chebi2fa[row["external_ids"]["chebi"][0]] = str(faid)
    return chebi2fa
