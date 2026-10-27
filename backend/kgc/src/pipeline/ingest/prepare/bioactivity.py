"""Bioactivity CSVs → ``data/Bioactivity/``, with whole-food heads re-keyed.

Seven files are copied verbatim: their heads are PubChem CIDs, MeSH ids and
assay ids the pipeline resolves itself, and the efficacy table's food ids are
checked to be foods of the snapshot. ``food_bioactivity_triplets.csv`` is not
copied: its foods are cited by entity id, and the delivery keys them on the
PTFI delta's private ids, which collide with real FoodAtlas ids of other
entities. Each id is replaced by a key the pipeline can resolve — the delta
takes precedence, since that is the id space the file was written in:

* an id the delta defines as a food → that food's PTFI sample id
  (``ptfi_sample_id``, its lowest GGB id, the same key the adapter uses)
* an id the delta does not define → kept as ``foodatlas_id``, provided it is a
  food in the snapshot
* anything else — a chemical, an unknown id — is a hard failure
"""

from __future__ import annotations

import logging
import shutil
from typing import TYPE_CHECKING

import pandas as pd

from .cells import as_dict
from .snapshot import ids_of_type

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

FOOD_FILE = "food_bioactivity_triplets.csv"
EFFICACY_FILE = "food_chemical_efficacy.csv"
COPIED_FILES = (
    "bioactivity_entities.csv",
    "bioactivity_metadata.csv",
    "bioassay_metadata.csv",
    "chemical_bioactivity_triplets.csv",
    "disease_bioactivity_triplets.csv",
    "bioactivity_disease_metadata.csv",
    EFFICACY_FILE,
)


def prepare_bioactivity(
    delivery_dir: Path, delta_entities: pd.DataFrame, snapshot: pd.DataFrame
) -> pd.DataFrame:
    """The re-keyed whole-food file; the other files only get checked."""
    efficacy = pd.read_csv(delivery_dir / EFFICACY_FILE, usecols=["foodatlas_id"])
    check_snapshot_foods(efficacy["foodatlas_id"], snapshot, EFFICACY_FILE)
    return rekey_food_heads(
        pd.read_csv(delivery_dir / FOOD_FILE), delta_entities, snapshot
    )


def write_bioactivity(
    delivery_dir: Path, food_edges: pd.DataFrame, out_dir: Path
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in COPIED_FILES:
        shutil.copy2(delivery_dir / name, out_dir / name)
    food_edges.to_csv(out_dir / FOOD_FILE, index=False)


def rekey_food_heads(
    food_edges: pd.DataFrame, delta_entities: pd.DataFrame, snapshot: pd.DataFrame
) -> pd.DataFrame:
    """Replace the delivery's food ids with keys the pipeline resolves (see
    module docstring). Output keeps the input columns plus ``ptfi_sample_id``;
    exactly one of ``foodatlas_id`` / ``ptfi_sample_id`` is set per row."""
    delta_foods = delta_entities[delta_entities["entity_type"] == "food"]
    sample_by_delta_id = {
        entity_id: min(as_dict(external)["ptfi"])
        for entity_id, external in zip(
            delta_foods["foodatlas_id"], delta_foods["external_ids"], strict=False
        )
    }
    cited = food_edges["foodatlas_id"].astype(str)
    is_delta = cited.isin(sample_by_delta_id)
    check_snapshot_foods(cited[~is_delta], snapshot, FOOD_FILE)

    rekeyed = food_edges.copy()
    rekeyed["foodatlas_id"] = cited.where(~is_delta, "")
    rekeyed.insert(1, "ptfi_sample_id", cited.map(sample_by_delta_id).fillna(""))
    logger.info(
        "%s: %d rows — %d re-keyed to PTFI sample ids, %d kept as FoodAtlas ids.",
        FOOD_FILE,
        len(rekeyed),
        int(is_delta.sum()),
        int((~is_delta).sum()),
    )
    return rekeyed


def check_snapshot_foods(cited: pd.Series, snapshot: pd.DataFrame, file: str) -> None:
    unknown = sorted(set(cited.astype(str)) - ids_of_type(snapshot, "food"))
    if unknown:
        msg = (
            f"{file}: {len(unknown)} food id(s) are not foods in the snapshot: "
            f"{unknown[:10]}"
        )
        raise ValueError(msg)
