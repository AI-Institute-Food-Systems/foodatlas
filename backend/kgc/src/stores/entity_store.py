"""EntityStore — runtime container wrapping a pandas DataFrame."""

import logging
from collections import OrderedDict
from pathlib import Path

import pandas as pd

from ..discovery.chemical import create_chemical_entities
from ..discovery.food import create_food_entities
from ..utils.json_io import read_json, write_json
from .schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    INDEX_COL,
)

logger = logging.getLogger(__name__)

FAID_PREFIX = "e"


def _load_lut(path: Path) -> dict[str, list[str]]:
    """Load a synonym → entity-ID lookup table from a JSON file."""
    data: dict[str, list[str]] = read_json(path)
    return data


def _save_lut(lut: dict[str, list[str]], path: Path) -> None:
    """Save a synonym → entity-ID lookup table to a JSON file."""
    write_json(path, lut)


class EntityStore:
    """Manages entities (food & chemical) in the knowledge graph.

    Stores a DataFrame of entities and lookup tables (LUTs) mapping
    synonym strings to lists of entity IDs for fast disambiguation.
    """

    def __init__(
        self,
        path_entities: Path,
        path_lut_food: Path,
        path_lut_chemical: Path,
        path_kg: Path | None = None,
        path_cache_dir: Path | None = None,
    ) -> None:
        self.path_entities = Path(path_entities)
        self.path_lut_food = Path(path_lut_food)
        self.path_lut_chemical = Path(path_lut_chemical)
        self.path_kg = Path(path_kg) if path_kg else None
        self.path_cache_dir = Path(path_cache_dir) if path_cache_dir else None

        self._entities: pd.DataFrame = pd.DataFrame()
        self._lut_food: dict[str, list[str]] = {}
        self._lut_chemical: dict[str, list[str]] = {}
        self._curr_eid: int = 1

        self._load()

    def _load(self) -> None:
        records = read_json(self.path_entities)
        self._entities = pd.DataFrame(records)
        if not self._entities.empty:
            self._entities = self._entities.set_index(INDEX_COL)

        self._lut_food = _load_lut(self.path_lut_food)
        self._lut_chemical = _load_lut(self.path_lut_chemical)

        if self._entities.empty:
            self._curr_eid = 1
        else:
            max_eid = self._entities.index.str.slice(1).astype(int).max()
            self._curr_eid = max_eid + 1 if pd.notna(max_eid) else 1

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        records = self._entities.reset_index().to_dict(orient="records")
        write_json(path_output_dir / FILE_ENTITIES, records)
        _save_lut(self._lut_food, path_output_dir / FILE_LUT_FOOD)
        _save_lut(self._lut_chemical, path_output_dir / FILE_LUT_CHEMICAL)

    def _get_lut(self, entity_type: str) -> dict[str, list[str]]:
        luts = {"food": self._lut_food, "chemical": self._lut_chemical}
        if entity_type not in luts:
            msg = f"Invalid entity type: {entity_type}"
            raise ValueError(msg)
        return luts[entity_type]

    def update_entity_synonyms(
        self,
        entity_id: str,
        synonyms_new: list[str],
    ) -> None:
        entity = self.get_entity(entity_id)
        lut = self._get_lut(entity["entity_type"])

        existing = OrderedDict.fromkeys(entity["synonyms"])
        updated = False
        for synonym in synonyms_new:
            if synonym not in existing:
                existing[synonym] = None
                if synonym not in lut:
                    lut[synonym] = []
                lut[synonym] += [entity_id]
                updated = True

        if updated:
            self._entities.at[entity_id, "synonyms"] = list(existing.keys())
            self.update_lut(self._entities.loc[[entity_id]])

    def update_lut(self, entities: pd.DataFrame) -> None:
        for entity_id, row in entities.iterrows():
            lut = self._get_lut(row["entity_type"])
            for synonym in row["synonyms"]:
                if synonym not in lut:
                    lut[synonym] = []
                if entity_id not in lut[synonym]:
                    lut[synonym] += [entity_id]

    def create(
        self,
        entity_type: str,
        entity_names_new: list[str],
    ) -> None:
        creators = {
            "food": create_food_entities,
            "chemical": create_chemical_entities,
        }
        if entity_type not in creators:
            msg = f"Invalid entity type: {entity_type}."
            raise ValueError(msg)
        creators[entity_type](self, entity_names_new)

    def get_entity_ids(
        self,
        entity_type: str,
        entity_name: str,
    ) -> list[str]:
        lut = self._get_lut(entity_type)
        return lut.get(entity_name, [])

    def get_entity(self, entity_id: str) -> pd.Series:
        return self._entities.loc[entity_id]

    def get_new_names(
        self,
        entity_type: str,
        names: list[str],
    ) -> list[str]:
        new_names = [n for n in names if not self.get_entity_ids(entity_type, n)]

        logger.info(
            "# of unique %s name existing/new: %d/%d",
            entity_type,
            len(names) - len(new_names),
            len(new_names),
        )
        return new_names
