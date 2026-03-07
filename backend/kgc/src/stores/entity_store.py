"""EntityStore — runtime container wrapping a pandas DataFrame."""

import logging
from ast import literal_eval
from collections import OrderedDict
from pathlib import Path

import pandas as pd

from ..entities.chemical import create_chemical_entities
from ..entities.food import create_food_entities

logger = logging.getLogger(__name__)

COLUMNS = [
    "foodatlas_id",
    "entity_type",
    "common_name",
    "scientific_name",
    "synonyms",
    "external_ids",
    "_synonyms_display",
]
FAID_PREFIX = "e"


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
        self._entities = pd.read_csv(
            self.path_entities,
            sep="\t",
            converters={
                "synonyms": literal_eval,
                "external_ids": literal_eval,
                "_synonyms_display": literal_eval,
            },
        ).set_index("foodatlas_id")

        for path_lut, attr in [
            (self.path_lut_food, "_lut_food"),
            (self.path_lut_chemical, "_lut_chemical"),
        ]:
            lut_df = pd.read_csv(
                path_lut,
                sep="\t",
                converters={
                    "foodatlas_id": literal_eval,
                    "name": str,
                },
            )
            setattr(
                self,
                attr,
                dict(zip(lut_df["name"], lut_df["foodatlas_id"], strict=False)),
            )

        eid = self._entities.index.str.slice(1).astype(int).max()
        self._curr_eid = eid + 1 if pd.notna(eid) else 1

    def save(self, path_output_dir: Path) -> None:
        path_output_dir = Path(path_output_dir)
        self._entities.to_csv(path_output_dir / "entities.tsv", sep="\t")
        pd.DataFrame(self._lut_food.items(), columns=["name", "foodatlas_id"]).to_csv(
            path_output_dir / "lookup_table_food.tsv", sep="\t", index=False
        )
        pd.DataFrame(
            self._lut_chemical.items(), columns=["name", "foodatlas_id"]
        ).to_csv(path_output_dir / "lookup_table_chemical.tsv", sep="\t", index=False)

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
        synonyms = OrderedDict.fromkeys(entity["synonyms"])
        updated = False
        for synonym in synonyms_new:
            if synonym not in synonyms:
                synonyms[synonym] = None
                if synonym not in self._lut_food:
                    self._lut_food[synonym] = []
                self._lut_food[synonym] += [entity_id]
                updated = True

        if updated:
            self._entities.at[entity_id, "synonyms"] = list(synonyms.keys())
            self.update_lut(self._entities.loc[[entity_id]])

    def update_lut(self, entities: pd.DataFrame) -> None:
        def _add_to_lut(row: pd.Series) -> None:
            lut = self._get_lut(row["entity_type"])
            for synonym in row["synonyms"]:
                if synonym not in lut:
                    lut[synonym] = []
                if row.name not in lut[synonym]:
                    lut[synonym] += [row.name]

        entities.apply(_add_to_lut, axis=1)

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
        n_found = 0
        names_not_in_lut: list[str] = []
        for name in names:
            if not self.get_entity_ids(entity_type, name):
                names_not_in_lut.append(name)
            else:
                n_found += 1

        logger.info(
            "# of unique %s name existing/new: %d/%d",
            entity_type,
            n_found,
            len(names_not_in_lut),
        )
        return names_not_in_lut
