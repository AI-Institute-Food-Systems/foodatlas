"""Tests for triplet builders — cartesian product on shared native IDs."""

import json
from pathlib import Path

import pandas as pd
from src.pipeline.triplets.chemical_chemical import create_chemical_ontology
from src.pipeline.triplets.disease_disease import create_disease_ontology
from src.pipeline.triplets.food_food import create_food_ontology
from src.stores.entity_store import EntityStore
from src.stores.schema import FILE_ENTITIES, FILE_LUT_CHEMICAL, FILE_LUT_FOOD


def _make_store(tmp_path: Path, entities: list[dict]) -> EntityStore:
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(tmp_path / FILE_ENTITIES, index=False)
    for f in (FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
        (tmp_path / f).write_text("{}")
    return EntityStore(
        path_entities=tmp_path / FILE_ENTITIES,
        path_lut_food=tmp_path / FILE_LUT_FOOD,
        path_lut_chemical=tmp_path / FILE_LUT_CHEMICAL,
    )


def _is_a_edges(pairs: list[tuple[str, str]], source: str) -> dict[str, pd.DataFrame]:
    rows = [
        {
            "source_id": source,
            "head_native_id": h,
            "tail_native_id": t,
            "edge_type": "is_a",
            "raw_attrs": {},
        }
        for h, t in pairs
    ]
    return {"edges": pd.DataFrame(rows)}


class TestFoodOntologyCartesian:
    def test_shared_foodon_id_produces_cartesian(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "food",
                    "common_name": "apple",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"foodon": ["F001"]},
                },
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "apple variant",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"foodon": ["F001"]},
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "food",
                    "common_name": "fruit",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"foodon": ["F002"]},
                },
            ],
        )
        sources = {"foodon": _is_a_edges([("F001", "F002")], "foodon")}
        triplets = create_food_ontology(store, sources)

        # 2 heads (e0,e1) x 1 tail (e2) = 2 triplets
        assert len(triplets) == 2


class TestChemicalOntologyCartesian:
    def test_shared_chebi_id_produces_cartesian(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "chemical",
                    "common_name": "aspirin",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"chebi": [100]},
                },
                {
                    "foodatlas_id": "e1",
                    "entity_type": "chemical",
                    "common_name": "aspirin alt",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"chebi": [100]},
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "chemical",
                    "common_name": "nsaid",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"chebi": [200]},
                },
            ],
        )
        # ChEBI is_a: head=100 is_a tail=200 -> reversed in builder
        sources = {"chebi": _is_a_edges([("100", "200")], "chebi")}
        triplets = create_chemical_ontology(store, sources)

        # head_ids from tail_key(200)->[e2], tail_ids from head_key(100)->[e0,e1]
        assert len(triplets) == 2


class TestDiseaseOntologyCartesian:
    def test_shared_ctd_id_produces_cartesian(self, tmp_path: Path) -> None:
        store = _make_store(
            tmp_path,
            [
                {
                    "foodatlas_id": "e0",
                    "entity_type": "disease",
                    "common_name": "flu",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"ctd": ["D001"]},
                },
                {
                    "foodatlas_id": "e1",
                    "entity_type": "disease",
                    "common_name": "flu variant",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"ctd": ["D001"]},
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "disease",
                    "common_name": "infection",
                    "scientific_name": "",
                    "synonyms": [],
                    "external_ids": {"ctd": ["D002"]},
                },
            ],
        )
        sources = {"ctd": _is_a_edges([("D001", "D002")], "ctd")}
        triplets = create_disease_ontology(store, sources)

        assert len(triplets) == 2
