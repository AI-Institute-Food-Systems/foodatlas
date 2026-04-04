"""Tests for triplet builders — cartesian product on shared native IDs."""

import json
from pathlib import Path

import pandas as pd
from src.models.settings import KGCSettings
from src.pipeline.knowledge_graph import KnowledgeGraph
from src.pipeline.triplets.chemical_chemical import merge_chemical_ontology
from src.pipeline.triplets.disease_disease import merge_disease_ontology
from src.pipeline.triplets.food_food import merge_food_ontology
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_EXTRACTIONS,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_TRIPLETS,
)


def _make_kg(tmp_path: Path, entities: list[dict]) -> KnowledgeGraph:
    kg_dir = tmp_path / "kg"
    kg_dir.mkdir(exist_ok=True)
    df = pd.DataFrame(entities)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(json.dumps)
    df.to_parquet(kg_dir / FILE_ENTITIES, index=False)
    pd.DataFrame().to_parquet(kg_dir / FILE_TRIPLETS)
    pd.DataFrame().to_parquet(kg_dir / FILE_EVIDENCE)
    pd.DataFrame().to_parquet(kg_dir / FILE_EXTRACTIONS)
    for f in (FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
        (kg_dir / f).parent.mkdir(parents=True, exist_ok=True)
        (kg_dir / f).write_text("{}")
    settings = KGCSettings(kg_dir=str(kg_dir), cache_dir=str(kg_dir / "_cache"))
    return KnowledgeGraph(settings)


def _is_a_sources(
    pairs: list[tuple[str, str]], source: str
) -> dict[str, dict[str, pd.DataFrame]]:
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
    return {source: {"edges": pd.DataFrame(rows)}}


class TestFoodOntologyCartesian:
    def test_shared_foodon_id_produces_cartesian(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        merge_food_ontology(kg, _is_a_sources([("F001", "F002")], "foodon"))
        # 2 heads (e0,e1) x 1 tail (e2) = 2 triplets
        assert len(kg.triplets._triplets) == 2


class TestChemicalOntologyCartesian:
    def test_shared_chebi_id_produces_cartesian(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        merge_chemical_ontology(kg, _is_a_sources([("100", "200")], "chebi"))
        assert len(kg.triplets._triplets) == 2


class TestDiseaseOntologyCartesian:
    def test_shared_ctd_id_produces_cartesian(self, tmp_path: Path) -> None:
        kg = _make_kg(
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
        merge_disease_ontology(kg, _is_a_sources([("D001", "D002")], "ctd"))
        assert len(kg.triplets._triplets) == 2
