"""Tests for food entity creation helpers."""

from pathlib import Path

import pandas as pd
import pytest
from src.entities.food import (
    _create_from_ncbi_taxonomy,
    _create_from_synonym_groups,
    _group_synonyms,
    _parse_ncbi_names,
    create_food_entities,
)
from src.stores.entity_store import EntityStore


@pytest.fixture()
def entities_dir(tmp_path: Path) -> Path:
    entities_df = pd.DataFrame(
        columns=[
            "foodatlas_id",
            "entity_type",
            "common_name",
            "scientific_name",
            "synonyms",
            "external_ids",
            "_synonyms_display",
        ]
    )
    entities_df.to_csv(tmp_path / "entities.tsv", sep="\t", index=False)

    for name in ("lookup_table_food.tsv", "lookup_table_chemical.tsv"):
        pd.DataFrame(columns=["name", "foodatlas_id"]).to_csv(
            tmp_path / name, sep="\t", index=False
        )
    return tmp_path


@pytest.fixture()
def entities(entities_dir: Path) -> EntityStore:
    return EntityStore(
        path_entities=entities_dir / "entities.tsv",
        path_lut_food=entities_dir / "lookup_table_food.tsv",
        path_lut_chemical=entities_dir / "lookup_table_chemical.tsv",
    )


class TestGroupSynonyms:
    def test_merges_overlapping_groups(self) -> None:
        groups = [["apple", "apples"], ["apples", "apple"]]
        result = _group_synonyms(groups)
        assert len(result) == 1
        assert set(result[0]) == {"apple", "apples"}

    def test_keeps_disjoint_groups(self) -> None:
        groups = [["apple"], ["banana"]]
        result = _group_synonyms(groups)
        assert len(result) == 2

    def test_complex_merge(self) -> None:
        groups = [
            ["apple", "apples"],
            ["apples", "apple"],
            ["olive", "olives"],
            ["olives", "olife"],
        ]
        result = _group_synonyms(groups)
        assert len(result) == 2
        names_flat = [name for group in result for name in group]
        assert "olife" in names_flat


class TestParseNcbiNames:
    def test_basic_record(self) -> None:
        row = pd.Series(
            {
                "ScientificName": "Malus domestica",
                "OtherNames": {
                    "Synonym": [],
                    "EquivalentName": [],
                    "Name": [],
                    "CommonName": ["apple"],
                    "GenbankCommonName": "apple",
                    "Includes": [],
                },
                "TaxId": 12345,
                "foodatlas_id": None,
                "entity_type": None,
                "common_name": None,
                "scientific_name": None,
                "synonyms": None,
                "external_ids": None,
                "_synonyms_display": None,
            }
        )
        result = _parse_ncbi_names(row)
        assert result["scientific_name"] == "malus domestica"
        assert result["common_name"] == "apple"
        assert "_NCBI_Taxon_ID:12345" in result["synonyms"]

    def test_no_other_names(self) -> None:
        row = pd.Series(
            {
                "ScientificName": "Triticum",
                "OtherNames": None,
                "TaxId": 999,
                "foodatlas_id": None,
                "entity_type": None,
                "common_name": None,
                "scientific_name": None,
                "synonyms": None,
                "external_ids": None,
                "_synonyms_display": None,
            }
        )
        result = _parse_ncbi_names(row)
        assert result["common_name"] == "triticum"
        assert result["scientific_name"] == "triticum"


class TestCreateFromNcbiTaxonomy:
    def test_creates_entities(self, entities: EntityStore) -> None:
        records = pd.DataFrame(
            [
                {
                    "ScientificName": "Malus domestica",
                    "OtherNames": {
                        "Synonym": [],
                        "EquivalentName": [],
                        "Name": [],
                        "CommonName": ["apple"],
                        "Includes": [],
                    },
                    "TaxId": 12345,
                },
            ]
        )
        _create_from_ncbi_taxonomy(entities, records)
        assert len(entities._entities) == 1
        assert entities._curr_eid == 2
        assert "apple" in entities._lut_food


class TestCreateFromSynonymGroups:
    def test_creates_entities(self, entities: EntityStore) -> None:
        groups = [["banana", "bananas"], ["cherry"]]
        _create_from_synonym_groups(entities, groups)
        assert len(entities._entities) == 2
        assert "banana" in entities._lut_food
        assert "cherry" in entities._lut_food

    def test_skips_existing(self, entities: EntityStore) -> None:
        groups = [["mango"]]
        _create_from_synonym_groups(entities, groups)
        count_before = len(entities._entities)
        _create_from_synonym_groups(entities, [["mango"]])
        assert len(entities._entities) == count_before


class TestCreateFoodEntityStore:
    def test_raises_when_query_not_implemented(self, entities: EntityStore) -> None:
        with pytest.raises(NotImplementedError):
            create_food_entities(entities, ["apple"])
