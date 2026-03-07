"""Tests for chemical entity creation helpers."""

import pandas as pd
import pytest
from src.discovery.chemical import (
    _create_from_names,
    _create_from_pubchem_compound,
    _parse_pubchem_names,
    create_chemical_entities,
)
from src.stores.entity_store import EntityStore


@pytest.fixture()
def entities(entity_store_empty: EntityStore) -> EntityStore:
    return entity_store_empty


class TestParsePubchemNames:
    def test_basic_record(self) -> None:
        row = pd.Series(
            {
                "IUPACName": "ascorbic acid",
                "SynonymList": ["vitamin c", "l-ascorbic acid"],
                "CID": 54670067,
                "foodatlas_id": None,
                "entity_type": None,
                "common_name": None,
                "scientific_name": None,
                "synonyms": None,
                "external_ids": None,
                "_synonyms_display": None,
            }
        )
        result = _parse_pubchem_names(row)
        assert result["scientific_name"] == "ascorbic acid"
        assert result["common_name"] == "vitamin c"
        assert "_PubChem_Compound_ID:54670067" in result["synonyms"]

    def test_no_iupac_name(self) -> None:
        row = pd.Series(
            {
                "IUPACName": float("nan"),
                "SynonymList": ["beta-carotene"],
                "CID": 5280489,
                "foodatlas_id": None,
                "entity_type": None,
                "common_name": None,
                "scientific_name": None,
                "synonyms": None,
                "external_ids": None,
                "_synonyms_display": None,
            }
        )
        result = _parse_pubchem_names(row)
        assert result["scientific_name"] is None
        assert result["common_name"] == "beta-carotene"


class TestCreateFromPubchemCompound:
    def test_creates_entities(self, entities: EntityStore) -> None:
        records = pd.DataFrame(
            [
                {
                    "IUPACName": "ascorbic acid",
                    "SynonymList": ["vitamin c"],
                    "CID": 54670067,
                },
            ]
        )
        _create_from_pubchem_compound(entities, records)
        assert len(entities._entities) == 1
        assert entities._curr_eid == 2
        assert "vitamin c" in entities._lut_chemical

    def test_skips_existing_cid(self, entities: EntityStore) -> None:
        records = pd.DataFrame(
            [
                {
                    "IUPACName": "ascorbic acid",
                    "SynonymList": ["vitamin c"],
                    "CID": 54670067,
                },
            ]
        )
        _create_from_pubchem_compound(entities, records)
        count_before = len(entities._entities)
        _create_from_pubchem_compound(entities, records)
        assert len(entities._entities) == count_before


class TestCreateFromNames:
    def test_creates_entities(self, entities: EntityStore) -> None:
        _create_from_names(entities, ["caffeine", "theobromine"])
        assert len(entities._entities) == 2
        assert "caffeine" in entities._lut_chemical
        assert "theobromine" in entities._lut_chemical

    def test_skips_existing(self, entities: EntityStore) -> None:
        _create_from_names(entities, ["caffeine"])
        count_before = len(entities._entities)
        _create_from_names(entities, ["caffeine"])
        assert len(entities._entities) == count_before

    def test_id_generation(self, entities: EntityStore) -> None:
        _create_from_names(entities, ["a", "b", "c"])
        assert entities._curr_eid == 4
        assert entities.get_entity_ids("chemical", "a") == ["e1"]
        assert entities.get_entity_ids("chemical", "c") == ["e3"]


class TestCreateChemicalEntityStore:
    def test_raises_when_query_not_implemented(self, entities: EntityStore) -> None:
        with pytest.raises(NotImplementedError):
            create_chemical_entities(entities, ["caffeine"])
