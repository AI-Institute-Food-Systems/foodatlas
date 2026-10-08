"""Tests for PTFI entity resolution — FoodOn-IRI dedup, CID linking, minting,
and the registry registration that keeps PTFI ids stable across runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from src.pipeline.entities.resolve_ptfi import (
    create_unlinked_ptfi_chemicals,
    create_unlinked_ptfi_foods,
    link_ptfi_chemicals,
    link_ptfi_foods,
)
from src.pipeline.entities.utils.lut import EntityLUT
from src.stores.entity_registry import EntityRegistry

if TYPE_CHECKING:
    from pathlib import Path

_IRI = "http://purl.obolibrary.org/obo/FOODON_1"


class _Store:
    """Minimal EntityStore stand-in: the resolvers only touch _entities."""

    def __init__(self, rows: list[dict]) -> None:
        self._entities = pd.DataFrame(rows).set_index("foodatlas_id")
        self._curr_eid = 1


def _sources() -> dict[str, dict[str, pd.DataFrame]]:
    """One food with an IRI, one without; one chemical with a known CID, one new."""
    nodes = pd.DataFrame(
        [
            {
                "source_id": "ptfi",
                "native_id": "PTFI_FOOD:GGB100",
                "name": "pepper",
                "synonyms": ["pepper"],
                "synonym_types": [],
                "node_type": "food",
                "raw_attrs": {
                    "sample_ids": ["GGB100", "GGB101"],
                    "scientific_name": "Capsicum annuum",
                    "food_groups": ["plant food product"],
                    "foodon_iri": _IRI,
                },
            },
            {
                "source_id": "ptfi",
                "native_id": "PTFI_FOOD:GGB900",
                "name": "falafel wrap",
                "synonyms": ["falafel wrap"],
                "synonym_types": [],
                "node_type": "food",
                "raw_attrs": {
                    "sample_ids": ["GGB900"],
                    "scientific_name": "",
                    "food_groups": [],
                    "foodon_iri": "",
                },
            },
            {
                "source_id": "ptfi",
                "native_id": "PTFI_CHEM:77",
                "name": "salicylic acid",
                "synonyms": ["salicylic acid"],
                "synonym_types": [],
                "node_type": "chemical",
                "raw_attrs": {"cid": "77", "ptfi_id": "", "inchikey": ""},
            },
            {
                "source_id": "ptfi",
                "native_id": "PTFI_CHEM:5192",
                "name": "quercitrin",
                "synonyms": ["quercitrin"],
                "synonym_types": [],
                "node_type": "chemical",
                "raw_attrs": {
                    "cid": "5192",
                    "ptfi_id": "MET_PTF1",
                    "inchikey": "AAA-B-C",
                },
            },
        ]
    )
    xrefs = pd.DataFrame(
        [
            {
                "source_id": "ptfi",
                "native_id": "PTFI_FOOD:GGB100",
                "target_source": "foodon",
                "target_id": _IRI,
            },
            {
                "source_id": "ptfi",
                "native_id": "PTFI_CHEM:77",
                "target_source": "pubchem",
                "target_id": "77",
            },
            {
                "source_id": "ptfi",
                "native_id": "PTFI_CHEM:5192",
                "target_source": "pubchem",
                "target_id": "5192",
            },
        ]
    )
    return {"ptfi": {"nodes": nodes, "xrefs": xrefs}}


def _existing_store() -> _Store:
    return _Store(
        [
            {
                "foodatlas_id": "e10",
                "entity_type": "food",
                "common_name": "pepper",
                "scientific_name": "",
                "synonyms": ["pepper"],
                "external_ids": {"foodon": [_IRI]},
                "attributes": {},
            },
            {
                "foodatlas_id": "e20",
                "entity_type": "chemical",
                "common_name": "salicylic acid",
                "scientific_name": "",
                "synonyms": ["salicylic acid"],
                "external_ids": {"pubchem_compound": [77]},
                "attributes": {},
            },
        ]
    )


@pytest.fixture
def registry(tmp_path: Path) -> EntityRegistry:
    path = tmp_path / "entity_registry.parquet"
    pd.DataFrame(columns=["source", "native_id", "foodatlas_id"]).to_parquet(path)
    reg = EntityRegistry(path)
    reg.register("foodon", _IRI, "e10")
    reg.register("pubchem", "77", "e20")
    return reg


class TestPass2Linking:
    def test_food_sharing_a_foodon_iri_links_instead_of_minting(
        self, registry: EntityRegistry
    ) -> None:
        """This is the dedup merge_ptfi_delta.py hand-rolled for 257 foods."""
        store, linked = _existing_store(), set()
        link_ptfi_foods(_sources(), store, linked, registry)
        assert linked == {"PTFI_FOOD:GGB100"}
        assert store._entities.at["e10", "external_ids"]["ptfi"] == ["GGB100", "GGB101"]

    def test_linked_food_samples_become_resolvable(
        self, registry: EntityRegistry
    ) -> None:
        link_ptfi_foods(_sources(), _existing_store(), set(), registry)
        assert registry.resolve("ptfi", "GGB101") == ["e10"]

    def test_chemical_sharing_a_cid_links_instead_of_minting(
        self, registry: EntityRegistry
    ) -> None:
        store, linked = _existing_store(), set()
        link_ptfi_chemicals(_sources(), store, linked, registry)
        assert linked == {"PTFI_CHEM:77"}

    def test_unknown_cid_is_left_for_pass_3(self, registry: EntityRegistry) -> None:
        linked: set[str] = set()
        link_ptfi_chemicals(_sources(), _existing_store(), linked, registry)
        assert "PTFI_CHEM:5192" not in linked


class TestPass3Minting:
    def test_only_the_food_without_an_iri_is_minted(
        self, registry: EntityRegistry
    ) -> None:
        store, linked = _existing_store(), set()
        link_ptfi_foods(_sources(), store, linked, registry)
        create_unlinked_ptfi_foods(_sources(), store, EntityLUT(), linked, registry)
        minted = store._entities[store._entities.common_name == "falafel wrap"]
        assert len(minted) == 1
        assert minted.iloc[0]["external_ids"] == {"ptfi": ["GGB900"]}

    def test_minted_food_registers_its_sample_ids(
        self, registry: EntityRegistry
    ) -> None:
        """Without this the id is re-derived next run — the merge script's bug."""
        store, linked = _existing_store(), set()
        link_ptfi_foods(_sources(), store, linked, registry)
        create_unlinked_ptfi_foods(_sources(), store, EntityLUT(), linked, registry)
        assert registry.resolve("ptfi", "GGB900")

    def test_minted_chemical_keyed_on_cid_with_ptfi_and_inchikey_kept(
        self, registry: EntityRegistry
    ) -> None:
        store, linked = _existing_store(), set()
        link_ptfi_chemicals(_sources(), store, linked, registry)
        create_unlinked_ptfi_chemicals(_sources(), store, EntityLUT(), linked, registry)
        minted = store._entities[store._entities.common_name == "quercitrin"].iloc[0]
        assert minted["external_ids"]["pubchem_compound"] == [5192]
        assert minted["external_ids"]["ptfi"] == ["MET_PTF1"]
        assert minted["external_ids"]["inchikey"] == ["AAA-B-C"]
        assert registry.resolve("pubchem", "5192") == [minted.name]

    def test_a_second_run_mints_nothing_and_moves_no_id(
        self, registry: EntityRegistry
    ) -> None:
        """The whole point: ids are stable once the registry remembers them."""
        sources = _sources()
        store, linked = _existing_store(), set()
        link_ptfi_foods(sources, store, linked, registry)
        link_ptfi_chemicals(sources, store, linked, registry)
        create_unlinked_ptfi_foods(sources, store, EntityLUT(), linked, registry)
        create_unlinked_ptfi_chemicals(sources, store, EntityLUT(), linked, registry)
        first = store._entities.copy()

        linked2: set[str] = set()
        link_ptfi_foods(sources, store, linked2, registry)
        link_ptfi_chemicals(sources, store, linked2, registry)
        create_unlinked_ptfi_foods(sources, store, EntityLUT(), linked2, registry)
        create_unlinked_ptfi_chemicals(sources, store, EntityLUT(), linked2, registry)

        assert len(store._entities) == len(first)
        assert list(store._entities.index) == list(first.index)
        assert (store._entities.common_name == first.common_name).all()


class TestNoSource:
    def test_every_entry_point_is_a_no_op_without_ptfi(
        self, registry: EntityRegistry
    ) -> None:
        store, lut, linked = _existing_store(), EntityLUT(), set()
        link_ptfi_foods({}, store, linked, registry)
        link_ptfi_chemicals({}, store, linked, registry)
        create_unlinked_ptfi_foods({}, store, lut, linked, registry)
        create_unlinked_ptfi_chemicals({}, store, lut, linked, registry)
        assert len(store._entities) == 2
