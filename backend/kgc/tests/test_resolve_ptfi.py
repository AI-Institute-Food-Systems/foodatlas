"""Tests for PTFI entity resolution — FoodOn-IRI dedup, linking chemicals by
CID then ChEBI then FDC nutrient, minting, and the registry registration that
keeps PTFI ids stable across runs."""

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


def _chemical(key: str, name: str, **ids: str) -> dict:
    attrs = {"cid": "", "chebi": "", "fdc_nutrient": "", "ptfi_id": "", "inchikey": ""}
    attrs.update(ids)
    return {
        "source_id": "ptfi",
        "native_id": f"PTFI_CHEM:{key}",
        "name": name,
        "synonyms": [name],
        "synonym_types": [],
        "node_type": "chemical",
        "raw_attrs": attrs,
    }


def _xref(native_id: str, target_source: str, target_id: str) -> dict:
    return {
        "source_id": "ptfi",
        "native_id": native_id,
        "target_source": target_source,
        "target_id": target_id,
    }


def _sources() -> dict[str, dict[str, pd.DataFrame]]:
    """One food with an IRI, one without; chemicals: salicylic acid (known CID,
    plus a ChEBI id), protein (ChEBI + FDC nutrient, no CID), quercitrin (new
    CID), and myristate (new, CID + ChEBI)."""
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
            _chemical(
                "pubchem:77",
                "salicylic acid",
                cid="77",
                chebi="16914",
                ptfi_id="MET_PTF7",
            ),
            _chemical("chebi:36080", "protein", chebi="36080", fdc_nutrient="1003"),
            _chemical(
                "pubchem:5192",
                "quercitrin",
                cid="5192",
                ptfi_id="MET_PTF1",
                inchikey="AAA-B-C",
            ),
            _chemical("pubchem:11005", "myristate", cid="11005", chebi="30807"),
        ]
    )
    xrefs = pd.DataFrame(
        [
            _xref("PTFI_FOOD:GGB100", "foodon", _IRI),
            _xref("PTFI_CHEM:pubchem:77", "chebi", "16914"),
            _xref("PTFI_CHEM:pubchem:77", "pubchem", "77"),
            _xref("PTFI_CHEM:chebi:36080", "chebi", "36080"),
            _xref("PTFI_CHEM:chebi:36080", "fdc_nutrient", "1003"),
            _xref("PTFI_CHEM:pubchem:5192", "pubchem", "5192"),
            _xref("PTFI_CHEM:pubchem:11005", "pubchem", "11005"),
            _xref("PTFI_CHEM:pubchem:11005", "chebi", "30807"),
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
            {
                "foodatlas_id": "e21",
                "entity_type": "chemical",
                "common_name": "salicylate",
                "scientific_name": "",
                "synonyms": [],
                "external_ids": {"chebi": [16914]},
                "attributes": {},
            },
            {
                "foodatlas_id": "e25",
                "entity_type": "chemical",
                "common_name": "protein",
                "scientific_name": "",
                "synonyms": [],
                "external_ids": {"chebi": [36080]},
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
    reg.register("chebi", "16914", "e21")
    reg.register("chebi", "36080", "e25")
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
        assert "PTFI_CHEM:pubchem:77" in linked

    def test_cid_is_tried_before_chebi(self, registry: EntityRegistry) -> None:
        """Salicylic acid's CID and ChEBI id resolve to different entities; the
        CID wins, so the PTFI id lands on e20 and not on the ChEBI class."""
        store, linked = _existing_store(), set()
        link_ptfi_chemicals(_sources(), store, linked, registry)
        assert store._entities.at["e20", "external_ids"]["ptfi"] == ["MET_PTF7"]
        assert "ptfi" not in store._entities.at["e21", "external_ids"]

    def test_chemical_without_a_cid_links_by_chebi(
        self, registry: EntityRegistry
    ) -> None:
        store, linked = _existing_store(), set()
        link_ptfi_chemicals(_sources(), store, linked, registry)
        assert "PTFI_CHEM:chebi:36080" in linked

    def test_unknown_keys_are_left_for_pass_3(self, registry: EntityRegistry) -> None:
        linked: set[str] = set()
        link_ptfi_chemicals(_sources(), _existing_store(), linked, registry)
        assert linked == {"PTFI_CHEM:pubchem:77", "PTFI_CHEM:chebi:36080"}


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

    def test_minted_chemical_registers_every_key_it_has(
        self, registry: EntityRegistry
    ) -> None:
        """A later ChEBI refresh bringing myristate in under 30807 must land on
        this entity, not a duplicate."""
        store, linked = _existing_store(), set()
        link_ptfi_chemicals(_sources(), store, linked, registry)
        create_unlinked_ptfi_chemicals(_sources(), store, EntityLUT(), linked, registry)
        minted = store._entities[store._entities.common_name == "myristate"].iloc[0]
        assert minted["external_ids"] == {"pubchem_compound": [11005], "chebi": [30807]}
        assert registry.resolve("pubchem", "11005") == [minted.name]
        assert registry.resolve("chebi", "30807") == [minted.name]

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
        assert len(store._entities) == 4
