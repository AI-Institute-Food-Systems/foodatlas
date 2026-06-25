"""Tests for apply_flavor_descriptions — matches FlavorDB nodes to
chemical entities by PubChem CID and writes descriptors into the chemical
entity's attributes."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from src.pipeline.enrichment.flavor import apply_flavor_descriptions


def _entities() -> pd.DataFrame:
    """Two chemicals (one with a PubChem CID, one without) + one food."""
    df = pd.DataFrame(
        [
            {
                "entity_type": "chemical",
                "external_ids": {"pubchem_compound": ["100"]},
                "attributes": {},
            },
            {
                "entity_type": "chemical",
                "external_ids": {},
                "attributes": {},
            },
            {
                "entity_type": "food",
                "external_ids": {"foodon": ["FOO_1"]},
                "attributes": {},
            },
        ],
        index=["CHEM1", "CHEM2", "FOOD1"],
    )
    df.index.name = "foodatlas_id"
    return df


def _kg(entities: pd.DataFrame) -> MagicMock:
    kg = MagicMock()
    kg.entities._entities = entities
    return kg


def test_no_flavordb_source_no_op() -> None:
    ents = _entities()
    kg = _kg(ents)
    apply_flavor_descriptions(kg, {})
    # attributes column untouched
    assert ents.at["CHEM1", "attributes"] == {}


def test_empty_flavordb_nodes_no_op() -> None:
    ents = _entities()
    kg = _kg(ents)
    apply_flavor_descriptions(
        kg, {"flavordb": {"nodes": pd.DataFrame(columns=["native_id"])}}
    )
    assert ents.at["CHEM1", "attributes"] == {}


def test_matched_cid_writes_sorted_descriptors() -> None:
    ents = _entities()
    kg = _kg(ents)
    nodes = pd.DataFrame(
        [
            {
                "native_id": "100",
                "raw_attrs": {"flavors": ["sweet", "floral", "sweet"]},
            }
        ]
    )
    apply_flavor_descriptions(kg, {"flavordb": {"nodes": nodes}})
    attrs = ents.at["CHEM1", "attributes"]
    assert attrs["flavor_descriptors"] == ["floral", "sweet"]


def test_unmatched_cid_skipped() -> None:
    ents = _entities()
    kg = _kg(ents)
    # Node has CID that doesn't map to any chemical entity
    nodes = pd.DataFrame(
        [{"native_id": "9999", "raw_attrs": {"flavors": ["bitter"]}}]
    )
    apply_flavor_descriptions(kg, {"flavordb": {"nodes": nodes}})
    # Nothing written
    assert ents.at["CHEM1", "attributes"] == {}


def test_non_dict_raw_attrs_skipped() -> None:
    ents = _entities()
    kg = _kg(ents)
    nodes = pd.DataFrame(
        [{"native_id": "100", "raw_attrs": "not-a-dict"}]
    )
    apply_flavor_descriptions(kg, {"flavordb": {"nodes": nodes}})
    assert ents.at["CHEM1", "attributes"] == {}
