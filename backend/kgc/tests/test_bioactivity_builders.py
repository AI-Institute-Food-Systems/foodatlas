"""Tests for triplets/bioactivity/builders — exercises merge_food_bioactivity,
merge_chemical_bioactivity, and merge_bioactivity_ontology against a stub
KnowledgeGraph. Stores are MagicMocks; we assert on the DataFrames passed
into kg.triplets.create / kg.evidence.create / kg.attestations.create."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
from src.pipeline.triplets.bioactivity.builders import (
    merge_bioactivity_ontology,
    merge_chemical_bioactivity,
    merge_food_bioactivity,
)


def _make_kg(entities: pd.DataFrame) -> MagicMock:
    """Build a KG stub whose entity_store, triplets, evidence, attestations
    are all MagicMocks. entities._entities is set to the given DataFrame."""
    kg = MagicMock()
    kg.entities._entities = entities
    # evidence.create returns a DataFrame whose .index is used as evidence_id;
    # attestations.create returns a DataFrame whose .index becomes triplet idx.
    kg.evidence.create.return_value = pd.DataFrame(index=["ev1"])
    kg.attestations.create.return_value = pd.DataFrame(index=["att1"])
    kg.triplets.create.return_value = pd.DataFrame()
    return kg


# A minimal entities DataFrame with bioactivity + food + chemical rows.
# external_ids columns are the "key → list[native_id]" maps that
# explode_external_ids walks.
_ENTITIES = pd.DataFrame(
    [
        {
            "entity_type": "bioactivity",
            "external_ids": {"bioactivity_concept": ["B1"]},
        },
        {
            "entity_type": "bioactivity",
            "external_ids": {"bioactivity_concept": ["B2"]},
        },
        {
            "entity_type": "food",
            "external_ids": {},
        },
        {
            "entity_type": "chemical",
            "external_ids": {"pubchem_compound": ["123"]},
        },
    ],
    index=["BIO1", "BIO2", "FOOD1", "CHEM1"],
)


def test_merge_food_bioactivity_passes_metadata_to_create() -> None:
    edges = pd.DataFrame(
        [
            {
                "source_id": "bioactivity",
                "head_native_id": "FOOD1",
                "tail_native_id": "B1",
                "edge_type": "exhibits",
                "raw_attrs": {"bioactivity_metadata_ids": ["bm1", "bm2"]},
            }
        ]
    )
    kg = _make_kg(_ENTITIES)
    merge_food_bioactivity(kg, {"bioactivity": {"edges": edges}})
    kg.triplets.create.assert_called_once()
    metadata = kg.triplets.create.call_args.args[0]
    assert list(metadata.index) == ["bm1", "bm2"]
    assert all(metadata["head_id"] == "FOOD1")
    assert all(metadata["tail_id"] == "BIO1")
    assert all(metadata["relationship_id"] == "r5")


def test_merge_chemical_bioactivity_resolves_via_pubchem_cid() -> None:
    edges = pd.DataFrame(
        [
            {
                "source_id": "bioactivity",
                "head_native_id": "123",
                "tail_native_id": "B2",
                "edge_type": "measured",
                "raw_attrs": {"bioactivity_metadata_ids": ["bm9"]},
            }
        ]
    )
    kg = _make_kg(_ENTITIES)
    merge_chemical_bioactivity(kg, {"bioactivity": {"edges": edges}})
    metadata = kg.triplets.create.call_args.args[0]
    assert list(metadata.index) == ["bm9"]
    assert all(metadata["head_id"] == "CHEM1")
    assert all(metadata["tail_id"] == "BIO2")
    assert all(metadata["relationship_id"] == "r6")


def test_merge_unknown_food_dropped_with_warning() -> None:
    edges = pd.DataFrame(
        [
            {
                "source_id": "bioactivity",
                "head_native_id": "UNKNOWN_FOOD",
                "tail_native_id": "B1",
                "edge_type": "exhibits",
                "raw_attrs": {"bioactivity_metadata_ids": ["bm1"]},
            }
        ]
    )
    kg = _make_kg(_ENTITIES)
    merge_food_bioactivity(kg, {"bioactivity": {"edges": edges}})
    # No surviving rows => triplets.create not called (empty branch)
    kg.triplets.create.assert_not_called()


def test_merge_returns_early_when_source_missing() -> None:
    kg = _make_kg(_ENTITIES)
    merge_food_bioactivity(kg, {})
    merge_chemical_bioactivity(kg, {})
    kg.triplets.create.assert_not_called()


def test_merge_returns_early_when_no_matching_edge_type() -> None:
    edges = pd.DataFrame(
        [
            {
                "edge_type": "is_a",
                "head_native_id": "B1",
                "tail_native_id": "B0",
                "source_id": "bioactivity",
                "raw_attrs": {},
            }
        ]
    )
    kg = _make_kg(_ENTITIES)
    merge_food_bioactivity(kg, {"bioactivity": {"edges": edges}})
    kg.triplets.create.assert_not_called()


def test_merge_ontology_creates_evidence_attestation_triplet() -> None:
    is_a = pd.DataFrame(
        [
            {
                "source_id": "bioactivity",
                "head_native_id": "B1",
                "tail_native_id": "B2",
                "edge_type": "is_a",
                "raw_attrs": {},
            }
        ]
    )
    kg = _make_kg(_ENTITIES)
    merge_bioactivity_ontology(kg, {"bioactivity": {"edges": is_a}})
    kg.evidence.create.assert_called_once()
    kg.attestations.create.assert_called_once()
    kg.triplets.create.assert_called_once()
    triplet = kg.triplets.create.call_args.args[0]
    assert list(triplet.columns) == ["head_id", "tail_id", "relationship_id"]
    assert all(triplet["relationship_id"] == "r2")


def test_merge_ontology_returns_early_when_no_is_a() -> None:
    kg = _make_kg(_ENTITIES)
    # No bioactivity source at all
    merge_bioactivity_ontology(kg, {})
    kg.triplets.create.assert_not_called()
    # Empty is_a edges
    merge_bioactivity_ontology(
        kg,
        {"bioactivity": {"edges": pd.DataFrame(columns=["edge_type"])}},
    )
    kg.triplets.create.assert_not_called()


def test_merge_ontology_returns_early_when_lookup_empty() -> None:
    """When entities have no bioactivity_concept keys, lookup is empty."""
    is_a = pd.DataFrame(
        [
            {
                "source_id": "bioactivity",
                "head_native_id": "B1",
                "tail_native_id": "B2",
                "edge_type": "is_a",
                "raw_attrs": {},
            }
        ]
    )
    no_concepts = pd.DataFrame(
        [{"entity_type": "food", "external_ids": {}}], index=["FOOD1"]
    )
    kg = _make_kg(no_concepts)
    merge_bioactivity_ontology(kg, {"bioactivity": {"edges": is_a}})
    kg.triplets.create.assert_not_called()
