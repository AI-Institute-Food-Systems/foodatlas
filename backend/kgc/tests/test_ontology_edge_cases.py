"""Cover the early-return branches of the three ontology mergers
(disease/food/chemical) — each returns early when no source is provided
OR when the entity lookup is empty."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
from src.pipeline.triplets.chemical_chemical.chebi import merge_chemical_ontology
from src.pipeline.triplets.disease_disease.ctd import merge_disease_ontology
from src.pipeline.triplets.food_food.foodon import merge_food_ontology


def _kg_empty() -> MagicMock:
    """KG with zero entities — lookups will be empty."""
    kg = MagicMock()
    kg.entities._entities = pd.DataFrame(
        columns=["entity_type", "external_ids"]
    ).astype({"external_ids": "object"})
    return kg


def test_disease_ontology_returns_early_when_no_ctd_source() -> None:
    kg = _kg_empty()
    merge_disease_ontology(kg, {})
    kg.triplets.create.assert_not_called()


def test_disease_ontology_returns_early_on_empty_lookup() -> None:
    kg = _kg_empty()
    edges = pd.DataFrame(
        [{"edge_type": "is_a", "head_native_id": "D1", "tail_native_id": "D2"}]
    )
    merge_disease_ontology(kg, {"ctd": {"edges": edges}})
    kg.triplets.create.assert_not_called()


def test_food_ontology_returns_early_when_no_foodon_source() -> None:
    kg = _kg_empty()
    merge_food_ontology(kg, {})
    kg.triplets.create.assert_not_called()


def test_food_ontology_returns_early_on_empty_lookup() -> None:
    kg = _kg_empty()
    edges = pd.DataFrame(
        [{"edge_type": "is_a", "head_native_id": "F1", "tail_native_id": "F2"}]
    )
    merge_food_ontology(kg, {"foodon": {"edges": edges}})
    kg.triplets.create.assert_not_called()


def test_chebi_ontology_returns_early_when_no_chebi_source() -> None:
    kg = _kg_empty()
    merge_chemical_ontology(kg, {})
    kg.triplets.create.assert_not_called()


def test_chebi_ontology_returns_early_on_empty_lookup() -> None:
    kg = _kg_empty()
    # ChEBI native ids are numeric (e.g. "30806") — adapter coerces via int.
    edges = pd.DataFrame(
        [{"edge_type": "is_a", "head_native_id": "1", "tail_native_id": "2"}]
    )
    merge_chemical_ontology(kg, {"chebi": {"edges": edges}})
    kg.triplets.create.assert_not_called()
