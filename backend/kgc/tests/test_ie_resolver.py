"""Tests for ie_resolver — LUT-only entity resolution."""

import pandas as pd
from src.pipeline.triplets.ie_resolver import resolve_ie_metadata
from src.stores.entity_store import EntityStore


class TestResolveIeMetadata:
    @staticmethod
    def _make_metadata(pairs: list[tuple[str, str]]) -> pd.DataFrame:
        rows = [
            {
                "_food_name": food,
                "_chemical_name": chem,
                "_food_part": "",
                "_conc": "",
                "conc_value": None,
                "conc_unit": "",
                "food_part": "",
                "food_processing": "",
                "source": "lit2kg",
                "reference": [f"pmcid:{i}"],
                "entity_linking_method": "ie_lut_lookup",
                "quality_score": 0.99,
            }
            for i, (food, chem) in enumerate(pairs)
        ]
        return pd.DataFrame(rows)

    def test_resolves_known_entities(self, entity_store_populated: EntityStore) -> None:
        meta = self._make_metadata([("apple", "vitamin c")])
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert len(result.resolved) == 1
        assert result.resolved.iloc[0]["head_id"] == "e0"
        assert result.resolved.iloc[0]["tail_id"] == "e1"

    def test_unresolved_food_tracked(self, entity_store_populated: EntityStore) -> None:
        meta = self._make_metadata([("banana", "vitamin c")])
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert result.resolved.empty
        assert "banana" in result.unresolved_food
        assert len(result.unresolved_chemical) == 0

    def test_unresolved_chemical_tracked(
        self, entity_store_populated: EntityStore
    ) -> None:
        meta = self._make_metadata([("apple", "potassium")])
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert result.resolved.empty
        assert "potassium" in result.unresolved_chemical

    def test_both_unresolved(self, entity_store_populated: EntityStore) -> None:
        meta = self._make_metadata([("banana", "potassium")])
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert result.resolved.empty
        assert "banana" in result.unresolved_food
        assert "potassium" in result.unresolved_chemical

    def test_mixed_resolved_and_unresolved(
        self, entity_store_populated: EntityStore
    ) -> None:
        meta = self._make_metadata(
            [
                ("apple", "vitamin c"),
                ("banana", "potassium"),
            ]
        )
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert len(result.resolved) == 1
        assert result.stats["resolved_rows"] == 1
        assert result.stats["dropped_rows"] == 1

    def test_ambiguous_explodes(self, entity_store_populated: EntityStore) -> None:
        # Add ambiguity: "fruit" maps to two food entities.
        entity_store_populated._lut_food["fruit"] = ["e0", "e99"]
        meta = self._make_metadata([("fruit", "vitamin c")])
        result = resolve_ie_metadata(meta, entity_store_populated)

        # Should produce 2 rows (one per head_id).
        assert len(result.resolved) == 2

    def test_stats_correctness(self, entity_store_populated: EntityStore) -> None:
        meta = self._make_metadata(
            [
                ("apple", "vitamin c"),
                ("apple", "ascorbic acid"),
                ("banana", "potassium"),
            ]
        )
        result = resolve_ie_metadata(meta, entity_store_populated)

        assert result.stats["total_ie_rows"] == 3
        assert result.stats["unique_food_names"] == 2
        assert result.stats["unique_chemical_names"] == 3
        assert result.stats["unresolved_food_names"] == 1
        assert result.stats["resolved_food_names"] == 1

    def test_empty_metadata(self, entity_store_populated: EntityStore) -> None:
        result = resolve_ie_metadata(pd.DataFrame(), entity_store_populated)
        assert result.resolved.empty
        assert result.stats["total_ie_rows"] == 0

    def test_relationship_id_set(self, entity_store_populated: EntityStore) -> None:
        meta = self._make_metadata([("apple", "vitamin c")])
        result = resolve_ie_metadata(meta, entity_store_populated)
        assert result.resolved.iloc[0]["relationship_id"] == "r1"
