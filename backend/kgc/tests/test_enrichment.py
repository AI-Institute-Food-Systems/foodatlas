"""Tests for enrichment modules: common_name, synonyms_display, grouping."""

import pandas as pd
import pytest
from src.pipeline.enrichment.common_name import (
    _is_internal_mention,
    count_synonym_mentions,
    update_common_names,
)
from src.pipeline.enrichment.grouping.foods import clean_groups
from src.pipeline.enrichment.synonyms_display import (
    build_synonyms_display,
    remove_plural_synonyms,
)
from src.stores.schema import INDEX_COL

# ── common_name tests ────────────────────────────────────────────────


class TestIsInternalMention:
    def test_internal_prefix_detected(self) -> None:
        assert _is_internal_mention("_FOODON_ID:12345")

    def test_normal_mention_not_internal(self) -> None:
        assert not _is_internal_mention("apple")

    def test_partial_prefix_not_internal(self) -> None:
        assert not _is_internal_mention("FOODON_ID:12345")


class TestCountSynonymMentions:
    @pytest.fixture()
    def entities(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    INDEX_COL: "e0",
                    "entity_type": "food",
                    "common_name": "apple",
                    "synonyms": ["apple", "apples", "malus domestica"],
                    "external_ids": {},
                },
                {
                    INDEX_COL: "e1",
                    "entity_type": "chemical",
                    "common_name": "vitamin c",
                    "synonyms": ["vitamin c", "ascorbic acid"],
                    "external_ids": {},
                },
            ]
        ).set_index(INDEX_COL)

    @pytest.fixture()
    def triplets(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    INDEX_COL: "t0",
                    "head_id": "e0",
                    "relationship_id": "r1",
                    "tail_id": "e1",
                    "metadata_ids": ["mc0", "mc1"],
                },
            ]
        ).set_index(INDEX_COL)

    @pytest.fixture()
    def metadata(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    INDEX_COL: "mc0",
                    "_food_name": "apple",
                    "_chemical_name": "vitamin c",
                },
                {
                    INDEX_COL: "mc1",
                    "_food_name": "apple",
                    "_chemical_name": "ascorbic acid",
                },
            ]
        ).set_index(INDEX_COL)

    def test_counts_mentions(
        self,
        entities: pd.DataFrame,
        triplets: pd.DataFrame,
        metadata: pd.DataFrame,
    ) -> None:
        result = count_synonym_mentions(entities, triplets, metadata)
        assert result.at["e0", "synonym_counts"]["apple"] == 2
        assert result.at["e0", "synonym_counts"]["malus domestica"] == 0
        assert result.at["e1", "synonym_counts"]["vitamin c"] == 1
        assert result.at["e1", "synonym_counts"]["ascorbic acid"] == 1

    def test_skips_internal_mentions(self, entities: pd.DataFrame) -> None:
        triplets = pd.DataFrame(
            [
                {
                    INDEX_COL: "t0",
                    "head_id": "e0",
                    "relationship_id": "r1",
                    "tail_id": "e1",
                    "metadata_ids": ["mc0"],
                },
            ]
        ).set_index(INDEX_COL)
        metadata = pd.DataFrame(
            [
                {
                    INDEX_COL: "mc0",
                    "_food_name": "_FOODON_ID:123",
                    "_chemical_name": "vitamin c",
                },
            ]
        ).set_index(INDEX_COL)
        result = count_synonym_mentions(entities, triplets, metadata)
        assert result.at["e0", "synonym_counts"]["apple"] == 0


class TestUpdateCommonNames:
    def test_picks_most_frequent(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e0",
                    "common_name": "old name",
                    "synonym_counts": {"apple": 5, "apples": 2},
                },
            ]
        ).set_index(INDEX_COL)
        result = update_common_names(entities)
        assert result.at["e0", "common_name"] == "apple"
        assert "synonym_counts" not in result.columns

    def test_keeps_existing_when_no_mentions(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e0",
                    "common_name": "original",
                    "synonym_counts": {"original": 0, "alt": 0},
                },
            ]
        ).set_index(INDEX_COL)
        result = update_common_names(entities)
        assert result.at["e0", "common_name"] == "original"


# ── synonyms_display tests ──────────────────────────────────────────


class TestRemovePluralSynonyms:
    def test_single_synonym_unchanged(self) -> None:
        assert remove_plural_synonyms(["apple"]) == ["apple"]

    def test_removes_plural_suffix(self) -> None:
        result = remove_plural_synonyms(["apple", "fruit", "apples", "fruits"])
        assert result == ["apple", "fruit"]

    def test_no_plurals_found_returns_all(self) -> None:
        result = remove_plural_synonyms(["water", "h2o"])
        assert result == ["water", "h2o"]

    def test_singularize_match(self) -> None:
        result = remove_plural_synonyms(["apples", "apple"])
        assert result == ["apples"]


class TestBuildSynonymsDisplay:
    def test_food_with_foodon(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e0",
                    "entity_type": "food",
                    "synonyms": [
                        "apple",
                        "fruit",
                        "apples",
                        "fruits",
                    ],
                    "external_ids": {"foodon": ["FOODON:123"]},
                },
            ]
        ).set_index(INDEX_COL)
        result = build_synonyms_display(entities)
        assert result.iloc[0] == {"foodon": ["apple", "fruit"]}

    def test_food_without_foodon(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e0",
                    "entity_type": "food",
                    "synonyms": ["apple"],
                    "external_ids": {},
                },
            ]
        ).set_index(INDEX_COL)
        result = build_synonyms_display(entities)
        assert result.iloc[0] == {}

    def test_chemical_with_chebi(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e1",
                    "entity_type": "chemical",
                    "synonyms": ["vitamin c", "ascorbic acid"],
                    "external_ids": {"chebi": [12345]},
                },
            ]
        ).set_index(INDEX_COL)
        result = build_synonyms_display(entities)
        expected = {"chebi": ["vitamin c", "ascorbic acid"]}
        assert result.iloc[0] == expected

    def test_chemical_with_mesh(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e1",
                    "entity_type": "chemical",
                    "synonyms": ["caffeine"],
                    "external_ids": {"mesh": ["D002110"]},
                },
            ]
        ).set_index(INDEX_COL)
        mesh = pd.DataFrame(
            [
                {
                    "id": "D002110",
                    "synonyms": [
                        "Caffeine",
                        "1,3,7-Trimethylxanthine",
                    ],
                },
            ]
        ).set_index("id")
        result = build_synonyms_display(entities, mesh=mesh)
        expected = ["Caffeine", "1,3,7-Trimethylxanthine"]
        assert result.iloc[0]["mesh"] == expected

    def test_chemical_with_mesh_missing_id(self) -> None:
        entities = pd.DataFrame(
            [
                {
                    INDEX_COL: "e1",
                    "entity_type": "chemical",
                    "synonyms": ["unknown"],
                    "external_ids": {"mesh": ["D999999"]},
                },
            ]
        ).set_index(INDEX_COL)
        mesh = pd.DataFrame([{"id": "D002110", "synonyms": ["Caffeine"]}]).set_index(
            "id"
        )
        result = build_synonyms_display(entities, mesh=mesh)
        assert "mesh" not in result.iloc[0]


# ── food grouping tests ─────────────────────────────────────────────


class TestCleanGroups:
    def test_single_group_unchanged(self) -> None:
        assert clean_groups(["fruit"]) == ["fruit"]

    def test_empty_becomes_unclassified(self) -> None:
        assert clean_groups([]) == ["unclassified"]

    def test_other_plant_removed_when_specific(self) -> None:
        assert clean_groups(["fruit", "other plant"]) == ["fruit"]

    def test_other_animal_removed_when_specific(self) -> None:
        result = clean_groups(["seafood", "other animal"])
        assert result == ["seafood"]

    def test_vegetable_removed_when_fruit_present(self) -> None:
        assert clean_groups(["fruit", "vegetable"]) == ["fruit"]

    def test_plant_seed_removed_when_legume(self) -> None:
        result = clean_groups(["legume", "plant seed or nut"])
        assert result == ["legume"]

    def test_mammalian_removed_when_avian(self) -> None:
        result = clean_groups(["avian", "mammalian meat"])
        assert result == ["avian"]

    def test_ambiguous_becomes_unclassified(self) -> None:
        assert clean_groups(["fruit", "dairy"]) == ["unclassified"]

    def test_multi_specific_resolved(self) -> None:
        result = clean_groups(["legume", "plant seed or nut", "other plant"])
        assert result == ["legume"]
