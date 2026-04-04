"""Tests for registry seeding from previous KG entities TSV."""

from pathlib import Path

import pandas as pd
import pytest
from src.stores.entity_registry import EntityRegistry
from src.stores.registry_seeder import extract_registry_pairs, seed_registry
from src.stores.schema import REGISTRY_COLUMNS


@pytest.fixture()
def empty_registry(tmp_path: Path) -> EntityRegistry:
    path = tmp_path / "entity_registry.parquet"
    pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)
    return EntityRegistry(path)


def _write_tsv(path: Path, rows: list[dict]) -> Path:
    tsv = path / "entities.tsv"
    df = pd.DataFrame(rows)
    df.to_csv(tsv, sep="\t", index=False)
    return tsv


class TestExtractRegistryPairs:
    def test_food_with_foodon(self) -> None:
        ext = {"foodon": ["http://purl.obolibrary.org/obo/FOODON_00001001"]}
        pairs = extract_registry_pairs("food", ext)
        assert len(pairs) == 1
        source, native, is_primary = pairs[0]
        assert source == "foodon"
        assert native == "http://purl.obolibrary.org/obo/FOODON_00001001"
        assert is_primary is True

    def test_food_with_fdc(self) -> None:
        ext = {"fdc": [325871]}
        pairs = extract_registry_pairs("food", ext)
        assert len(pairs) == 1
        source, native, is_primary = pairs[0]
        assert source == "fdc"
        assert native == "325871"
        assert is_primary is False

    def test_food_with_foodon_and_fdc(self) -> None:
        ext = {"foodon": ["http://example.org/F1"], "fdc": [100, 200]}
        pairs = extract_registry_pairs("food", ext)
        sources = [(s, n) for s, n, _ in pairs]
        assert ("foodon", "http://example.org/F1") in sources
        assert ("fdc", "100") in sources
        assert ("fdc", "200") in sources

    def test_chemical_with_chebi(self) -> None:
        ext = {"chebi": [9349]}
        pairs = extract_registry_pairs("chemical", ext)
        assert len(pairs) == 1
        assert pairs[0] == ("chebi", "9349", True)

    def test_chemical_with_cdno(self) -> None:
        ext = {"cdno": ["CDNO_001"]}
        pairs = extract_registry_pairs("chemical", ext)
        assert pairs[0] == ("cdno", "CDNO_001", False)

    def test_chemical_with_fdc_nutrient(self) -> None:
        ext = {"fdc_nutrient": [1003]}
        pairs = extract_registry_pairs("chemical", ext)
        assert pairs[0] == ("fdc_nutrient", "1003", False)

    def test_disease_with_mesh(self) -> None:
        ext = {"mesh": ["D000006"]}
        pairs = extract_registry_pairs("disease", ext)
        assert len(pairs) == 1
        assert pairs[0] == ("ctd", "MESH:D000006", True)

    def test_unknown_keys_skipped(self) -> None:
        ext = {"pubchem_compound": [6213], "dmd": ["DMD302680"]}
        pairs = extract_registry_pairs("chemical", ext)
        assert len(pairs) == 0

    def test_empty_dict(self) -> None:
        assert extract_registry_pairs("food", {}) == []

    def test_flavor_returns_nothing(self) -> None:
        ext = {"some_key": ["val"]}
        assert extract_registry_pairs("flavor", ext) == []

    def test_multi_value_primary_only_first(self) -> None:
        ext = {"fdc": [100, 200, 300]}
        pairs = extract_registry_pairs("food", ext)
        primaries = [p for p in pairs if p[2]]
        aliases = [p for p in pairs if not p[2]]
        assert len(primaries) == 0  # fdc for food is_primary=False
        assert len(aliases) == 3

    def test_multi_chebi_first_is_primary(self) -> None:
        ext = {"chebi": [111, 222]}
        pairs = extract_registry_pairs("chemical", ext)
        assert pairs[0] == ("chebi", "111", True)
        assert pairs[1] == ("chebi", "222", False)


class TestSeedRegistry:
    def test_seeds_food_entities(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "apple",
                    "scientific_name": "",
                    "synonyms": "['apple']",
                    "external_ids": "{'foodon': ['http://example.org/FOOD_001']}",
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 1
        assert empty_registry.resolve("foodon", "http://example.org/FOOD_001") == "e1"

    def test_seeds_chemical_entities(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e100",
                    "entity_type": "chemical",
                    "common_name": "water",
                    "scientific_name": "",
                    "synonyms": "['water']",
                    "external_ids": "{'chebi': [15377]}",
                },
            ],
        )
        seed_registry(empty_registry, tsv)
        assert empty_registry.resolve("chebi", "15377") == "e100"

    def test_seeds_disease_with_mesh_prefix(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e200",
                    "entity_type": "disease",
                    "common_name": "flu",
                    "scientific_name": "",
                    "synonyms": "['flu']",
                    "external_ids": "{'mesh': ['D000006']}",
                },
            ],
        )
        seed_registry(empty_registry, tsv)
        assert empty_registry.resolve("ctd", "MESH:D000006") == "e200"

    def test_next_eid_reflects_max(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e500",
                    "entity_type": "food",
                    "common_name": "banana",
                    "scientific_name": "",
                    "synonyms": "['banana']",
                    "external_ids": "{'foodon': ['http://example.org/F500']}",
                },
                {
                    "foodatlas_id": "e10",
                    "entity_type": "chemical",
                    "common_name": "salt",
                    "scientific_name": "",
                    "synonyms": "['salt']",
                    "external_ids": "{'chebi': [26710]}",
                },
            ],
        )
        seed_registry(empty_registry, tsv)
        assert empty_registry.next_eid == 501

    def test_returns_added_count(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "a",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "{'foodon': ['http://x/F1'], 'fdc': [100]}",
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 2

    def test_skips_malformed_external_ids(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "bad",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "NOT_VALID_PYTHON",
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "food",
                    "common_name": "good",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "{'foodon': ['http://x/F2']}",
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 1
        assert empty_registry.resolve("foodon", "http://x/F2") == "e2"

    def test_skips_empty_external_ids(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "flavor",
                    "common_name": "sweet",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "{}",
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 0

    def test_duplicate_key_skipped(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e1",
                    "entity_type": "food",
                    "common_name": "a",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "{'foodon': ['http://x/F1']}",
                },
                {
                    "foodatlas_id": "e2",
                    "entity_type": "food",
                    "common_name": "b",
                    "scientific_name": "",
                    "synonyms": "[]",
                    "external_ids": "{'foodon': ['http://x/F1']}",
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 1
        assert empty_registry.resolve("foodon", "http://x/F1") == "e1"

    def test_multi_source_entity(
        self, empty_registry: EntityRegistry, tmp_path: Path
    ) -> None:
        tsv = _write_tsv(
            tmp_path,
            [
                {
                    "foodatlas_id": "e42",
                    "entity_type": "chemical",
                    "common_name": "caffeine",
                    "scientific_name": "",
                    "synonyms": "['caffeine']",
                    "external_ids": (
                        "{'chebi': [27732], 'pubchem_compound': [2519],"
                        " 'mesh': ['D002110']}"
                    ),
                },
            ],
        )
        count = seed_registry(empty_registry, tsv)
        assert count == 1
        assert empty_registry.resolve("chebi", "27732") == "e42"
        # pubchem and mesh are not mapped for chemicals
        assert empty_registry.resolve("pubchem_compound", "2519") == ""
