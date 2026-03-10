"""Tests for initialization scaffold."""

import json
from pathlib import Path
from typing import Any

import pytest
from src.integration.scaffold import (
    create_empty_entity_files,
    create_empty_triplet_files,
)
from src.models.relationship import RelationshipType
from src.models.settings import KGCSettings
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_METADATA_CONTAINS,
    FILE_RELATIONSHIPS,
    FILE_RETIRED,
    FILE_TRIPLETS,
)


def _load_json(path: Path) -> Any:
    with path.open() as f:
        return json.load(f)


@pytest.fixture()
def kg_dir(tmp_path: Path) -> Path:
    return tmp_path / "kg"


@pytest.fixture()
def settings(kg_dir: Path) -> KGCSettings:
    return KGCSettings(kg_dir=str(kg_dir))


class TestCreateEmptyEntityFiles:
    def test_creates_entity_files(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_entity_files(settings)

        for f in (FILE_ENTITIES, FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
            assert (kg_dir / f).exists(), f"Missing: {f}"

    def test_entities_is_empty_list(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_entity_files(settings)
        data = _load_json(kg_dir / FILE_ENTITIES)
        assert data == []

    def test_lut_files_are_empty_json(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_entity_files(settings)
        for f in (FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
            data = _load_json(kg_dir / f)
            assert data == {}

    def test_does_not_create_triplet_files(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_entity_files(settings)
        for f in (
            FILE_TRIPLETS,
            FILE_METADATA_CONTAINS,
            FILE_RELATIONSHIPS,
            FILE_RETIRED,
        ):
            assert not (kg_dir / f).exists(), f"Should not exist: {f}"

    def test_creates_missing_directory(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        assert not kg_dir.exists()
        create_empty_entity_files(settings)
        assert kg_dir.exists()

    def test_idempotent(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_entity_files(settings)
        create_empty_entity_files(settings)
        data = _load_json(kg_dir / FILE_ENTITIES)
        assert data == []


class TestCreateEmptyTripletFiles:
    def test_creates_triplet_files(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)

        for f in (
            FILE_RELATIONSHIPS,
            FILE_TRIPLETS,
            FILE_METADATA_CONTAINS,
            FILE_RETIRED,
        ):
            assert (kg_dir / f).exists(), f"Missing: {f}"

    def test_relationships_has_default_rows(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        data = _load_json(kg_dir / FILE_RELATIONSHIPS)
        assert len(data) == len(RelationshipType)
        assert [r["foodatlas_id"] for r in data] == [
            rt.value for rt in RelationshipType
        ]

    def test_triplets_is_empty_list(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)
        data = _load_json(kg_dir / FILE_TRIPLETS)
        assert data == []

    def test_metadata_is_empty_list(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)
        data = _load_json(kg_dir / FILE_METADATA_CONTAINS)
        assert data == []

    def test_retired_is_empty_list(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)
        data = _load_json(kg_dir / FILE_RETIRED)
        assert data == []

    def test_does_not_create_entity_files(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        for f in (FILE_ENTITIES, FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
            assert not (kg_dir / f).exists(), f"Should not exist: {f}"

    def test_creates_missing_directory(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        assert not kg_dir.exists()
        create_empty_triplet_files(settings)
        assert kg_dir.exists()

    def test_idempotent(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)
        create_empty_triplet_files(settings)
        data = _load_json(kg_dir / FILE_TRIPLETS)
        assert data == []
