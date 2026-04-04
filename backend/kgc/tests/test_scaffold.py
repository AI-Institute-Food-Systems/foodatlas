"""Tests for initialization scaffold."""

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from src.models.relationship import RelationshipType
from src.models.settings import KGCSettings
from src.pipeline.scaffold import (
    create_empty_entity_files,
    create_empty_triplet_files,
    ensure_registry_exists,
)
from src.stores.schema import (
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_EXTRACTIONS,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_REGISTRY,
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
    return KGCSettings(
        kg_dir=str(kg_dir),
        pipeline={"stages": {"kg_init": {"previous_kg_entities": ""}}},
    )


class TestCreateEmptyEntityFiles:
    def test_creates_entity_files(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_entity_files(settings)

        for f in (FILE_ENTITIES, FILE_LUT_FOOD, FILE_LUT_CHEMICAL):
            assert (kg_dir / f).exists(), f"Missing: {f}"

    def test_entities_is_empty_parquet(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_entity_files(settings)
        df = pd.read_parquet(kg_dir / FILE_ENTITIES)
        assert len(df) == 0

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
            FILE_EVIDENCE,
            FILE_EXTRACTIONS,
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
        df = pd.read_parquet(kg_dir / FILE_ENTITIES)
        assert len(df) == 0


class TestCreateEmptyTripletFiles:
    def test_creates_triplet_files(self, settings: KGCSettings, kg_dir: Path) -> None:
        create_empty_triplet_files(settings)

        for f in (
            FILE_RELATIONSHIPS,
            FILE_TRIPLETS,
            FILE_EVIDENCE,
            FILE_EXTRACTIONS,
            FILE_RETIRED,
        ):
            assert (kg_dir / f).exists(), f"Missing: {f}"

    def test_relationships_has_default_rows(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        df = pd.read_parquet(kg_dir / FILE_RELATIONSHIPS)
        assert len(df) == len(RelationshipType)
        assert df["foodatlas_id"].tolist() == [rt.value for rt in RelationshipType]

    def test_triplets_is_empty_parquet(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        df = pd.read_parquet(kg_dir / FILE_TRIPLETS)
        assert len(df) == 0

    def test_metadata_is_empty_parquet(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        df = pd.read_parquet(kg_dir / FILE_EVIDENCE)
        assert len(df) == 0

    def test_retired_is_empty_parquet(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        create_empty_triplet_files(settings)
        df = pd.read_parquet(kg_dir / FILE_RETIRED)
        assert len(df) == 0

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
        df = pd.read_parquet(kg_dir / FILE_TRIPLETS)
        assert len(df) == 0


class TestEnsureRegistryExists:
    def test_creates_empty_registry(self, settings: KGCSettings, kg_dir: Path) -> None:
        kg_dir.mkdir(parents=True, exist_ok=True)
        ensure_registry_exists(settings)
        path = kg_dir / FILE_REGISTRY
        assert path.exists()
        df = pd.read_parquet(path)
        assert len(df) == 0

    def test_seeds_from_previous_kg(self, kg_dir: Path, tmp_path: Path) -> None:
        prev_dir = tmp_path / "prev_kg"
        prev_dir.mkdir()
        tsv_path = prev_dir / "entities.tsv"
        tsv_path.write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name"
            "\tsynonyms\texternal_ids\n"
            "e5\tfood\tapple\t\t['apple']\t"
            "{'foodon': ['http://example.org/F5']}\n"
        )
        settings = KGCSettings(
            kg_dir=str(kg_dir),
            pipeline={"stages": {"kg_init": {"previous_kg_entities": str(tsv_path)}}},
        )
        ensure_registry_exists(settings)
        # Registry copied into new kg_dir
        df = pd.read_parquet(kg_dir / FILE_REGISTRY)
        assert len(df) == 1
        assert df.iloc[0]["foodatlas_id"] == "e5"
        # Registry also generated in previous KG folder
        assert (prev_dir / FILE_REGISTRY).exists()

    def test_reuses_existing_previous_kg_registry(
        self, kg_dir: Path, tmp_path: Path
    ) -> None:
        prev_dir = tmp_path / "prev_kg"
        prev_dir.mkdir()
        tsv_path = prev_dir / "entities.tsv"
        tsv_path.write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name"
            "\tsynonyms\texternal_ids\n"
            "e5\tfood\tapple\t\t['apple']\t"
            "{'foodon': ['http://example.org/F5']}\n"
        )
        # Pre-create a registry in the previous KG folder
        prev_registry = pd.DataFrame(
            [{"source": "foodon", "native_id": "PREBUILT", "foodatlas_id": "e99"}]
        )
        prev_registry.to_parquet(prev_dir / FILE_REGISTRY, index=False)

        settings = KGCSettings(
            kg_dir=str(kg_dir),
            pipeline={"stages": {"kg_init": {"previous_kg_entities": str(tsv_path)}}},
        )
        ensure_registry_exists(settings)
        # Should copy the pre-existing registry, not regenerate from TSV
        df = pd.read_parquet(kg_dir / FILE_REGISTRY)
        assert len(df) == 1
        assert df.iloc[0]["foodatlas_id"] == "e99"

    def test_does_not_reseed_existing_registry(
        self, kg_dir: Path, tmp_path: Path
    ) -> None:
        kg_dir.mkdir(parents=True, exist_ok=True)
        path = kg_dir / FILE_REGISTRY
        existing = pd.DataFrame(
            [{"source": "foodon", "native_id": "F1", "foodatlas_id": "e1"}]
        )
        existing.to_parquet(path, index=False)

        tsv_path = tmp_path / "prev_entities.tsv"
        tsv_path.write_text(
            "foodatlas_id\tentity_type\tcommon_name\tscientific_name"
            "\tsynonyms\texternal_ids\n"
            "e99\tfood\tbanana\t\t['banana']\t"
            "{'foodon': ['http://example.org/F99']}\n"
        )
        settings = KGCSettings(
            kg_dir=str(kg_dir),
            pipeline={"stages": {"kg_init": {"previous_kg_entities": str(tsv_path)}}},
        )
        ensure_registry_exists(settings)
        df = pd.read_parquet(path)
        assert len(df) == 1
        assert df.iloc[0]["foodatlas_id"] == "e1"

    def test_does_not_overwrite_existing(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        kg_dir.mkdir(parents=True, exist_ok=True)
        path = kg_dir / FILE_REGISTRY
        existing = pd.DataFrame(
            [{"source": "foodon", "native_id": "F1", "foodatlas_id": "e1"}]
        )
        existing.to_parquet(path, index=False)

        ensure_registry_exists(settings)
        df = pd.read_parquet(path)
        assert len(df) == 1

    def test_entity_files_do_not_touch_registry(
        self, settings: KGCSettings, kg_dir: Path
    ) -> None:
        kg_dir.mkdir(parents=True, exist_ok=True)
        path = kg_dir / FILE_REGISTRY
        existing = pd.DataFrame(
            [{"source": "foodon", "native_id": "F1", "foodatlas_id": "e1"}]
        )
        existing.to_parquet(path, index=False)

        create_empty_entity_files(settings)
        df = pd.read_parquet(path)
        assert len(df) == 1
