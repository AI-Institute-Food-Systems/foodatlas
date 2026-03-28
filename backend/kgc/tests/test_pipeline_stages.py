"""Tests for pipeline stages enum."""

from src.pipeline.stages import ALL_STAGES, PipelineStage


def test_all_stages_present() -> None:
    assert len(PipelineStage) == 6


def test_stage_names() -> None:
    expected = {
        "DATA_CLEANING",
        "ENTITY_INIT",
        "TRIPLET_INIT",
        "METADATA_PROCESSING",
        "TRIPLET_EXPANSION",
        "POSTPROCESSING",
    }
    assert {s.name for s in PipelineStage} == expected


def test_all_stages_sorted_by_value() -> None:
    values = [s.value for s in ALL_STAGES]
    assert values == sorted(values)


def test_all_stages_contains_every_member() -> None:
    assert set(ALL_STAGES) == set(PipelineStage)
