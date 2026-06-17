"""End-to-end test for NewsletterRunner (curation disabled — no API calls)."""

import json

import pandas as pd
import pytest
from src.models.settings import KGCSettings
from src.pipeline.newsletter import runner as runner_mod
from src.pipeline.newsletter.runner import NewsletterRunner, _top_by_compounds
from src.pipeline.newsletter.statistics import FoodHighlight

_TRIPLET_COLS = ["head_id", "relationship_id", "tail_id", "attestation_ids"]
_ENTITY_COLS = ["foodatlas_id", "entity_type", "common_name", "synonyms"]
_EVIDENCE_COLS = ["evidence_id", "source_type", "reference"]
_ATTESTATION_COLS = ["attestation_id", "evidence_id"]


def _write_kg(path, *, triplets, entities, evidence, attestations) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(triplets, columns=_TRIPLET_COLS).to_parquet(path / "triplets.parquet")
    pd.DataFrame(entities, columns=_ENTITY_COLS).to_parquet(path / "entities.parquet")
    pd.DataFrame(evidence, columns=_EVIDENCE_COLS).to_parquet(path / "evidence.parquet")
    pd.DataFrame(attestations, columns=_ATTESTATION_COLS).to_parquet(
        path / "attestations.parquet"
    )


def _settings(tmp_path) -> KGCSettings:
    settings = KGCSettings(kg_dir=str(tmp_path / "kg"), data_dir=str(tmp_path / "data"))
    settings.pipeline.stages.newsletter.curate = False  # no LLM calls in tests
    return settings


def _seed_kgs(tmp_path) -> None:
    current = tmp_path / "kg"
    snapshot = tmp_path / "data" / "PreviousFAKG" / "20200101T000000Z"
    _write_kg(
        current,
        triplets=[
            ("f1", "r1", "c1", "[]"),
            ("f1", "r1", "c2", "[]"),
            ("f2", "r1", "c1", "[]"),
            ("f2", "r1", "c3", "[]"),
            ("c1", "r3", "d1", "[]"),
        ],
        entities=[
            ("f1", "food", "apple", "[]"),
            ("f2", "food", "pear", "[]"),
            ("c1", "chemical", "vitamin c", "[]"),
            ("c2", "chemical", "quercetin", "[]"),
            ("c3", "chemical", "lutein", "[]"),
            ("d1", "disease", "scurvy", "[]"),
        ],
        evidence=[("e1", "pubmed", '{"pmcid": "PMC1"}')],
        attestations=[],
    )
    _write_kg(
        snapshot,
        triplets=[("f1", "r1", "c1", "[]")],
        entities=[
            ("f1", "food", "apple", "[]"),
            ("c1", "chemical", "vitamin c", "[]"),
        ],
        evidence=[],
        attestations=[],
    )


def test_run_writes_newsletter_json(tmp_path) -> None:
    _seed_kgs(tmp_path)
    NewsletterRunner(_settings(tmp_path)).run()

    payload = json.loads((tmp_path / "kg" / "newsletter.json").read_text())
    assert set(payload) >= {
        "date",
        "atlas",
        "highlights",
        "most_characterized",
        "health_linked",
    }
    assert len(payload["atlas"]) == 5
    # curation off -> literal foodatlas names in the highlights
    names = [h["name"] for h in payload["highlights"]]
    assert "apple" in names or "pear" in names


def _hl(idx: int, chemicals: list[str]) -> FoodHighlight:
    return FoodHighlight(f"f{idx}", f"food{idx}", 1, 2, chemicals, [f"c{idx}"])


def test_top_by_compounds_ranks_by_distinct_count() -> None:
    items = [_hl(1, ["a"]), _hl(2, ["b", "c", "d"]), _hl(3, []), _hl(4, ["e", "f"])]
    out = _top_by_compounds(items, top_n=2)
    assert [h.food_id for h in out] == [
        "f2",
        "f4",
    ]  # 3 compounds, then 2; empty dropped


def test_run_raises_without_snapshot(tmp_path) -> None:
    (tmp_path / "kg").mkdir(parents=True)
    (tmp_path / "data").mkdir(parents=True)  # no PreviousFAKG/ inside
    with pytest.raises(FileNotFoundError):
        NewsletterRunner(_settings(tmp_path)).run()


def test_run_with_curation_invokes_agents(tmp_path, monkeypatch) -> None:
    calls = {"chems": 0}

    def fake_curate(highlights, _entities, **_kw):
        calls["chems"] += 1
        return highlights

    monkeypatch.setattr(runner_mod, "curate_chemicals", fake_curate)

    _seed_kgs(tmp_path)
    settings = _settings(tmp_path)
    settings.pipeline.stages.newsletter.curate = True
    NewsletterRunner(settings).run()

    assert (tmp_path / "kg" / "newsletter.json").exists()
    assert calls["chems"] == 1  # one curation pass over the highlights pool
