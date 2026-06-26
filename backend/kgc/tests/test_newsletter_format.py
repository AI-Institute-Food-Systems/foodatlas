"""Tests for the newsletter JSON payload builder (format.build_payload)."""

from src.pipeline.newsletter.format import build_payload
from src.pipeline.newsletter.statistics import (
    FoodHighlight,
    FoodRank,
    Headline,
    NewsletterStats,
)


def _headline(associations: int, foods: int, pubs: int) -> Headline:
    return Headline(
        foods=foods,
        chemicals=100,
        diseases=10,
        associations=associations,
        publications=pubs,
        food_chemical=associations - 5,
        chemical_disease=3,
        is_a=2,
    )


def _stats(**overrides) -> NewsletterStats:
    base = {
        "previous": _headline(100, 50, 1000),
        "current": _headline(130, 55, 1100),
        "new_food_chemical": 25,
        "foods_touched": 5,
        "chemicals_touched": 20,
        "new_papers": 12,
        "highlights": [
            FoodHighlight("f1", "apple", 10, 40, ["vitamin C", "quercetin"], ["c1"]),
        ],
        "most_characterized": [FoodRank("f3", "milk", 8028)],
        "health_linked": [FoodRank("f4", "tomato", 1164)],
    }
    base.update(overrides)
    return NewsletterStats(**base)


def test_payload_headline_and_atlas() -> None:
    p = build_payload(_stats(), "June 11, 2026")
    assert p["date"] == "June 11, 2026"
    assert p["new_associations"] == 30  # 130 - 100
    assert p["new_food_chemical"] == 25
    assert p["new_papers"] == 12
    assert len(p["atlas"]) == 5
    assert p["atlas"][0] == {"label": "Associations", "value": 130, "delta": 30}
    assert p["atlas"][1] == {"label": "Foods", "value": 55, "delta": 5}


def test_payload_highlights_count_compounds_and_link() -> None:
    p = build_payload(_stats(), "June 11, 2026")
    assert p["highlights"][0] == {
        "name": "apple",
        "url": "https://www.foodatlas.ai/food/f1",
        "count": 2,  # number of distinct compounds
        "chemicals": ["vitamin C", "quercetin"],
    }


def test_payload_leaderboards() -> None:
    p = build_payload(_stats(), "June 11, 2026")
    assert p["most_characterized"][0] == {
        "name": "milk",
        "url": "https://www.foodatlas.ai/food/f3",
        "value": 8028,
    }
    assert p["health_linked"][0]["name"] == "tomato"


def test_payload_has_no_quality_field() -> None:
    assert "quality" not in build_payload(_stats(), "d")
