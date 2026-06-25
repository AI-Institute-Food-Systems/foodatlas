"""Tests for newsletter chemical curation (curation.py) with a mocked client."""

from types import SimpleNamespace

import pandas as pd
from src.pipeline.newsletter import curation
from src.pipeline.newsletter.curation import (
    _as_list,
    _format_entities,
    _name_map,
    _synonym_map,
    curate_chemicals,
)
from src.pipeline.newsletter.statistics import FoodHighlight

_ENTITY_COLS = ["foodatlas_id", "entity_type", "common_name", "synonyms"]


def _entities() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("f1", "food", "apple (raw)", '["apple", "malus"]'),
            ("c1", "chemical", "l-ascorbate", '["vitamin C", "ascorbic acid"]'),
        ],
        columns=_ENTITY_COLS,
    )


class _FakeClient:
    """Stand-in for anthropic.Anthropic returning a fixed chemical list."""

    def __init__(self, chemicals=None, *, raise_exc=False):
        parsed = SimpleNamespace(chemicals=list(chemicals or []))

        class _Messages:
            def parse(self, **_kwargs):
                if raise_exc:
                    raise RuntimeError("boom")
                return SimpleNamespace(parsed_output=parsed)

        self.messages = _Messages()


def _patch(monkeypatch, **kwargs) -> None:
    monkeypatch.setattr(curation.anthropic, "Anthropic", lambda: _FakeClient(**kwargs))


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def test_as_list_parses_json_string() -> None:
    assert _as_list('["a", "b"]') == ["a", "b"]


def test_as_list_handles_list_none_and_plain_string() -> None:
    assert _as_list(["x", "y"]) == ["x", "y"]
    assert _as_list(None) == []
    assert _as_list("plain") == ["plain"]


def test_format_entities_drops_synonym_equal_to_name() -> None:
    out = _format_entities(
        ["c1"], {"c1": "vitamin C"}, {"c1": ["vitamin C", "ascorbic acid"]}
    )
    assert out == "- vitamin C; ascorbic acid"


def test_format_entities_empty_ids() -> None:
    assert _format_entities([], {}, {}) == "(none)"


def test_name_and_synonym_maps() -> None:
    e = _entities()
    assert _name_map(e)["f1"] == "apple (raw)"
    assert _synonym_map(e)["c1"] == ["vitamin C", "ascorbic acid"]


# --------------------------------------------------------------------------
# curate_chemicals — per highlight; food_name is left untouched
# --------------------------------------------------------------------------
def test_curate_chemicals_replaces_chemicals_keeps_name(monkeypatch) -> None:
    _patch(monkeypatch, chemicals=["vitamin C"])
    h = FoodHighlight("f1", "apple", 1, 2, ["l-ascorbate"], ["c1"])
    out = curate_chemicals([h], _entities(), model="m", max_chemicals=5, max_workers=1)
    assert out[0].food_name == "apple"  # never rewritten
    assert out[0].new_chemicals == ["vitamin C"]


def test_curate_chemicals_empty() -> None:
    assert (
        curate_chemicals([], _entities(), model="m", max_chemicals=5, max_workers=1)
        == []
    )


def test_curate_chemicals_caps(monkeypatch) -> None:
    _patch(monkeypatch, chemicals=["a", "b", "c"])
    h = FoodHighlight("f1", "apple", 1, 2, ["x"], ["c1"])
    out = curate_chemicals([h], _entities(), model="m", max_chemicals=2, max_workers=1)
    assert out[0].new_chemicals == ["a", "b"]


def test_curate_chemicals_falls_back_on_error(monkeypatch) -> None:
    _patch(monkeypatch, chemicals=["y"], raise_exc=True)
    h = FoodHighlight("f1", "apple", 1, 2, ["raw chem"], ["c1"])
    out = curate_chemicals([h], _entities(), model="m", max_chemicals=5, max_workers=1)
    assert out[0].new_chemicals == ["raw chem"]
