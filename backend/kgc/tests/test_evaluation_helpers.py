"""Tests for the small pure helpers used by the evaluation stage: cost
accounting (Usage), prompt formatting, and the papers I/O edge."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from src.pipeline.evaluation.cost import Usage, usage_from_response
from src.pipeline.evaluation.prompts import format_pairs
from src.pipeline.newsletter.papers import PaperMeta, fetch_paper_meta


class TestUsage:
    def test_add_combines_all_token_counts(self) -> None:
        a = Usage(calls=1, input_tokens=10, output_tokens=2, cache_read_tokens=1)
        b = Usage(calls=2, input_tokens=5, output_tokens=3, cache_write_tokens=4)
        total = a + b
        assert total.calls == 3
        assert total.input_tokens == 15
        assert total.output_tokens == 5
        assert total.cache_read_tokens == 1
        assert total.cache_write_tokens == 4

    def test_cost_usd_applies_per_million_pricing(self) -> None:
        u = Usage(
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            cache_read_tokens=1_000_000,
            cache_write_tokens=1_000_000,
        )
        # 5 + 25 + 0.5 + 6.25
        assert u.cost_usd == 36.75

    def test_usage_from_response_handles_missing_cache_fields(self) -> None:
        resp = SimpleNamespace(
            usage=SimpleNamespace(input_tokens=100, output_tokens=20)
        )
        u = usage_from_response(resp)
        assert u.calls == 1
        assert u.input_tokens == 100
        assert u.output_tokens == 20
        assert u.cache_read_tokens == 0
        assert u.cache_write_tokens == 0

    def test_usage_from_response_picks_up_cache_fields(self) -> None:
        resp = SimpleNamespace(
            usage=SimpleNamespace(
                input_tokens=10,
                output_tokens=2,
                cache_read_input_tokens=3,
                cache_creation_input_tokens=4,
            )
        )
        u = usage_from_response(resp)
        assert u.cache_read_tokens == 3
        assert u.cache_write_tokens == 4

    def test_usage_from_response_treats_none_as_zero(self) -> None:
        resp = SimpleNamespace(
            usage=SimpleNamespace(input_tokens=None, output_tokens=None)
        )
        u = usage_from_response(resp)
        assert u.input_tokens == 0
        assert u.output_tokens == 0


class TestFormatPairs:
    def test_empty(self) -> None:
        assert format_pairs([]) == "(none)"

    def test_numbered_with_concentration(self) -> None:
        out = format_pairs([("tomato", "lycopene", "5 mg/g"), ("apple", "quercetin", "")])
        assert "1. tomato -> lycopene @ 5 mg/g" in out
        assert "2. apple -> quercetin\n" in out + "\n"
        # Second line has no @ when concentration absent
        assert "@ " not in out.split("\n")[1]


class TestFetchPaperMeta:
    def test_missing_file_returns_empty_meta(self, tmp_path: Path) -> None:
        result = fetch_paper_meta(["999"], tmp_path)
        assert result["999"] == PaperMeta(title=None, doi=None)

    def test_reads_title_from_bioc_json(self, tmp_path: Path) -> None:
        (tmp_path / "PMC123.xml").write_text(
            json.dumps(
                {
                    "documents": [
                        {
                            "passages": [
                                {
                                    "infons": {
                                        "section_type": "TITLE",
                                        "article-id_doi": "10.1/abc",
                                    },
                                    "text": "  My Title  ",
                                }
                            ]
                        }
                    ]
                }
            )
        )
        result = fetch_paper_meta(["123"], tmp_path)
        meta = result["123"]
        assert meta.title == "My Title"
        assert meta.doi == "10.1/abc"

    def test_no_title_passage_returns_empty(self, tmp_path: Path) -> None:
        (tmp_path / "PMC456.xml").write_text(
            json.dumps(
                {
                    "documents": [
                        {
                            "passages": [
                                {"infons": {"section_type": "ABSTRACT"}, "text": "x"}
                            ]
                        }
                    ]
                }
            )
        )
        result = fetch_paper_meta(["456"], tmp_path)
        assert result["456"] == PaperMeta(title=None, doi=None)

    def test_empty_documents_returns_empty(self, tmp_path: Path) -> None:
        (tmp_path / "PMC789.xml").write_text(json.dumps({"documents": []}))
        result = fetch_paper_meta(["789"], tmp_path)
        assert result["789"] == PaperMeta(title=None, doi=None)
