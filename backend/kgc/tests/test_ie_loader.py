"""Tests for ie_loader — raw TSV parsing and name standardization."""

from pathlib import Path

import pandas as pd
import pytest
from src.pipeline.ie.loader import (
    _parse_tuple,
    load_ie_raw,
    standardize_name,
)


class TestStandardizeName:
    def test_greek_beta(self) -> None:
        assert standardize_name("β-carotene") == "beta-carotene"

    def test_greek_alpha(self) -> None:
        assert standardize_name("α-tocopherol") == "alpha-tocopherol"

    def test_unicode_hyphen(self) -> None:
        assert standardize_name("3,4\u2010dihydroxy") == "3,4-dihydroxy"

    def test_combined(self) -> None:
        assert standardize_name("  β\u2013Sitosterol  ") == "beta-sitosterol"

    def test_plain_name_lowered(self) -> None:
        assert standardize_name("Quercetin") == "quercetin"


class TestParseTuple:
    def test_simple(self) -> None:
        result = _parse_tuple("(apricot, fruits, anthocyanins, )")
        assert result == ("apricot", "fruits", "anthocyanins", "")

    def test_empty_food_part(self) -> None:
        result = _parse_tuple("(Curcuma, , curcumin, )")
        assert result == ("Curcuma", "", "curcumin", "")

    def test_comma_in_chemical(self) -> None:
        result = _parse_tuple("(olive oil, , 3,4-dihydroxyphenylethanol | DOPET, )")
        assert result is not None
        food, part, chem, qty = result
        assert food == "olive oil"
        assert part == ""
        assert chem == "3,4-dihydroxyphenylethanol"
        assert qty == ""

    def test_pipe_takes_first_alias(self) -> None:
        result = _parse_tuple("(apple, , Rutin | vitamin P | rutoside, )")
        assert result is not None
        assert result[2] == "Rutin"

    def test_with_quantity(self) -> None:
        result = _parse_tuple("(apple, peel, vitamin c, 1.5mg/g)")
        assert result is not None
        assert result[3] == "1.5mg/g"

    def test_empty_line(self) -> None:
        assert _parse_tuple("") is None
        assert _parse_tuple("()") is None

    def test_missing_chemical(self) -> None:
        assert _parse_tuple("(apple, , , )") is None

    def test_too_few_fields(self) -> None:
        assert _parse_tuple("(only_one)") is None


class TestLoadIeRaw:
    @pytest.fixture()
    def ie_tsv(self, tmp_path: Path) -> Path:
        path = tmp_path / "ie.tsv"
        # Multi-tuple responses use quoted fields with embedded newlines.
        rows = [
            {
                "pmcid": "111",
                "section": "INTRO",
                "matched_query": "apple",
                "sentence": "Apples have vitamin C.",
                "prob": 0.99,
                "response": "(apple, peel, β-carotene, )",
            },
            {
                "pmcid": "222",
                "section": "RESULTS",
                "matched_query": "olive",
                "sentence": "Olive has polyphenols.",
                "prob": 0.80,
                "response": "(olive, , polyphenols, )",
            },
            {
                "pmcid": "333",
                "section": "INTRO",
                "matched_query": "berry",
                "sentence": "Berries are healthy.",
                "prob": 0.98,
                "response": "(berry, , anthocyanins, )\n(berry, , quercetin, )",
            },
        ]
        pd.DataFrame(rows).to_csv(path, sep="\t", index=False)
        return path

    def test_greek_normalized(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent)
        assert "beta-carotene" in df["_chemical_name"].values

    def test_multi_tuple_response(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent)
        berry_rows = df[df["_food_name"] == "berry"]
        assert len(berry_rows) == 2

    def test_source_and_reference(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent)
        row = df[df["_food_name"] == "apple"].iloc[0]
        assert row["source"] == "lit2kg"
        assert row["source_type"] == "pubmed"
        assert "111" in row["reference"]

    def test_missing_column_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.tsv"
        path.write_text("col_a\tcol_b\n1\t2\n")
        with pytest.raises(ValueError, match="missing columns"):
            load_ie_raw(path, tmp_path)

    def test_empty_response_skipped(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.tsv"
        lines = [
            "pmcid\tsection\tmatched_query\tsentence\tprob\tresponse",
            "111\tINTRO\tapple\tSentence.\t0.99\t",
        ]
        path.write_text("\n".join(lines))
        df = load_ie_raw(path, tmp_path)
        assert df.empty
