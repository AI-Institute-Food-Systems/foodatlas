"""Tests for ie_loader — raw TSV/JSON parsing and name standardization."""

import json
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
        df = load_ie_raw(ie_tsv, ie_tsv.parent, method="gpt-4")
        assert "beta-carotene" in df["_chemical_name"].values

    def test_multi_tuple_response(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent, method="gpt-4")
        berry_rows = df[df["_food_name"] == "berry"]
        assert len(berry_rows) == 2

    def test_source_includes_method(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent, method="gpt-4")
        row = df[df["_food_name"] == "apple"].iloc[0]
        assert row["source"] == "lit2kg:gpt-4"
        assert row["source_type"] == "pubmed"
        assert "111" in row["reference"]

    def test_source_different_method(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent, method="gpt-3.5-finetuned")
        assert (df["source"] == "lit2kg:gpt-3.5-finetuned").all()

    def test_missing_column_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.tsv"
        path.write_text("col_a\tcol_b\n1\t2\n")
        with pytest.raises(ValueError, match="missing columns"):
            load_ie_raw(path, tmp_path, method="gpt-4")

    def test_quantity_parsed_into_raw_fields(self, tmp_path: Path) -> None:
        path = tmp_path / "conc.tsv"
        rows = [
            {
                "pmcid": "444",
                "section": "RESULTS",
                "matched_query": "apple",
                "sentence": "Apples contain 1.5mg/g vitamin C.",
                "prob": 0.95,
                "response": "(apple, peel, vitamin c, 1.5mg/g)",
            },
        ]
        pd.DataFrame(rows).to_csv(path, sep="\t", index=False)
        df = load_ie_raw(path, tmp_path, method="gpt-4")
        row = df.iloc[0]
        assert row["conc_value_raw"] == "1.5"
        assert row["conc_unit_raw"] == "mg/g"

    def test_unparseable_quantity_logged(self, tmp_path: Path) -> None:
        path = tmp_path / "bad_conc.tsv"
        rows = [
            {
                "pmcid": "555",
                "section": "RESULTS",
                "matched_query": "tea",
                "sentence": "Tea has traces of lead.",
                "prob": 0.90,
                "response": "(tea, leaf, lead, trace)",
            },
        ]
        pd.DataFrame(rows).to_csv(path, sep="\t", index=False)
        df = load_ie_raw(path, tmp_path, method="gpt-4")
        # Row is still created, with empty raw fields.
        assert len(df) == 1
        assert df.iloc[0]["conc_value_raw"] == ""
        assert df.iloc[0]["conc_unit_raw"] == ""
        # Error was written to diagnostics.
        errors_path = tmp_path / "diagnostics" / "ie_parse_errors.tsv"
        assert errors_path.exists()
        errors = pd.read_csv(errors_path, sep="\t")
        assert (errors["reason"] == "bad_conc").any()

    def test_empty_quantity_no_error(self, ie_tsv: Path) -> None:
        df = load_ie_raw(ie_tsv, ie_tsv.parent, method="gpt-4")
        # All fixtures have empty quantities — should not produce bad_conc errors.
        assert (df["conc_value_raw"] == "").all()
        assert (df["conc_unit_raw"] == "").all()

    def test_empty_response_skipped(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.tsv"
        lines = [
            "pmcid\tsection\tmatched_query\tsentence\tprob\tresponse",
            "111\tINTRO\tapple\tSentence.\t0.99\t",
        ]
        path.write_text("\n".join(lines))
        df = load_ie_raw(path, tmp_path, method="gpt-4")
        assert df.empty

    def test_load_json_format(self, tmp_path: Path) -> None:
        path = tmp_path / "ie.json"
        data = {
            "0": {
                "pmcid": 111,
                "section": "INTRO",
                "matched_query": "apple",
                "text": "Apples have vitamin C.",
                "prob": 0.99,
                "response": "(apple, peel, vitamin c, )",
                "triplets": [["apple", "peel", "vitamin c", ""]],
            },
        }
        path.write_text(json.dumps(data))
        df = load_ie_raw(path, tmp_path, method="gpt-4")
        assert len(df) == 1
        assert df.iloc[0]["source"] == "lit2kg:gpt-4"
        assert df.iloc[0]["_food_name"] == "apple"

    def test_load_json_null_prob(self, tmp_path: Path) -> None:
        path = tmp_path / "ie.json"
        data = {
            "0": {
                "pmcid": 111,
                "text": "Apples have vitamin C.",
                "prob": None,
                "response": "(apple, , vitamin c, )",
                "triplets": [["apple", "", "vitamin c", ""]],
            },
        }
        path.write_text(json.dumps(data))
        df = load_ie_raw(path, tmp_path, method="gpt-4")
        assert len(df) == 1
        assert df.iloc[0]["filter_score"] == 0.0
