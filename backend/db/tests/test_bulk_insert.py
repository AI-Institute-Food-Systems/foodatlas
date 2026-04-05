"""Tests for src.etl.bulk_insert — serialization and buffer helpers."""

import math

import pandas as pd
from src.etl.bulk_insert import _df_to_copy_buffer, _serialize_value


class TestSerializeValue:
    """Unit tests for _serialize_value."""

    def test_none_returns_null_escape(self):
        assert _serialize_value(None) == r"\N"

    def test_nan_returns_null_escape(self):
        assert _serialize_value(float("nan")) == r"\N"

    def test_numpy_nan_returns_null_escape(self):
        # pd.isna handles numpy NaN too
        assert _serialize_value(math.nan) == r"\N"

    def test_list_returns_json(self):
        assert _serialize_value(["a", "b"]) == '["a", "b"]'

    def test_empty_list_returns_json(self):
        assert _serialize_value([]) == "[]"

    def test_dict_returns_json(self):
        result = _serialize_value({"key": "val"})
        assert result == '{"key": "val"}'

    def test_empty_dict_returns_json(self):
        assert _serialize_value({}) == "{}"

    def test_bool_true(self):
        assert _serialize_value(True) == "t"

    def test_bool_false(self):
        assert _serialize_value(False) == "f"

    def test_int_returns_string(self):
        assert _serialize_value(42) == "42"

    def test_float_returns_string(self):
        assert _serialize_value(3.14) == "3.14"

    def test_string_passthrough(self):
        assert _serialize_value("hello") == "hello"

    def test_empty_string(self):
        assert _serialize_value("") == ""

    def test_nested_structure(self):
        val = {"items": [1, 2], "nested": {"a": True}}
        result = _serialize_value(val)
        assert '"items"' in result
        assert '"nested"' in result


class TestDfToCopyBuffer:
    """Unit tests for _df_to_copy_buffer."""

    def test_basic_output(self):
        df = pd.DataFrame(
            {
                "name": ["Apple", "Banana"],
                "count": [10, 20],
            }
        )
        buf = _df_to_copy_buffer(df, ["name", "count"])
        content = buf.read()
        lines = content.strip().split("\n")
        assert len(lines) == 2
        assert lines[0] == "Apple\t10"
        assert lines[1] == "Banana\t20"

    def test_subset_of_columns(self):
        df = pd.DataFrame(
            {
                "a": [1],
                "b": [2],
                "c": [3],
            }
        )
        buf = _df_to_copy_buffer(df, ["a", "c"])
        content = buf.read()
        assert content.strip() == "1\t3"

    def test_none_serialized_as_null(self):
        df = pd.DataFrame(
            {
                "name": ["X"],
                "value": [None],
            }
        )
        buf = _df_to_copy_buffer(df, ["name", "value"])
        content = buf.read()
        assert r"\N" in content

    def test_list_column_serialized_as_json(self):
        df = pd.DataFrame(
            {
                "tags": [["a", "b"]],
            }
        )
        buf = _df_to_copy_buffer(df, ["tags"])
        content = buf.read()
        assert '["a", "b"]' in content

    def test_bool_column(self):
        df = pd.DataFrame(
            {
                "flag": [True, False],
            }
        )
        buf = _df_to_copy_buffer(df, ["flag"])
        content = buf.read()
        lines = content.strip().split("\n")
        assert lines[0] == "t"
        assert lines[1] == "f"

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.Series([], dtype=object)})
        buf = _df_to_copy_buffer(df, ["a"])
        content = buf.read()
        assert content == ""

    def test_buffer_position_at_start(self):
        df = pd.DataFrame({"x": [1]})
        buf = _df_to_copy_buffer(df, ["x"])
        # Buffer should be seeked to position 0
        assert buf.tell() == 0

    def test_mixed_types(self):
        df = pd.DataFrame(
            {
                "id": ["f001"],
                "score": [0.95],
                "tags": [["x"]],
                "meta": [{"k": "v"}],
                "active": [True],
            }
        )
        buf = _df_to_copy_buffer(df, ["id", "score", "tags", "meta", "active"])
        content = buf.read().strip()
        parts = content.split("\t")
        assert parts[0] == "f001"
        assert parts[1] == "0.95"
        assert parts[2] == '["x"]'
        assert parts[3] == '{"k": "v"}'
        assert parts[4] == "t"
