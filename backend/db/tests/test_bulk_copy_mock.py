"""Tests for src.etl.bulk_insert — bulk_copy and truncate_tables with mocks."""

from unittest.mock import MagicMock

import pandas as pd
from src.etl.bulk_insert import bulk_copy, truncate_tables


class TestBulkCopy:
    """Test bulk_copy with a mocked psycopg connection."""

    def _make_mock_conn(self):
        """Build a nested mock matching psycopg3's cursor/copy protocol."""
        conn = MagicMock()
        raw = MagicMock()
        conn.connection = raw

        copy_ctx = MagicMock()
        copy_ctx.__enter__ = MagicMock(return_value=copy_ctx)
        copy_ctx.__exit__ = MagicMock(return_value=False)

        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.copy.return_value = copy_ctx

        raw.cursor.return_value = cursor
        return conn, cursor, copy_ctx

    def test_returns_row_count(self):
        conn, _, _ = self._make_mock_conn()
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        total = bulk_copy(conn, "test_table", df, ["a", "b"])
        assert total == 3

    def test_single_chunk(self):
        conn, cursor, _copy_ctx = self._make_mock_conn()
        df = pd.DataFrame({"col": [1, 2]})
        bulk_copy(conn, "my_table", df, ["col"], chunk_size=100)

        cursor.copy.assert_called_once()
        copy_sql = cursor.copy.call_args[0][0]
        assert "my_table" in copy_sql
        assert '"col"' in copy_sql

    def test_multiple_chunks(self):
        conn, cursor, _ = self._make_mock_conn()
        df = pd.DataFrame({"a": range(10)})
        total = bulk_copy(conn, "tbl", df, ["a"], chunk_size=3)
        assert total == 10
        # 10 rows / 3 chunk_size = 4 chunks
        assert cursor.copy.call_count == 4

    def test_empty_dataframe(self):
        conn, cursor, _ = self._make_mock_conn()
        df = pd.DataFrame({"a": pd.Series([], dtype=object)})
        total = bulk_copy(conn, "tbl", df, ["a"])
        assert total == 0
        cursor.copy.assert_not_called()

    def test_copy_writes_lines(self):
        conn, _, copy_ctx = self._make_mock_conn()
        df = pd.DataFrame({"name": ["Alice"]})
        bulk_copy(conn, "tbl", df, ["name"])
        # copy.write should have been called with the line
        copy_ctx.write.assert_called()

    def test_column_quoting_in_sql(self):
        conn, cursor, _ = self._make_mock_conn()
        df = pd.DataFrame({"my col": [1]})
        bulk_copy(conn, "tbl", df, ["my col"])
        copy_sql = cursor.copy.call_args[0][0]
        assert '"my col"' in copy_sql


class TestTruncateTables:
    """Test truncate_tables with a mocked connection."""

    def test_truncates_all_tables(self):
        conn = MagicMock()
        truncate_tables(conn, ["table_a", "table_b", "table_c"])
        assert conn.execute.call_count == 3

    def test_truncate_sql_contains_cascade(self):
        conn = MagicMock()
        truncate_tables(conn, ["my_table"])
        call_args = conn.execute.call_args[0][0]
        # text() objects compare by their text attribute
        assert "CASCADE" in str(call_args)
        assert "my_table" in str(call_args)

    def test_empty_list_no_calls(self):
        conn = MagicMock()
        truncate_tables(conn, [])
        conn.execute.assert_not_called()

    def test_truncate_order_preserved(self):
        conn = MagicMock()
        tables = ["first", "second", "third"]
        truncate_tables(conn, tables)
        calls = conn.execute.call_args_list
        for i, name in enumerate(tables):
            sql_text = calls[i][0][0]
            assert name in sql_text.text
