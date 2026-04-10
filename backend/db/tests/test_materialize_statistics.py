"""Tests for _materialize_statistics with mocked DB."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_search import _materialize_statistics

# Minimal triplets: food→chem (r1), chem IS_A chem (r2), chem→disease (r3)
_TRIPLETS = pd.DataFrame(
    {
        "head_id": ["f1", "f1", "c1", "c1", "c2", "d1"],
        "tail_id": ["c1", "c2", "c3", "d1", "d2", "d3"],
        "relationship_id": ["r1", "r1", "r2", "r3", "r3", "r2"],
    }
)
# c1 is in r1 tail (food-connected), c2 is in r1 tail, c2→d2 is scoped
# c1→c3 is r2 (IS_A), d1→d3 is r2 (IS_A)
# Scoped r3/r4: c1→d1 and c2→d2 (both heads are r1 tails)

_ENTITIES = pd.DataFrame(
    {
        "foodatlas_id": ["f1", "c1", "c2", "c3", "d1", "d2", "d3"],
        "entity_type": [
            "food",
            "chemical",
            "chemical",
            "chemical",
            "disease",
            "disease",
            "disease",
        ],
    }
)


def _mock_read_sql(query, _conn):
    """Route pd.read_sql calls to test DataFrames."""
    sql = str(query)
    if "base_triplets" in sql:
        return _TRIPLETS.copy()
    if "base_entities" in sql:
        return _ENTITIES.copy()
    return pd.DataFrame()


class TestMaterializeStatistics:
    """Test _materialize_statistics end-to-end with mocked DB."""

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_entity_counts(self, _mock_sql, mock_copy):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 0
        _materialize_statistics(conn)

        df = mock_copy.call_args[0][2]
        stats = dict(zip(df["field"], df["count"], strict=True))
        assert stats["number of foods"] == 1  # f1
        assert stats["number of chemicals"] == 2  # c1, c2
        assert stats["number of diseases"] == 2  # d1, d2

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_association_count(self, _mock_sql, mock_copy):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 0
        _materialize_statistics(conn)

        df = mock_copy.call_args[0][2]
        stats = dict(zip(df["field"], df["count"], strict=True))
        # r1=2, scoped r3/r4=2, r2 food=0, r2 chem=1 (c1→c3), r2 disease=1 (d1→d3)
        assert stats["number of associations"] == 6

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_publications_sums_pmcid_and_pmid(self, _mock_sql, mock_copy):
        conn = MagicMock()
        # First scalar call = pmcid count, second = ctd pmid count
        conn.execute.return_value.scalar.side_effect = [10, 5]
        _materialize_statistics(conn)

        df = mock_copy.call_args[0][2]
        stats = dict(zip(df["field"], df["count"], strict=True))
        assert stats["number of publications"] == 15

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_writes_to_correct_table(self, _mock_sql, mock_copy):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 0
        _materialize_statistics(conn)

        assert mock_copy.call_args[0][1] == "mv_metadata_statistics"

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_executes_two_sql_queries_for_publications(self, _mock_sql, mock_copy):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 0
        _materialize_statistics(conn)

        assert conn.execute.call_count == 2
        calls = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert any("pmcid" in c for c in calls)
        assert any("pmid" in c for c in calls)

    @patch("src.etl.materializer_search.bulk_copy")
    @patch("src.etl.materializer_search.pd.read_sql", side_effect=_mock_read_sql)
    def test_unscoped_r3r4_excluded(self, _mock_sql, mock_copy):
        """r3/r4 where head is NOT an r1 tail should not count."""
        # Add an unscoped r3/r4 triplet (c3 is not in r1 tails)
        extra = pd.DataFrame(
            {
                "head_id": ["c3"],
                "tail_id": ["d3"],
                "relationship_id": ["r3"],
            }
        )
        patched = pd.concat([_TRIPLETS, extra], ignore_index=True)

        def read_sql_with_extra(query, _conn):
            sql = str(query)
            if "base_triplets" in sql:
                return patched.copy()
            if "base_entities" in sql:
                return _ENTITIES.copy()
            return pd.DataFrame()

        with patch(
            "src.etl.materializer_search.pd.read_sql",
            side_effect=read_sql_with_extra,
        ):
            conn = MagicMock()
            conn.execute.return_value.scalar.return_value = 0
            _materialize_statistics(conn)

        df = mock_copy.call_args[0][2]
        stats = dict(zip(df["field"], df["count"], strict=True))
        # Same as before: c3→d3 via r3 is unscoped (c3 not in r1 tails)
        assert stats["number of diseases"] == 2
        assert stats["number of associations"] == 6
