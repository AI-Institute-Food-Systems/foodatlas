"""Tests for src.etl.materializer — orchestration with mocked DB."""

from unittest.mock import MagicMock, patch

from src.etl.materializer import MV_TABLES, refresh_all


class TestMVTablesConstant:
    """Verify MV_TABLES list."""

    def test_contains_expected_tables(self):
        assert "mv_food_entities" in MV_TABLES
        assert "mv_chemical_entities" in MV_TABLES
        assert "mv_disease_entities" in MV_TABLES
        assert "mv_food_chemical_composition" in MV_TABLES
        assert "mv_chemical_disease_correlation" in MV_TABLES
        assert "mv_disease_bioactivity" in MV_TABLES
        assert "mv_assay_target_labels" in MV_TABLES


class TestRefreshAll:
    """Test refresh_all orchestration."""

    @patch("src.etl.materializer.materialize_assay_target_labels")
    @patch("src.etl.materializer.materialize_disease_bioactivity")
    @patch("src.etl.materializer.materialize_chemical_disease_bioactivity")
    @patch("src.etl.materializer.materialize_food_chemical_efficacy")
    @patch("src.etl.materializer.materialize_bioactivity")
    @patch("src.etl.materializer.materialize_chemical_disease_correlation")
    @patch("src.etl.materializer.materialize_food_chemical_composition")
    @patch("src.etl.materializer._materialize_entity_views")
    @patch("src.etl.materializer.truncate_tables")
    def test_calls_truncate_then_materialize(
        self,
        mock_truncate,
        mock_entities,
        mock_fcc,
        mock_cdc,
        mock_bio,
        mock_efficacy,
        mock_cdb,
        mock_dbio,
        mock_labels,
    ):
        conn = MagicMock()
        refresh_all(conn)
        mock_truncate.assert_called_once_with(conn, MV_TABLES)
        mock_entities.assert_called_once_with(conn)
        mock_fcc.assert_called_once_with(conn)
        mock_cdc.assert_called_once_with(conn)
        mock_bio.assert_called_once_with(conn)
        mock_efficacy.assert_called_once_with(conn)
        mock_cdb.assert_called_once_with(conn)
        mock_dbio.assert_called_once_with(conn)
        mock_labels.assert_called_once_with(conn)
        conn.commit.assert_called_once()
