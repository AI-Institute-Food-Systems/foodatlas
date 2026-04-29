"""Tests for trust-signal loader behaviour and TrustBase isolation."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.loader import _UPSERT_BATCH_SIZE, _load_trust_signals, load_trust_only
from src.models import Base, BaseTrustSignal, TrustBase


class TestTrustBaseIsolation:
    """`base_trust_signals` must live on TrustBase, not Base."""

    def test_trust_table_not_in_base_metadata(self):
        # If it were in Base.metadata, drop_all() in _recreate_schema would
        # wipe accumulated trust signals on every db load.
        assert "base_trust_signals" not in Base.metadata.tables

    def test_trust_table_in_trust_metadata(self):
        assert "base_trust_signals" in TrustBase.metadata.tables

    def test_no_fk_to_base_attestations(self):
        # No FK because base_attestations is dropped/recreated each db load,
        # but trust signals must outlive that cycle. attestation_id is
        # content-addressed and stable across rebuilds.
        fks = list(BaseTrustSignal.__table__.foreign_keys)
        assert fks == []


class TestLoadTrustSignals:
    """Skips cleanly when parquet is missing; upserts when rows are present."""

    @patch("src.etl.loader.parquet_reader")
    def test_skips_when_parquet_missing(self, mock_reader: MagicMock):
        mock_reader.read_trust_signals.return_value = None
        conn = MagicMock()
        _load_trust_signals(conn, Path("/nonexistent"))
        # create_all is called on TrustBase (idempotent), but no execute() for
        # the upsert because there's nothing to load.
        conn.execute.assert_not_called()

    @patch("src.etl.loader.parquet_reader")
    def test_skips_when_dataframe_empty(self, mock_reader: MagicMock):
        mock_reader.read_trust_signals.return_value = pd.DataFrame()
        conn = MagicMock()
        _load_trust_signals(conn, Path("/nonexistent"))
        conn.execute.assert_not_called()

    @patch("src.etl.loader.parquet_reader")
    def test_chunks_large_inputs_under_postgres_param_cap(self, mock_reader: MagicMock):
        # Postgres caps statements at 65535 bind params; the loader must
        # chunk rather than send one giant INSERT. 12_000 rows by 10 cols
        # would be 120k params in one shot - chunking should split it.
        n_rows = 2 * _UPSERT_BATCH_SIZE + 100  # forces 3 chunks
        rows = [
            {
                "signal_id": f"{i:064x}",
                "attestation_id": f"at{i:028x}"[:30],
                "signal_kind": "llm_plausibility",
                "version": "v1",
                "config_hash": "h" * 64,
                "model": "gemini:gemini-2.5-flash-lite",
                "score": 0.5,
                "reason": "",
                "error_text": "",
                "created_at": datetime.now(UTC),
            }
            for i in range(n_rows)
        ]
        mock_reader.read_trust_signals.return_value = pd.DataFrame(rows)
        conn = MagicMock()
        _load_trust_signals(conn, Path("/nonexistent"))
        assert conn.execute.call_count == 3  # 5000 + 5000 + 100
        conn.commit.assert_called_once()  # one commit at the end, not per chunk

    @patch("src.etl.loader.parquet_reader")
    def test_upserts_when_rows_present(self, mock_reader: MagicMock):
        df = pd.DataFrame(
            [
                {
                    "signal_id": "s" * 64,
                    "attestation_id": "atabc",
                    "signal_kind": "llm_plausibility",
                    "version": "v1",
                    "config_hash": "h" * 64,
                    "model": "gemini:gemini-2.5-flash-lite",
                    "score": 0.9,
                    "reason": "ok",
                    "error_text": "",
                    "created_at": datetime.now(UTC),
                }
            ]
        )
        mock_reader.read_trust_signals.return_value = df
        conn = MagicMock()
        _load_trust_signals(conn, Path("/nonexistent"))
        # execute is called with the upsert; commit happens after.
        assert conn.execute.call_count == 1
        conn.commit.assert_called_once()


class TestLoadTrustOnly:
    """`load_trust_only` is the public CLI entry point — should NOT touch the
    full-ETL helpers (no _recreate_schema, no bulk_copy, no MV refresh)."""

    @patch("src.etl.loader.refresh_materialized_views")
    @patch("src.etl.loader.bulk_copy")
    @patch("src.etl.loader._recreate_schema")
    @patch("src.etl.loader.parquet_reader")
    def test_skips_full_etl_helpers(
        self,
        mock_reader: MagicMock,
        mock_recreate: MagicMock,
        mock_bulk: MagicMock,
        mock_refresh: MagicMock,
    ):
        mock_reader.read_trust_signals.return_value = None
        conn = MagicMock()
        load_trust_only(conn, Path("/nonexistent"))
        mock_recreate.assert_not_called()
        mock_bulk.assert_not_called()
        mock_refresh.assert_not_called()
        # but it DOES read the trust parquet
        mock_reader.read_trust_signals.assert_called_once()
