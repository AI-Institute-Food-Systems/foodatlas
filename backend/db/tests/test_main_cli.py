"""CLI wiring in main.py: post-load VACUUM ANALYZE and the index migration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import main
from click.testing import CliRunner
from src.models.base import Base
from src.models.views import MVChemicalBioactivity, MVFoodBioactivity


def _engine(valid: dict[str, bool] | None = None) -> tuple[MagicMock, list[str]]:
    """A sync engine whose connections record each SQL string executed.

    ``valid`` maps index name -> pg_index.indisvalid; unlisted indexes
    report valid, as after a successful CONCURRENTLY build.
    """
    executed: list[str] = []
    valid = valid or {}

    def execute(stmt: object, params: dict | None = None, *_a: object) -> MagicMock:
        executed.append(str(stmt))
        result = MagicMock()
        name = (params or {}).get("n")
        result.scalar.return_value = valid.get(name, True) if name else None
        return result

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn.execution_options.return_value = conn
    conn.__enter__.return_value = conn
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, executed


def test_refresh_ends_with_vacuum_analyze() -> None:
    engine, executed = _engine()
    with (
        patch.object(main, "create_sync_engine", return_value=engine),
        patch.object(main, "DBSettings"),
        patch.object(main, "refresh_materialized_views"),
    ):
        result = CliRunner().invoke(main.cli, ["refresh"])
    assert result.exit_code == 0, result.output
    assert executed == ["VACUUM ANALYZE"]
    engine.connect.return_value.execution_options.assert_called_with(
        isolation_level="AUTOCOMMIT"
    )


def test_load_ends_with_vacuum_analyze(tmp_path: object) -> None:
    engine, executed = _engine()
    with (
        patch.object(main, "create_sync_engine", return_value=engine),
        patch.object(main, "DBSettings"),
        patch.object(main, "load_kg") as load_kg,
    ):
        result = CliRunner().invoke(main.cli, ["load", "--parquet-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    load_kg.assert_called_once()
    assert executed == ["VACUUM ANALYZE"]


def test_migration_builds_each_index_before_dropping_the_old_one() -> None:
    engine, executed = _engine()
    with (
        patch.object(main, "create_sync_engine", return_value=engine),
        patch.object(main, "DBSettings"),
    ):
        result = CliRunner().invoke(main.cli, ["migrate-sort-key-indexes"])
    assert result.exit_code == 0, result.output
    for old, _, _ in main._SORT_KEY_INDEXES:
        create = next(i for i, s in enumerate(executed) if f" {old}_key " in s)
        drop = executed.index(f"DROP INDEX CONCURRENTLY IF EXISTS {old}")
        assert create < drop
    assert executed[-1] == "VACUUM ANALYZE"


def test_migration_matches_the_model_indexes() -> None:
    # A fresh `db load` builds the model's indexes; the migration must
    # leave an already-loaded RDS with exactly the same ones.
    model = {
        str(idx.name): [c.name for c in idx.columns]
        for model_cls in (MVChemicalBioactivity, MVFoodBioactivity)
        for idx in Base.metadata.tables[model_cls.__tablename__].indexes
    }
    for old, table, cols in main._SORT_KEY_INDEXES:
        migrated = [c.strip() for c in f"{cols}, {main._ROW_KEY[table]}".split(",")]
        assert model[f"{old}_key"] == migrated
        assert old not in model


def test_migration_rebuilds_an_invalid_leftover_index() -> None:
    old = main._SORT_KEY_INDEXES[0][0]
    engine, executed = _engine({f"{old}_key": False})
    with (
        patch.object(main, "create_sync_engine", return_value=engine),
        patch.object(main, "DBSettings"),
    ):
        result = CliRunner().invoke(main.cli, ["migrate-sort-key-indexes"])
    # The rebuild still reports invalid, so the old index must survive.
    assert result.exit_code != 0
    assert f"DROP INDEX CONCURRENTLY IF EXISTS {old}_key" in executed
    assert f"DROP INDEX CONCURRENTLY IF EXISTS {old}" not in executed
