"""Tests for ``main.py report --fail-if-unchanged`` (the weekly no-release signal)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pandas as pd
from click.testing import CliRunner
from main import UNCHANGED_EXIT_CODE, cli

if TYPE_CHECKING:
    from pathlib import Path

Triplet = tuple[str, str, str]
_OLD: list[Triplet] = [("e1", "r1", "e2")]


def _write_kg(kg_dir: Path, triplets: list[Triplet]) -> None:
    kg_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "foodatlas_id": ["e1", "e2"],
            "entity_type": ["food", "chemical"],
            "common_name": ["apple", "water"],
        }
    ).to_parquet(kg_dir / "entities.parquet", index=False)
    columns = ["head_id", "relationship_id", "tail_id"]
    pd.DataFrame(triplets, columns=columns).to_parquet(
        kg_dir / "triplets.parquet", index=False
    )
    pd.DataFrame({"source": ["fdc"] * len(triplets)}).to_parquet(
        kg_dir / "attestations.parquet", index=False
    )
    pd.DataFrame({"source_type": ["fdc"] * len(triplets)}).to_parquet(
        kg_dir / "evidence.parquet", index=False
    )


def _run_report(tmp_path: Path, new: list[Triplet]) -> int:
    _write_kg(tmp_path / "data" / "PreviousFAKG" / "20260101T000000Z", _OLD)
    _write_kg(tmp_path / "kg", new)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"data_dir": str(tmp_path / "data"), "kg_dir": str(tmp_path / "kg")})
    )
    result = CliRunner().invoke(
        cli, ["--config", str(config), "report", "--fail-if-unchanged"]
    )
    return result.exit_code


def test_unchanged_graph_exits_with_unchanged_code(tmp_path: Path) -> None:
    assert _run_report(tmp_path, _OLD) == UNCHANGED_EXIT_CODE
    changelog = (tmp_path / "kg" / "CHANGELOG.md").read_text()
    assert "No changes since the previous release" in changelog


def test_changed_graph_exits_cleanly(tmp_path: Path) -> None:
    assert _run_report(tmp_path, [*_OLD, ("e2", "r2", "e1")]) == 0
    changelog = (tmp_path / "kg" / "CHANGELOG.md").read_text()
    assert "No changes since the previous release" not in changelog
