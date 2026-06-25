"""Small util coverage: json_io and timing context manager."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from src.utils.json_io import read_json, write_json
from src.utils.timing import log_duration


class TestJsonIO:
    def test_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "out.json"
        write_json(path, {"a": 1, "b": ["x"]})
        assert path.exists()
        assert read_json(path) == {"a": 1, "b": ["x"]}

    def test_write_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "deep" / "nested" / "x.json"
        write_json(path, [1, 2, 3])
        assert read_json(path) == [1, 2, 3]

    def test_read_via_str_path(self, tmp_path: Path) -> None:
        path = tmp_path / "x.json"
        path.write_text(json.dumps({"k": "v"}))
        assert read_json(str(path)) == {"k": "v"}


class TestLogDuration:
    def test_logs_start_and_done(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        logger = logging.getLogger("test_log_duration")
        with caplog.at_level(logging.INFO, logger=logger.name):
            with log_duration("step", logger):
                pass
        msgs = [r.message for r in caplog.records]
        assert any("[START] step" in m for m in msgs)
        assert any("[DONE]" in m and "step" in m for m in msgs)

    def test_uses_module_logger_when_none(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # log=None branch
        with caplog.at_level(logging.INFO, logger="src.utils.timing"):
            with log_duration("auto"):
                pass
        assert any("auto" in r.message for r in caplog.records)
