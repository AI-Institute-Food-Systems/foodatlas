"""Tests for bioc_fetch — covers _fetch_one's outcome classification and
fetch_missing's orchestration. Network is fully mocked via a fake Session;
no real HTTP traffic. ThreadPoolExecutor is exercised end-to-end so the
result aggregation is real."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
import requests
from src.pipeline.search.bioc_fetch import (
    FetchResult,
    _cleanup_stray_tmp,
    _emit_progress,
    _fetch_one,
    _make_session,
    fetch_missing,
)

if TYPE_CHECKING:
    from pathlib import Path


class _FakeResp:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


def _session_returning(*responses: _FakeResp) -> MagicMock:
    """Build a fake Session whose .get cycles through the given responses."""
    sess = MagicMock(spec=requests.Session)
    sess.get.side_effect = list(responses)
    return sess


@pytest.fixture
def out_dir(tmp_path: Path) -> Path:
    d = tmp_path / "bioc"
    d.mkdir()
    return d


class TestFetchOne:
    def test_cached_when_dest_exists(self, out_dir: Path) -> None:
        (out_dir / "PMC1.xml").write_text("{}")
        pmcid, status = _fetch_one("PMC1", out_dir, MagicMock(), timeout=1.0)
        assert (pmcid, status) == ("PMC1", "cached")

    def test_ok_unwraps_single_element_list(self, out_dir: Path) -> None:
        body = json.dumps([{"id": "PMC2"}]).encode()
        sess = _session_returning(_FakeResp(200, body))
        pmcid, status = _fetch_one("PMC2", out_dir, sess, timeout=1.0)
        assert (pmcid, status) == ("PMC2", "ok")
        # File written + tmp file removed by rename
        written = json.loads((out_dir / "PMC2.xml").read_text())
        assert written == {"id": "PMC2"}
        assert not (out_dir / "PMC2.xml.tmp").exists()

    def test_404_is_not_in_oa(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(404, b""))
        _, status = _fetch_one("PMC3", out_dir, sess, timeout=1.0)
        assert status == "not_in_oa"

    def test_200_with_error_body_is_not_in_oa(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(200, b"[Error] : not OA"))
        _, status = _fetch_one("PMC4", out_dir, sess, timeout=1.0)
        assert status == "not_in_oa"

    def test_non_200_other_is_http_error(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(503, b""))
        _, status = _fetch_one("PMC5", out_dir, sess, timeout=1.0)
        assert status == "error:http_503"

    def test_request_exception_wrapped(self, out_dir: Path) -> None:
        sess = MagicMock(spec=requests.Session)
        sess.get.side_effect = requests.Timeout()
        _, status = _fetch_one("PMC6", out_dir, sess, timeout=1.0)
        assert status == "error:Timeout"

    def test_invalid_json_reported(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(200, b"not json"))
        _, status = _fetch_one("PMC7", out_dir, sess, timeout=1.0)
        assert status == "error:invalid_json"

    def test_unexpected_list_length(self, out_dir: Path) -> None:
        body = json.dumps([{"a": 1}, {"b": 2}]).encode()
        sess = _session_returning(_FakeResp(200, body))
        _, status = _fetch_one("PMC8", out_dir, sess, timeout=1.0)
        assert status == "error:unexpected_list_len_2"

    def test_unexpected_non_dict_type(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(200, json.dumps("hi").encode()))
        _, status = _fetch_one("PMC9", out_dir, sess, timeout=1.0)
        assert status == "error:unexpected_type_str"

    def test_bare_dict_response(self, out_dir: Path) -> None:
        sess = _session_returning(_FakeResp(200, json.dumps({"id": 1}).encode()))
        _, status = _fetch_one("PMC10", out_dir, sess, timeout=1.0)
        assert status == "ok"


class TestHelpers:
    def test_cleanup_stray_tmp_removes_xml_tmp(self, out_dir: Path) -> None:
        (out_dir / "a.xml.tmp").write_text("")
        (out_dir / "b.xml.tmp").write_text("")
        (out_dir / "c.xml").write_text("")
        removed = _cleanup_stray_tmp(out_dir)
        assert removed == 2
        assert (out_dir / "c.xml").exists()
        assert not (out_dir / "a.xml.tmp").exists()

    def test_make_session_configures_retries(self) -> None:
        sess = _make_session(total_retries=3, backoff_factor=0.5, user_agent="ua")
        assert sess.headers["User-Agent"] == "ua"
        # Adapter mounted on https://
        adapter = sess.get_adapter("https://example.com/")
        assert adapter is not None

    def test_emit_progress_writes_expected_fields(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        result = FetchResult(fetched=10, cached=5, not_in_oa=["x"], errors=[("y", "e")])
        logger = logging.getLogger("test_emit_progress")
        with caplog.at_level(logging.INFO, logger=logger.name):
            _emit_progress(logger, result, total=20, elapsed=2.0, tag="progress")
        assert any(
            "ok=10" in rec.message and "cached=5" in rec.message
            for rec in caplog.records
        )


class TestFetchMissing:
    def test_no_pmcids_returns_empty_result(self, out_dir: Path) -> None:
        result = fetch_missing([], out_dir)
        assert result.fetched == 0
        assert result.cached == 0
        assert result.not_in_oa == []
        assert result.errors == []

    def test_cached_pmcid_counted_without_fetch(self, out_dir: Path) -> None:
        # Pre-seed a cached file so _fetch_one returns "cached" without
        # needing the network.
        (out_dir / "PMC100.xml").write_text("{}")
        result = fetch_missing(["PMC100"], out_dir, max_workers=1)
        assert result.cached == 1
        assert result.fetched == 0

    def test_log_path_creates_progress_file(
        self, out_dir: Path, tmp_path: Path
    ) -> None:
        # Pre-seed so no network is touched
        (out_dir / "PMC200.xml").write_text("{}")
        log_path = tmp_path / "logs" / "progress.log"
        fetch_missing(["PMC200"], out_dir, max_workers=1, log_path=log_path)
        assert log_path.exists()
        text = log_path.read_text()
        assert "start" in text and "done" in text
