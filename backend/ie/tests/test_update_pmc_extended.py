"""Extended tests for step 0: download, extract, local_max, main."""

from __future__ import annotations

import importlib
import json
import tarfile
from unittest.mock import MagicMock, patch

step0 = importlib.import_module("src.lit2kg.0_update_PMC_BioC")


def test_local_max_pmc_id_from_cache(tmp_path, monkeypatch):
    meta_file = tmp_path / "meta.json"
    meta_file.write_text(json.dumps({"max_pmc_id": 9999}))
    monkeypatch.setattr(step0, "META_FILE", meta_file)
    monkeypatch.setattr(step0, "LOCAL_DIR", tmp_path)
    assert step0.local_max_pmc_id() == 9999


def test_local_max_pmc_id_from_scan(tmp_path, monkeypatch):
    meta_file = tmp_path / "meta.json"
    monkeypatch.setattr(step0, "META_FILE", meta_file)
    monkeypatch.setattr(step0, "LOCAL_DIR", tmp_path)

    (tmp_path / "PMC100.xml").touch()
    (tmp_path / "PMC200.xml").touch()
    (tmp_path / "other.txt").touch()

    result = step0.local_max_pmc_id()
    assert result == 200
    assert meta_file.exists()


def test_download(tmp_path, monkeypatch):
    mock_resp = MagicMock()
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.iter_content.return_value = [b"data"]

    mock_requests = MagicMock()
    mock_requests.get.return_value = mock_resp
    monkeypatch.setattr(step0, "requests", mock_requests)

    dest = tmp_path / "test.tar.gz"
    step0.download("http://example.com/test.tar.gz", dest)
    assert dest.read_bytes() == b"data"


def test_extract_all(tmp_path):
    local_dir = tmp_path / "local"
    local_dir.mkdir()

    archive_path = tmp_path / "test.tar.gz"
    inner_dir = tmp_path / "inner"
    inner_dir.mkdir()
    (inner_dir / "PMC100.xml").write_text("<doc/>")
    (inner_dir / "PMC300.xml").write_text("<doc/>")
    (inner_dir / "other.txt").write_text("skip")

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(inner_dir / "PMC100.xml", arcname="subdir/PMC100.xml")
        tar.add(inner_dir / "PMC300.xml", arcname="subdir/PMC300.xml")
        tar.add(inner_dir / "other.txt", arcname="subdir/other.txt")

    with patch.object(step0, "LOCAL_DIR", local_dir):
        count, max_id = step0.extract_all(archive_path)

    assert count == 2
    assert max_id == 300
    assert (local_dir / "PMC100.xml").exists()
    assert (local_dir / "PMC300.xml").exists()


def test_fetch_ftp_filenames(monkeypatch):
    mock_resp = MagicMock()
    mock_resp.text = (
        '<a href="PMC100_json_unicode.tar.gz">file</a>'
        '<a href="PMC200_json_unicode.tar.gz">file</a>'
        '<a href="other.tar.gz">file</a>'
    )
    mock_requests = MagicMock()
    mock_requests.get.return_value = mock_resp
    monkeypatch.setattr(step0, "requests", mock_requests)

    result = step0.fetch_ftp_filenames()
    assert len(result) == 2
    assert "PMC100_json_unicode.tar.gz" in result


def test_main_no_files(tmp_path, monkeypatch):
    monkeypatch.setattr(step0, "LOCAL_DIR", tmp_path / "local")
    monkeypatch.setattr(step0, "DL_DIR", tmp_path / "dl")
    monkeypatch.setattr(step0, "fetch_ftp_filenames", lambda: [])
    step0.main()


def test_main_skips_old_archives(tmp_path, monkeypatch):
    monkeypatch.setattr(step0, "LOCAL_DIR", tmp_path / "local")
    monkeypatch.setattr(step0, "DL_DIR", tmp_path / "dl")
    monkeypatch.setattr(
        step0,
        "fetch_ftp_filenames",
        lambda: ["PMC10XXXXX_json_unicode.tar.gz"],
    )
    monkeypatch.setattr(step0, "local_max_pmc_id", lambda: 1100000)
    mock_dl = MagicMock()
    monkeypatch.setattr(step0, "download", mock_dl)

    step0.main()
    mock_dl.assert_not_called()


def test_main_downloads_new_archives(tmp_path, monkeypatch):
    local_dir = tmp_path / "local"
    dl_dir = tmp_path / "dl"
    monkeypatch.setattr(step0, "LOCAL_DIR", local_dir)
    monkeypatch.setattr(step0, "DL_DIR", dl_dir)
    monkeypatch.setattr(step0, "META_FILE", local_dir / "meta.json")
    monkeypatch.setattr(
        step0,
        "fetch_ftp_filenames",
        lambda: ["PMC10XXXXX_json_unicode.tar.gz"],
    )
    monkeypatch.setattr(step0, "local_max_pmc_id", lambda: 500000)
    mock_dl = MagicMock()
    monkeypatch.setattr(step0, "download", mock_dl)
    monkeypatch.setattr(step0, "extract_all", lambda archive: (10, 1050000))

    step0.main()
    mock_dl.assert_called_once()


def test_main_old_format_skip(tmp_path, monkeypatch):
    monkeypatch.setattr(step0, "LOCAL_DIR", tmp_path / "local")
    monkeypatch.setattr(step0, "DL_DIR", tmp_path / "dl")
    monkeypatch.setattr(
        step0,
        "fetch_ftp_filenames",
        lambda: ["PMC0100000_json_unicode.tar.gz"],
    )
    monkeypatch.setattr(step0, "local_max_pmc_id", lambda: 200000)
    mock_dl = MagicMock()
    monkeypatch.setattr(step0, "download", mock_dl)

    step0.main()
    mock_dl.assert_not_called()
