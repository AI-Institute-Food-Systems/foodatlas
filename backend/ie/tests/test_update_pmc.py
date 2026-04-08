"""Tests for step 0: 0_update_PMC_BioC."""

from __future__ import annotations

import importlib
import json

step0 = importlib.import_module("src.lit2kg.0_update_PMC_BioC")

archive_start_id = step0.archive_start_id
archive_end_id = step0.archive_end_id
read_meta = step0.read_meta
write_meta = step0.write_meta


def test_archive_start_id_old_format():
    assert archive_start_id("PMC0305000_json_unicode.tar.gz") == 305000


def test_archive_start_id_new_format():
    assert archive_start_id("PMC115XXXXX_json_unicode.tar.gz") == 11500000


def test_archive_start_id_unknown():
    assert archive_start_id("unknown_file.tar.gz") == 0


def test_archive_end_id_new_format():
    result = archive_end_id("PMC115XXXXX_json_unicode.tar.gz")
    assert result == 11599999


def test_archive_end_id_old_format():
    result = archive_end_id("PMC0305000_json_unicode.tar.gz")
    assert result is None


def test_archive_end_id_unknown():
    result = archive_end_id("unknown.tar.gz")
    assert result is None


def test_read_meta_nonexistent(tmp_path):
    result = read_meta(tmp_path / "nonexistent.json")
    assert result == {}


def test_read_meta_valid(tmp_path):
    meta_file = tmp_path / "meta.json"
    meta_file.write_text(json.dumps({"max_pmc_id": 12345}))
    result = read_meta(meta_file)
    assert result["max_pmc_id"] == 12345


def test_read_meta_corrupt(tmp_path):
    meta_file = tmp_path / "meta.json"
    meta_file.write_text("not json{")
    result = read_meta(meta_file)
    assert result == {}


def test_write_meta(tmp_path):
    meta_file = tmp_path / "meta.json"
    write_meta(meta_file, {"max_pmc_id": 999})
    data = json.loads(meta_file.read_text())
    assert data["max_pmc_id"] == 999


def test_archive_start_id_few_x():
    assert archive_start_id("PMC12XX_json_unicode.tar.gz") == 1200


def test_archive_end_id_few_x():
    assert archive_end_id("PMC12XX_json_unicode.tar.gz") == 1299
