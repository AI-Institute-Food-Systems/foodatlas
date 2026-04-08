"""Incrementally update a local BioC-PMC directory from the NCBI FTP server.

Logic:
  1. Find the max PMC numeric ID already present locally.
  2. For each *_json_unicode.tar.gz on the NCBI FTP server, parse the starting
     PMC ID encoded in its filename.
  3. Download and extract only archives whose starting ID is greater than the
     local max (i.e., archives that contain only new articles).
"""

from __future__ import annotations

import json
import logging
import re
import tarfile
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/wilbur/BioC-PMC/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def fetch_ftp_filenames() -> list[str]:
    """Return sorted list of *_json_unicode.tar.gz names from the FTP index."""
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    return sorted(set(re.findall(r'href="([^"]*_json_unicode\.tar\.gz)"', resp.text)))


def archive_start_id(filename: str) -> int:
    """Parse the leading PMC number from a BioC-PMC archive filename.

    Handles two naming conventions:
      Old: 'PMC0305000_json_unicode.tar.gz'  -> 305000
      New: 'PMC115XXXXX_json_unicode.tar.gz' -> 11500000
    """
    m = re.match(r"PMC(\d+)_json_unicode", filename)
    if m:
        return int(m.group(1))
    m = re.match(r"PMC(\d+)(X+)_json_unicode", filename)
    if m:
        prefix: int = int(m.group(1))
        num_x: int = len(str(m.group(2)))
        scale: int = 10**num_x
        return prefix * scale
    return 0


def archive_end_id(filename: str) -> int | None:
    """Return the last PMC ID that could appear in this archive.

    For the new XXXXX format the archive spans
    start_id to start_id + 10^(#X's) - 1.
    For old-style all-digit names we cannot determine an end without
    the next archive's start, so we return None.
    """
    m = re.match(r"PMC(\d+)(X+)_json_unicode", filename)
    if m:
        prefix: int = int(m.group(1))
        num_x: int = len(str(m.group(2)))
        scale: int = 10**num_x
        return prefix * scale + scale - 1
    return None


def read_meta(meta_file: Path) -> dict[str, Any]:
    """Read cached metadata, returning {} if absent or corrupt."""
    if meta_file.exists():
        try:
            result: dict[str, Any] = json.loads(meta_file.read_text())
            return result
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def write_meta(meta_file: Path, data: dict[str, Any]) -> None:
    """Write metadata to the cache file."""
    meta_file.write_text(json.dumps(data, indent=2))


def local_max_pmc_id(local_dir: Path) -> int:
    """Return the maximum PMC numeric ID using cache when available."""
    meta_file = local_dir / "meta.json"
    meta = read_meta(meta_file)
    if "max_pmc_id" in meta:
        log.info("Using cached max PMC ID from meta.json")
        return int(meta["max_pmc_id"])

    log.info("No cache found - scanning local_dir for max PMC ID...")
    max_id = 0
    for f in local_dir.iterdir():
        if not f.name.startswith("PMC"):
            continue
        try:
            num = int(f.stem[3:])
            max_id = max(max_id, num)
        except ValueError:
            pass
    write_meta(meta_file, {**meta, "max_pmc_id": max_id})
    return max_id


def download(url: str, dest: Path) -> None:
    """Download a file from url to dest with streaming."""
    log.info("  Downloading %s ...", Path(url).name)
    with requests.get(url, stream=True, timeout=3600) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def extract_all(archive: Path, local_dir: Path) -> tuple[int, int]:
    """Extract all article files from archive into *local_dir*.

    Returns (count, max_id) where max_id is the highest PMC numeric ID.
    """
    count = 0
    max_id = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            stem = Path(member.name).stem
            if not member.isfile() or not stem.startswith("PMC"):
                continue
            member.name = Path(member.name).name
            tar.extract(member, local_dir)
            count += 1
            try:
                num = int(stem[3:])
                max_id = max(max_id, num)
            except ValueError:
                pass
    log.info("  Extracted %d articles", count)
    return count, max_id


def main(bioc_pmc_dir: str | Path, dl_dir: str | Path | None = None) -> None:
    """Run the incremental BioC-PMC update.

    Args:
        bioc_pmc_dir: Local directory for the BioC-PMC corpus.
        dl_dir: Temporary download directory.  Defaults to
            ``{bioc_pmc_dir}_download``.
    """
    local_dir = Path(bioc_pmc_dir)
    download_dir = Path(dl_dir) if dl_dir else Path(f"{local_dir}_download")
    meta_file = local_dir / "meta.json"

    local_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    log.info("Fetching FTP index from %s", BASE_URL)
    ftp_files = fetch_ftp_filenames()
    if not ftp_files:
        log.error("No *_json_unicode.tar.gz files found. Exiting.")
        return
    log.info("Found %d archive(s) on FTP.", len(ftp_files))

    local_max = local_max_pmc_id(local_dir)
    log.info("Local max PMC ID: PMC%d", local_max)

    total_new = 0
    for filename in ftp_files:
        start_id = archive_start_id(filename)
        end_id = archive_end_id(filename)

        if end_id is not None:
            if end_id <= local_max:
                log.info(
                    "Skipping %s (end ID %d <= local max %d)",
                    filename,
                    end_id,
                    local_max,
                )
                continue
        elif start_id <= local_max:
            log.info(
                "Skipping %s (start ID %d <= local max %d)",
                filename,
                start_id,
                local_max,
            )
            continue

        end_str = str(end_id) if end_id else "?"
        log.info(
            "Downloading %s (IDs %d-%s, local max %d) ...",
            filename,
            start_id,
            end_str,
            local_max,
        )
        archive = download_dir / filename
        try:
            download(BASE_URL + filename, archive)
            n, max_id = extract_all(archive, local_dir)
            total_new += n
            if max_id > local_max:
                local_max = max_id
                write_meta(meta_file, {"max_pmc_id": local_max})
        finally:
            if archive.exists():
                archive.unlink()

    log.info("Done. %d new article(s) added.", total_new)
