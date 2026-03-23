#!/usr/bin/env python3
"""
Incrementally update /mnt/data/shared/BioC-PMC.

Logic:
  1. Find the max PMC numeric ID already present locally.
  2. For each *_json_unicode.tar.gz on the NCBI FTP server, parse the starting
     PMC ID encoded in its filename.
  3. Download and extract only archives whose starting ID is greater than the
     local max (i.e., archives that contain only new articles).
"""

import json
import logging
import re
import tarfile
from pathlib import Path

import requests

BASE_URL  = "https://ftp.ncbi.nlm.nih.gov/pub/wilbur/BioC-PMC/"
LOCAL_DIR = Path("/mnt/data/shared/BioC-PMC")
DL_DIR    = Path("/mnt/data/shared/BioC-PMC_download")
META_FILE = LOCAL_DIR / "meta.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def fetch_ftp_filenames():
    """Return sorted list of *_json_unicode.tar.gz names from the FTP index."""
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    return sorted(set(re.findall(r'href="([^"]*_json_unicode\.tar\.gz)"', resp.text)))


def archive_start_id(filename):
    """Parse the leading PMC number from a BioC-PMC archive filename.

    Handles two naming conventions:
      Old: 'PMC0305000_json_unicode.tar.gz'  → 305000
      New: 'PMC115XXXXX_json_unicode.tar.gz' → 11500000
           (NCBI uses literal 'X' as digit placeholders; each X represents one
            wildcard digit, so the start ID is the numeric prefix × 10^(#X's))
    """
    # Old format: all digits after PMC
    m = re.match(r'PMC(\d+)_json_unicode', filename)
    if m:
        return int(m.group(1))
    # New format: digits followed by one or more X placeholders
    m = re.match(r'PMC(\d+)(X+)_json_unicode', filename)
    if m:
        return int(m.group(1)) * (10 ** len(m.group(2)))
    return 0


def archive_end_id(filename):
    """Return the last PMC ID that could appear in this archive.

    For the new XXXXX format (e.g. PMC115XXXXX) the archive spans
    start_id … start_id + 10^(#X's) - 1.
    For old-style all-digit names we cannot determine an end without
    the next archive's start, so we return None (unknown).
    """
    m = re.match(r'PMC(\d+)(X+)_json_unicode', filename)
    if m:
        num_x = len(m.group(2))
        return int(m.group(1)) * (10 ** num_x) + (10 ** num_x) - 1
    return None


def read_meta() -> dict:
    """Read cached metadata, returning {} if absent or corrupt."""
    if META_FILE.exists():
        try:
            return json.loads(META_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def write_meta(data: dict):
    META_FILE.write_text(json.dumps(data, indent=2))


def local_max_pmc_id():
    """Return the maximum PMC numeric ID, using .meta.json cache when available.

    Falls back to a full directory scan (slow) only when the cache is absent,
    and writes the result back to the cache for future runs.
    """
    meta = read_meta()
    if "max_pmc_id" in meta:
        log.info("Using cached max PMC ID from .meta.json")
        return meta["max_pmc_id"]

    log.info("No cache found — scanning LOCAL_DIR for max PMC ID (may be slow) ...")
    max_id = 0
    for f in LOCAL_DIR.iterdir():
        if not f.name.startswith("PMC"):
            continue
        try:
            num = int(f.stem[3:])   # strip "PMC" prefix, e.g. "10000003" -> 10000003
            if num > max_id:
                max_id = num
        except ValueError:
            pass
    write_meta({**meta, "max_pmc_id": max_id})
    return max_id


# --------------------------------------------------------------------------- #
# Core logic
# --------------------------------------------------------------------------- #

def download(url, dest):
    log.info(f"  Downloading {Path(url).name} ...")
    with requests.get(url, stream=True, timeout=3600) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def extract_all(archive):
    """Extract all article files from archive into LOCAL_DIR.

    Returns (count, max_id) where max_id is the highest PMC numeric ID extracted.
    """
    count = 0
    max_id = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            stem = Path(member.name).stem
            if not member.isfile() or not stem.startswith("PMC"):
                continue
            member.name = Path(member.name).name   # flatten subdirs
            tar.extract(member, LOCAL_DIR)
            count += 1
            try:
                num = int(stem[3:])
                if num > max_id:
                    max_id = num
            except ValueError:
                pass
    log.info(f"  Extracted {count} articles")
    return count, max_id


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    DL_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Fetching FTP index from {BASE_URL}")
    ftp_files = fetch_ftp_filenames()
    if not ftp_files:
        log.error("No *_json_unicode.tar.gz files found. Exiting.")
        return
    log.info(f"Found {len(ftp_files)} archive(s) on FTP.")

    local_max = local_max_pmc_id()
    log.info(f"Local max PMC ID: PMC{local_max}")

    total_new = 0
    for filename in ftp_files:
        start_id = archive_start_id(filename)
        end_id   = archive_end_id(filename)

        # Skip only when the entire archive is guaranteed to be already present.
        # For range archives (new XXXXX format) we know the end ID precisely;
        # skip only if end_id <= local_max.  For old all-digit names end_id is
        # None, so fall back to the original start_id heuristic.
        if end_id is not None:
            if end_id <= local_max:
                log.info(f"Skipping {filename} (end ID {end_id} <= local max {local_max})")
                continue
        else:
            if start_id <= local_max:
                log.info(f"Skipping {filename} (start ID {start_id} <= local max {local_max})")
                continue

        log.info(f"Downloading {filename} (IDs {start_id}–{end_id if end_id else '?'}, local max {local_max}) ...")
        archive = DL_DIR / filename
        try:
            download(BASE_URL + filename, archive)
            n, max_id = extract_all(archive)
            total_new += n
            if max_id > local_max:
                local_max = max_id
                write_meta({"max_pmc_id": local_max})
        finally:
            if archive.exists():
                archive.unlink()

    log.info(f"Done. {total_new} new article(s) added.")


if __name__ == "__main__":
    main()
