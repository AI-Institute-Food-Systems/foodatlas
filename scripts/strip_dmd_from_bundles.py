#!/usr/bin/env python3
"""One-off: strip DMD attestations from every published download bundle.

Iterates the prod downloads manifest (``bundles/index.json``), and for
each ``foodatlas-vX.Y.zip`` in place:

- ``attestations.parquet`` — drop rows where ``source == 'dmd'``.
- ``evidence.parquet`` — drop rows where ``source_type == 'dmd'``.
- ``triplets.parquet`` — remove dropped attestation_ids from each row's
  ``attestation_ids`` array; drop the triplet if the array empties.
- ``entities.parquet`` — parse ``external_ids`` JSON, drop the ``dmd``
  key if present; re-serialise. Entities themselves are kept per the
  plan (least destructive).
- ``CHANGELOG.md`` — drop the ``dmd:`` line from "Source coverage",
  prepend a note explaining the retroactive strip.

Repacks the zip under the same S3 key. After all bundles, updates
``bundles/index.json`` with the new file_size for each entry.

Usage:
    aws sso login --profile foodatlas-prod-admin
    AWS_PROFILE=foodatlas-prod-admin python3 scripts/strip_dmd_from_bundles.py

Add ``--yes`` to skip the per-bundle confirmation prompt. Requires
``boto3``, ``pandas``, ``pyarrow`` — install into a scratch venv:

    uvx --with boto3 --with pandas --with pyarrow \\
        python scripts/strip_dmd_from_bundles.py
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import boto3  # type: ignore[import-untyped]
import pandas as pd

BUCKET = "foodatlasdownloadsstack-downloadsbucketb54b8c20-hfoegjgwag4w"
REGION = "us-west-1"
MANIFEST_KEY = "bundles/index.json"
NOTE = (
    "> **2026-07-06**: DMD (Dairy Molecule Database) attestations were "
    "retroactively removed from this bundle. Source coverage counts and "
    "triplet totals reflect the post-strip data. The `dmd_evidences` "
    "column in the DB is untouched — this only affects the public "
    "download.\n\n"
)


def _human_size(nbytes: int) -> str:
    """Return ``67.4 MB`` style string matching the existing manifest."""
    value = float(nbytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _drop_dmd_from_external_ids(raw: str | None) -> str | None:
    """Remove the ``dmd`` key from a JSON-encoded external_ids dict."""
    if not raw:
        return raw
    try:
        d: dict[str, Any] = json.loads(raw)
    except (ValueError, TypeError):
        return raw
    if "dmd" not in d:
        return raw
    d.pop("dmd", None)
    return json.dumps(d, separators=(",", ":"))


def _regenerate_changelog(text: str, *, dropped_atts: int, dropped_trips: int) -> str:
    """Prepend the strip note, drop the DMD source-coverage line.

    The bundle CHANGELOG is human-only; we don't try to keep the
    triplet-count table perfectly accurate — just prepend a note that
    counts moved. The Source coverage list has a stable per-line
    prefix (``- dmd:``) which we filter out.
    """
    lines = text.splitlines(keepends=True)
    kept = [line for line in lines if not line.lstrip().startswith("- `dmd`:")]
    body = "".join(kept)
    header = (
        f"{NOTE}"
        f"_Retro-strip removed {dropped_atts:,} DMD attestations and "
        f"{dropped_trips:,} triplets that had only DMD attestations._\n\n"
        "---\n\n"
    )
    return header + body


def _strip_bundle(extracted: Path) -> tuple[int, int]:
    """Apply all four parquet transformations + CHANGELOG rewrite.

    Returns ``(dropped_atts, dropped_trips)`` for the summary note.
    """
    # attestations.parquet — filter + capture dropped IDs.
    att_path = extracted / "attestations.parquet"
    atts = pd.read_parquet(att_path)
    dropped_att_ids = set(atts.loc[atts["source"] == "dmd", "attestation_id"].tolist())
    dropped_atts = len(dropped_att_ids)
    atts_kept = atts[atts["source"] != "dmd"].reset_index(drop=True)
    atts_kept.to_parquet(att_path, index=False)
    print(
        f"  attestations: {len(atts):,} → {len(atts_kept):,} "
        f"(dropped {dropped_atts:,})"
    )

    # evidence.parquet — same, on source_type.
    ev_path = extracted / "evidence.parquet"
    ev = pd.read_parquet(ev_path)
    ev_kept = ev[ev["source_type"] != "dmd"].reset_index(drop=True)
    ev_kept.to_parquet(ev_path, index=False)
    print(
        f"  evidence:     {len(ev):,} → {len(ev_kept):,} "
        f"(dropped {len(ev) - len(ev_kept):,})"
    )

    # triplets.parquet — filter attestation_ids arrays, drop rows left empty.
    trip_path = extracted / "triplets.parquet"
    trips = pd.read_parquet(trip_path)
    original_trips = len(trips)

    def _clean(ids: Any) -> list[str]:
        if ids is None:
            return []
        return [i for i in ids if i not in dropped_att_ids]

    trips["attestation_ids"] = trips["attestation_ids"].apply(_clean)
    trips_kept = trips[
        trips["attestation_ids"].apply(lambda v: len(v) > 0)
    ].reset_index(drop=True)
    trips_kept.to_parquet(trip_path, index=False)
    dropped_trips = original_trips - len(trips_kept)
    print(
        f"  triplets:     {original_trips:,} → {len(trips_kept):,} "
        f"(dropped {dropped_trips:,})"
    )

    # entities.parquet — null out the dmd key in external_ids JSON.
    ent_path = extracted / "entities.parquet"
    ents = pd.read_parquet(ent_path)
    before = ents["external_ids"].apply(
        lambda v: "dmd" in json.loads(v) if v and v != "null" else False
    ).sum()
    ents["external_ids"] = ents["external_ids"].apply(_drop_dmd_from_external_ids)
    ents.to_parquet(ent_path, index=False)
    print(f"  entities:     {len(ents):,} rows; scrubbed {before:,} DMD xrefs")

    # CHANGELOG.md — prepend note, drop DMD source line.
    cl_path = extracted / "CHANGELOG.md"
    if cl_path.exists():
        cl_path.write_text(
            _regenerate_changelog(
                cl_path.read_text(),
                dropped_atts=dropped_atts,
                dropped_trips=dropped_trips,
            ),
            encoding="utf-8",
        )
        print("  CHANGELOG.md: rewritten")

    return dropped_atts, dropped_trips


def _repack(zip_root_name: str, extracted: Path, out_zip: Path) -> int:
    """Zip everything under ``extracted`` back into ``out_zip``.

    ``zip_root_name`` becomes the top-level dir in the archive, matching
    the original layout (e.g. ``foodatlas-v4.4/``).
    """
    if out_zip.exists():
        out_zip.unlink()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(extracted.rglob("*")):
            if path.is_dir():
                continue
            arcname = f"{zip_root_name}/{path.relative_to(extracted).as_posix()}"
            zf.write(path, arcname)
    return out_zip.stat().st_size


def _self_check(zip_path: Path, root_name: str) -> None:
    """Post-repack sanity — re-open the zip and confirm no DMD rows survived."""
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(f"{root_name}/attestations.parquet") as f:
            atts = pd.read_parquet(f)
        with zf.open(f"{root_name}/evidence.parquet") as f:
            ev = pd.read_parquet(f)
    assert (atts["source"] == "dmd").sum() == 0, "DMD attestations survived"
    assert (ev["source_type"] == "dmd").sum() == 0, "DMD evidence rows survived"


def _process_bundle(
    s3: Any,
    entry: dict[str, Any],
    *,
    tmp: Path,
    interactive: bool,
) -> int:
    """Download, strip, repack, upload a single bundle. Returns new size."""
    version = entry["version"]
    dl_url = entry["download_link"]
    key = dl_url.split(".amazonaws.com/", 1)[1]
    zip_name = Path(key).name  # foodatlas-vX.Y.zip
    root_name = zip_name.replace(".zip", "")  # foodatlas-vX.Y

    print(f"\n=== {version}  ({key}) ===")
    if interactive:
        resp = input(f"Rewrite {key}? [y/N] ").strip().lower()
        if resp != "y":
            print("  skipped by user")
            return int(entry["file_size"].split()[0])

    workdir = tmp / version
    workdir.mkdir(parents=True, exist_ok=True)
    in_zip = workdir / zip_name
    extracted = workdir / "extracted"
    out_zip = workdir / f"{root_name}.rewritten.zip"

    # Download
    print(f"  downloading s3://{BUCKET}/{key} ...")
    s3.download_file(BUCKET, key, str(in_zip))

    # Extract
    if extracted.exists():
        shutil.rmtree(extracted)
    extracted.mkdir()
    with zipfile.ZipFile(in_zip) as zf:
        for member in zf.namelist():
            if member.startswith(f"{root_name}/"):
                target = extracted / Path(member).relative_to(root_name)
                if member.endswith("/"):
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

    # Strip
    _strip_bundle(extracted)

    # Repack + verify
    new_bytes = _repack(root_name, extracted, out_zip)
    _self_check(out_zip, root_name)
    print(f"  repacked: {_human_size(new_bytes)}")

    # Upload
    print(f"  uploading to s3://{BUCKET}/{key} ...")
    s3.upload_file(str(out_zip), BUCKET, key)

    return new_bytes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--yes", action="store_true", help="skip per-bundle confirmation"
    )
    parser.add_argument(
        "--profile", default=None, help="AWS profile override"
    )
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    s3 = session.client("s3")

    print(f"Reading manifest from s3://{BUCKET}/{MANIFEST_KEY} ...")
    manifest_obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
    manifest = json.loads(manifest_obj["Body"].read())
    entries: list[dict[str, Any]] = manifest.get("bundles") or manifest
    if not isinstance(entries, list):
        print("Unexpected manifest shape.", file=sys.stderr)
        return 2
    print(f"Found {len(entries)} bundles: {[e.get('version') for e in entries]}")

    with tempfile.TemporaryDirectory(prefix="dmd-strip-") as tmp_str:
        tmp = Path(tmp_str)
        for entry in entries:
            new_bytes = _process_bundle(s3, entry, tmp=tmp, interactive=not args.yes)
            entry["file_size"] = _human_size(new_bytes)

    print("\nUploading updated manifest ...")
    body = json.dumps(entries if isinstance(manifest, list) else manifest, indent=2)
    s3.put_object(
        Bucket=BUCKET,
        Key=MANIFEST_KEY,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
