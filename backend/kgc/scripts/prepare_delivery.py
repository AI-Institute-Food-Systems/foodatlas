"""Prepare a bioactivity + PTFI delivery for ingest.

Rewrites the delivery into the source files ``PTFIAdapter`` and
``BioactivityAdapter`` read — ``data/PTFI/`` and ``data/Bioactivity/`` — keyed
on external identifiers only (see ``src/pipeline/ingest/prepare``). Entity ids
the delivery cites are de-referenced against a KG snapshot, by default the
newest under ``data/PreviousFAKG/``; pass ``--kg-snapshot`` when the delivery
was built on an older one. Nothing is written if any id cannot be keyed.

Run once per delivery. The outputs are source data for the next build and for
``sync-data-to-s3.sh``.

Usage:
    cd backend/kgc
    uv run python scripts/prepare_delivery.py --delivery ../../../bioactivity_ptfi_data
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.pipeline.ingest.prepare import prepare_delivery
from src.pipeline.ingest.prepare.snapshot import latest_snapshot


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_arguments()
    snapshot = args.kg_snapshot or latest_snapshot(args.data_dir / "PreviousFAKG")
    prepare_delivery(args.delivery, snapshot, args.data_dir)
    return 0


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--delivery",
        type=Path,
        required=True,
        help="delivery dir: the eight bioactivity CSVs plus ptfi/*_ptfi_delta.csv",
    )
    parser.add_argument(
        "--kg-snapshot",
        type=Path,
        default=None,
        help="KG snapshot dir holding entities.parquet (default: newest PreviousFAKG)",
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    return parser.parse_args()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (FileNotFoundError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
