"""Print the score distribution from base_trust_signals.

Default: latest signals across all attestations. Optionally filter by
signal_kind / version / source-prefix on the attestation.

Usage::

    cd backend/db
    uv run python scripts/trust_distribution.py
    uv run python scripts/trust_distribution.py --source lit2kg:gpt-4
"""

from __future__ import annotations

import argparse

from sqlalchemy import text
from src.config import DBSettings
from src.engine import create_sync_engine

_BINS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0001]
_BIN_LABELS = [
    "  0.0",
    "≤ 0.1",
    "≤ 0.2",
    "≤ 0.3",
    "≤ 0.4",
    "≤ 0.5",
    "≤ 0.6",
    "≤ 0.7",
    "≤ 0.8",
    "≤ 0.9",
    "≤ 1.0",
]


def main(signal_kind: str, version: str | None, source_prefix: str | None) -> None:
    settings = DBSettings()
    engine = create_sync_engine(settings)

    where = ["ts.signal_kind = :kind"]
    params: dict[str, str] = {"kind": signal_kind}
    if version:
        where.append("ts.version = :ver")
        params["ver"] = version
    if source_prefix:
        where.append("a.source LIKE :src_prefix")
        params["src_prefix"] = source_prefix + "%"

    where_sql = " AND ".join(where)
    sql = text(f"""
        SELECT ts.score
        FROM base_trust_signals ts
        JOIN base_attestations a USING (attestation_id)
        WHERE {where_sql}
    """)

    with engine.connect() as conn:
        rows = list(conn.execute(sql, params))

    scores = [float(r._mapping["score"]) for r in rows]
    if not scores:
        print(f"No trust signals match (kind={signal_kind}, version={version}).")
        return

    valid = [s for s in scores if s >= 0]
    errors = len(scores) - len(valid)
    print(f"Total signals: {len(scores)}  (errors / score=-1: {errors})")
    print(f"Valid scores : {len(valid)}")
    if not valid:
        return

    valid_sorted = sorted(valid)
    n = len(valid_sorted)
    mean = sum(valid_sorted) / n
    median = (
        valid_sorted[n // 2]
        if n % 2
        else (valid_sorted[n // 2 - 1] + valid_sorted[n // 2]) / 2
    )
    print(
        f"Min / Median / Mean / Max : "
        f"{valid_sorted[0]:.3f} / {median:.3f} / "
        f"{mean:.3f} / {valid_sorted[-1]:.3f}"
    )
    print()

    print("Below threshold:")
    for t in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7):
        below = sum(1 for s in valid if s <= t)
        print(f"  score ≤ {t:.1f}  {below:>6}  {100 * below / n:5.1f}%")
    print()

    counts = [0] * (len(_BINS) - 1)
    for s in valid:
        for i in range(len(_BINS) - 1):
            if _BINS[i] <= s < _BINS[i + 1]:
                counts[i] += 1
                break

    width = max(counts) or 1
    print(f"Histogram (bin: count, {width} = full bar)")
    for label, c in zip(_BIN_LABELS[1:], counts, strict=True):
        bar = "█" * int(40 * c / width)
        pct = 100 * c / n
        print(f"  {label:>6}  {c:>6}  {pct:5.1f}%  {bar}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--signal-kind", default="llm_plausibility")
    parser.add_argument("--version", default=None)
    parser.add_argument(
        "--source",
        default=None,
        help="Filter to attestations whose `source` starts with this prefix "
        "(e.g. 'lit2kg:gpt-4').",
    )
    args = parser.parse_args()
    main(args.signal_kind, args.version, args.source)
