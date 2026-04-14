"""Format KG diff results as a text report."""

from __future__ import annotations

from io import StringIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .compare import (
        EntityDetailChanges,
        EntitySummary,
        KGDiffResult,
        SourceCoverage,
        TripletSummary,
    )

_REL_LABELS: dict[str, str] = {
    "r1": "CONTAINS",
    "r2": "IS_A",
    "r3": "POS_CORRELATES",
    "r4": "NEG_CORRELATES",
}

_MAX_SAMPLE = 20
_W = 64


def format_report(result: KGDiffResult) -> str:
    """Render full diff report as plaintext."""
    buf = StringIO()
    _header(buf, "KG DIFF: v3.3 (old) vs current (new)")
    _entity_summary(buf, result.entity_summary)
    _triplet_summary(buf, result.triplet_summary)
    _entity_details(buf, result.entity_details)
    _source_coverage(buf, result.source_coverage)
    return buf.getvalue()


def _header(buf: StringIO, title: str) -> None:
    buf.write("=" * _W + "\n")
    buf.write(f"  {title}\n")
    buf.write("=" * _W + "\n\n")


def _section(buf: StringIO, title: str) -> None:
    buf.write(f"-- {title} " + "-" * max(0, _W - len(title) - 4) + "\n")


def _entity_summary(buf: StringIO, s: EntitySummary) -> None:
    _section(buf, "Entity Summary")
    all_types = sorted(set(s.old_counts) | set(s.new_counts))

    buf.write(f"  {'Type':<12} {'Old':>10} {'New':>10} {'Delta':>10}\n")
    old_total, new_total = 0, 0
    for t in all_types:
        o, n = s.old_counts.get(t, 0), s.new_counts.get(t, 0)
        old_total += o
        new_total += n
        buf.write(f"  {t:<12} {o:>10,} {n:>10,} {n - o:>+10,}\n")
    delta = new_total - old_total
    buf.write(f"  {'TOTAL':<12} {old_total:>10,} {new_total:>10,} {delta:>+10,}\n")
    buf.write("\n")

    buf.write(f"  New entity IDs:     {len(s.new_ids):,}\n")
    buf.write(f"  Retired entity IDs: {len(s.retired_ids):,}\n")
    buf.write(f"  Stable entity IDs:  {s.stable_count:,}\n\n")


def _triplet_summary(buf: StringIO, s: TripletSummary) -> None:
    _section(buf, "Triplet Summary")
    all_rels = sorted(set(s.old_counts) | set(s.new_counts))

    buf.write(f"  {'Rel':<6} {'Label':<16} {'Old':>10} {'New':>10} {'Delta':>10}\n")
    old_total, new_total = 0, 0
    for r in all_rels:
        o, n = s.old_counts.get(r, 0), s.new_counts.get(r, 0)
        old_total += o
        new_total += n
        label = _REL_LABELS.get(r, r)
        buf.write(f"  {r:<6} {label:<16} {o:>10,} {n:>10,} {n - o:>+10,}\n")
    delta = new_total - old_total
    buf.write(
        f"  {'':6} {'TOTAL':<16} {old_total:>10,} {new_total:>10,} {delta:>+10,}\n"
    )
    buf.write("\n")

    buf.write(f"  New triplet keys:     {s.new_count:,}\n")
    buf.write(f"  Removed triplet keys: {s.removed_count:,}\n")
    buf.write(f"  Stable triplet keys:  {s.stable_count:,}\n\n")


def _entity_details(buf: StringIO, d: EntityDetailChanges) -> None:
    _section(buf, "Entity Detail (matched IDs)")
    buf.write(f"  Name changes: {len(d.name_changes):,} entities\n")
    buf.write(f"  Type changes: {len(d.type_changes):,} entities\n")

    if d.name_changes:
        buf.write(f"\n  Sample name changes (first {_MAX_SAMPLE}):\n")
        for eid, old, new in d.name_changes[:_MAX_SAMPLE]:
            buf.write(f"    {eid}: {old!r} -> {new!r}\n")
    if d.type_changes:
        buf.write(f"\n  Sample type changes (first {_MAX_SAMPLE}):\n")
        for eid, old, new in d.type_changes[:_MAX_SAMPLE]:
            buf.write(f"    {eid}: {old} -> {new}\n")
    buf.write("\n")


def _source_coverage(buf: StringIO, c: SourceCoverage) -> None:
    _section(buf, "Source Coverage")
    buf.write("  OLD metadata_contains by source:\n")
    for src, cnt in sorted(c.old_contains_by_source.items(), key=lambda x: -x[1]):
        buf.write(f"    {src:<30} {cnt:>10,}\n")

    buf.write("\n  OLD metadata_diseases by source:\n")
    for src, cnt in sorted(c.old_diseases_by_source.items(), key=lambda x: -x[1]):
        buf.write(f"    {src:<30} {cnt:>10,}\n")

    buf.write("\n  NEW attestations by source:\n")
    for src, cnt in sorted(c.new_attestations_by_source.items(), key=lambda x: -x[1]):
        buf.write(f"    {src:<30} {cnt:>10,}\n")

    buf.write("\n  NEW evidence by source_type:\n")
    for src, cnt in sorted(c.new_evidence_by_type.items(), key=lambda x: -x[1]):
        buf.write(f"    {src:<30} {cnt:>10,}\n")
    buf.write("\n")
