"""Ambiguity tracking derived from extraction candidate fields."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ...stores.schema import FILE_AMBIGUITY_REPORT
from ...utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path

    from ...stores.extraction_store import ExtractionStore

logger = logging.getLogger(__name__)

MAX_SAMPLES = 200


@dataclass
class AmbiguityRecord:
    """One extraction whose head or tail resolved to multiple entities."""

    extraction_id: str
    extractor: str
    head_name_raw: str
    tail_name_raw: str
    head_candidates: list[str]
    tail_candidates: list[str]


@dataclass
class AmbiguityReport:
    """Collection of ambiguous extractions."""

    records: list[AmbiguityRecord] = field(default_factory=list)

    @property
    def ambiguous_count(self) -> int:
        return len(self.records)


def build_ambiguity_from_extractions(
    store: ExtractionStore,
) -> AmbiguityReport:
    """Scan extraction store for records with multi-valued candidates."""
    records: list[AmbiguityRecord] = []
    for eid, row in store._records.iterrows():
        head_cands = row.get("head_candidates", [])
        tail_cands = row.get("tail_candidates", [])
        if head_cands is None or (
            hasattr(head_cands, "__len__") and len(head_cands) == 0
        ):
            head_cands = []
        else:
            head_cands = list(head_cands)
        if tail_cands is None or (
            hasattr(tail_cands, "__len__") and len(tail_cands) == 0
        ):
            tail_cands = []
        else:
            tail_cands = list(tail_cands)
        if len(head_cands) > 1 or len(tail_cands) > 1:
            records.append(
                AmbiguityRecord(
                    extraction_id=str(eid),
                    extractor=str(row.get("extractor", "")),
                    head_name_raw=str(row.get("head_name_raw", "")),
                    tail_name_raw=str(row.get("tail_name_raw", "")),
                    head_candidates=head_cands,
                    tail_candidates=tail_cands,
                )
            )
    return AmbiguityReport(records=records)


def write_ambiguity_report(report: AmbiguityReport, output_dir: Path) -> None:
    """Write ``_ambiguity_report.json`` summarising ambiguous extractions."""
    if not report.records:
        return

    detail = [
        {
            "extraction_id": r.extraction_id,
            "extractor": r.extractor,
            "head_name_raw": r.head_name_raw,
            "tail_name_raw": r.tail_name_raw,
            "head_candidates": r.head_candidates,
            "tail_candidates": r.tail_candidates,
        }
        for r in report.records[:MAX_SAMPLES]
    ]

    payload: dict[str, Any] = {
        "ambiguous_count": report.ambiguous_count,
        "sample_count": len(detail),
        "records": detail,
    }

    out = output_dir / FILE_AMBIGUITY_REPORT
    write_json(out, payload)
    logger.info(
        "Ambiguity: %d extractions have multi-valued candidates.",
        report.ambiguous_count,
    )
