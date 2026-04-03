"""Ambiguity tracking for entity resolution in triplet construction."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ...stores.schema import FILE_AMBIGUITY_REPORT
from ...utils.json_io import write_json

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd

logger = logging.getLogger(__name__)

MAX_SAMPLES = 200


@dataclass
class AmbiguityRecord:
    """One instance of ambiguous name/ID resolution."""

    name_or_id: str
    entity_type: str
    candidate_ids: list[str]
    candidate_names: list[str]
    source: str
    triplets_produced: int = 0


@dataclass
class AmbiguityReport:
    """Collection of ambiguity records from one or more pipeline stages."""

    records: list[AmbiguityRecord] = field(default_factory=list)

    @property
    def ambiguous_count(self) -> int:
        return len(self.records)

    @property
    def total_triplets_from_ambiguity(self) -> int:
        return sum(r.triplets_produced for r in self.records)


def collect_ambiguity(
    id_map: dict[Any, list[str]],
    entities: pd.DataFrame,
    entity_type: str,
    source: str,
) -> list[AmbiguityRecord]:
    """Build AmbiguityRecords for entries in *id_map* with >1 candidate."""
    records: list[AmbiguityRecord] = []
    for key, ids in id_map.items():
        if len(ids) <= 1:
            continue
        names: list[str] = []
        for eid in ids:
            if eid in entities.index:
                names.append(str(entities.at[eid, "common_name"]))
            else:
                names.append("")
        records.append(
            AmbiguityRecord(
                name_or_id=str(key),
                entity_type=entity_type,
                candidate_ids=list(ids),
                candidate_names=names,
                source=source,
            )
        )
    return records


def write_ambiguity_report(report: AmbiguityReport, output_dir: Path) -> None:
    """Write ``_ambiguity_report.json`` summarising ambiguous resolutions."""
    if not report.records:
        return

    detail = [
        {
            "name_or_id": r.name_or_id,
            "entity_type": r.entity_type,
            "candidate_ids": r.candidate_ids,
            "candidate_names": r.candidate_names,
            "source": r.source,
            "triplets_produced": r.triplets_produced,
        }
        for r in report.records[:MAX_SAMPLES]
    ]

    payload: dict[str, Any] = {
        "ambiguous_count": report.ambiguous_count,
        "total_triplets_from_ambiguity": report.total_triplets_from_ambiguity,
        "sample_count": len(detail),
        "records": detail,
    }

    out = output_dir / FILE_AMBIGUITY_REPORT
    write_json(out, payload)
    logger.info(
        "Ambiguity: %d names/IDs resolved to multiple entities, "
        "producing %d extra triplets.",
        report.ambiguous_count,
        report.total_triplets_from_ambiguity,
    )
