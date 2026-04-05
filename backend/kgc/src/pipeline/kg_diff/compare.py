"""Compare old v3.3 KG with new KG and produce diff statistics."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .load_old import OldKG

logger = logging.getLogger(__name__)


@dataclass
class EntitySummary:
    """Entity count comparison by type."""

    old_counts: dict[str, int] = field(default_factory=dict)
    new_counts: dict[str, int] = field(default_factory=dict)
    new_ids: list[str] = field(default_factory=list)
    retired_ids: list[str] = field(default_factory=list)
    stable_count: int = 0


@dataclass
class TripletSummary:
    """Triplet count comparison by relationship type."""

    old_counts: dict[str, int] = field(default_factory=dict)
    new_counts: dict[str, int] = field(default_factory=dict)
    new_count: int = 0
    removed_count: int = 0
    stable_count: int = 0


@dataclass
class EntityDetailChanges:
    """Detail-level changes for matched entities."""

    name_changes: list[tuple[str, str, str]] = field(default_factory=list)
    type_changes: list[tuple[str, str, str]] = field(default_factory=list)


@dataclass
class SourceCoverage:
    """Source-level attestation/metadata counts."""

    old_contains_by_source: dict[str, int] = field(default_factory=dict)
    old_diseases_by_source: dict[str, int] = field(default_factory=dict)
    new_attestations_by_source: dict[str, int] = field(default_factory=dict)
    new_evidence_by_type: dict[str, int] = field(default_factory=dict)


@dataclass
class KGDiffResult:
    """Full diff result."""

    entity_summary: EntitySummary
    triplet_summary: TripletSummary
    entity_details: EntityDetailChanges
    source_coverage: SourceCoverage


def _make_triplet_keys(df: pd.DataFrame) -> pd.Series:
    return df["head_id"] + "_" + df["relationship_id"] + "_" + df["tail_id"]


def compare_entities(
    old_ents: pd.DataFrame,
    new_ents: pd.DataFrame,
) -> EntitySummary:
    """Compare entity counts and ID overlap."""
    old_counts = old_ents["entity_type"].value_counts().to_dict()
    new_counts = new_ents["entity_type"].value_counts().to_dict()

    old_ids = set(old_ents.index)
    new_ids = set(new_ents.index)

    return EntitySummary(
        old_counts=old_counts,
        new_counts=new_counts,
        new_ids=sorted(new_ids - old_ids),
        retired_ids=sorted(old_ids - new_ids),
        stable_count=len(old_ids & new_ids),
    )


def compare_triplets(
    old_trips: pd.DataFrame,
    new_trips: pd.DataFrame,
) -> TripletSummary:
    """Compare triplet counts by relationship and composite key overlap."""
    old_counts = old_trips["relationship_id"].value_counts().to_dict()
    new_counts = new_trips["relationship_id"].value_counts().to_dict()

    old_keys = set(_make_triplet_keys(old_trips))
    new_keys = set(_make_triplet_keys(new_trips))

    return TripletSummary(
        old_counts=old_counts,
        new_counts=new_counts,
        new_count=len(new_keys - old_keys),
        removed_count=len(old_keys - new_keys),
        stable_count=len(old_keys & new_keys),
    )


def compare_entity_details(
    old_ents: pd.DataFrame,
    new_ents: pd.DataFrame,
) -> EntityDetailChanges:
    """For matched entities, check name and type changes."""
    stable_ids = old_ents.index.intersection(new_ents.index)
    old_sub = old_ents.loc[stable_ids]
    new_sub = new_ents.loc[stable_ids]

    name_mask = old_sub["common_name"] != new_sub["common_name"]
    name_changes = [
        (eid, str(old_sub.at[eid, "common_name"]), str(new_sub.at[eid, "common_name"]))
        for eid in old_sub.index[name_mask]
    ]

    type_mask = old_sub["entity_type"] != new_sub["entity_type"]
    type_changes = [
        (eid, str(old_sub.at[eid, "entity_type"]), str(new_sub.at[eid, "entity_type"]))
        for eid in old_sub.index[type_mask]
    ]

    return EntityDetailChanges(name_changes=name_changes, type_changes=type_changes)


def compare_sources(
    old_contains_sources: pd.Series,
    old_diseases_sources: pd.Series,
    new_att: pd.DataFrame,
    new_ev: pd.DataFrame,
) -> SourceCoverage:
    """Compare attestation/metadata counts by source."""
    new_att_by_src = new_att["source"].value_counts().to_dict()
    new_ev_by_type = new_ev["source_type"].value_counts().to_dict()

    return SourceCoverage(
        old_contains_by_source=old_contains_sources.to_dict(),
        old_diseases_by_source=old_diseases_sources.to_dict(),
        new_attestations_by_source=new_att_by_src,
        new_evidence_by_type=new_ev_by_type,
    )


def _load_new_entities(kg_dir: Path) -> pd.DataFrame:
    df = pd.read_parquet(kg_dir / "entities.parquet")
    if "foodatlas_id" in df.columns:
        df = df.set_index("foodatlas_id")
    for col in ("synonyms", "external_ids"):
        if col in df.columns:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, str):
                df[col] = df[col].apply(json.loads)
    return df


def run_diff(old_kg: OldKG, kg_dir: str) -> KGDiffResult:
    """Load new KG from parquet and compare with old KG."""
    kg_path = Path(kg_dir)
    new_ents = _load_new_entities(kg_path)
    new_trips = pd.read_parquet(kg_path / "triplets.parquet")
    new_att = pd.read_parquet(kg_path / "attestations.parquet", columns=["source"])
    new_ev = pd.read_parquet(kg_path / "evidence.parquet", columns=["source_type"])

    ent_summary = compare_entities(old_kg.entities, new_ents)
    trip_summary = compare_triplets(old_kg.triplets, new_trips)
    ent_details = compare_entity_details(old_kg.entities, new_ents)
    src_coverage = compare_sources(
        old_kg.metadata_contains_sources,
        old_kg.metadata_diseases_sources,
        new_att,
        new_ev,
    )
    return KGDiffResult(ent_summary, trip_summary, ent_details, src_coverage)
