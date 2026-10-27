"""Build food-chemical (CONTAINS) triplets from Phase 1 PTFI edges.

One attestation per physical PTFI sample measurement, one triplet per
(food, chemical) pair — several samples of the same food collapse onto one
triplet with all their attestations, which the triplet store does on insert.

Values pass through unconverted, under the evidence record the adapter attached
to each edge. LC-MS relative abundance keeps ``conc_unit = relative_abundance``
and surfaces with no ``median_concentration``; reference-panel values arrive as
``mg/100g`` and join the composition median like any FDC row.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.relationship import RelationshipType
from ...ingest.ptfi_keys import CHEMICAL_KEYS
from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

SOURCE = "ptfi"


def merge_ptfi_triplets(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Create food-chemical CONTAINS triplets from PTFI measurement edges."""
    ptfi = sources.get(SOURCE)
    if ptfi is None:
        return
    edges = ptfi.get("edges", pd.DataFrame())
    contains = (
        edges[edges["edge_type"] == "contains"].copy() if not edges.empty else edges
    )
    if contains.empty:
        logger.info("No PTFI contains edges.")
        return

    resolved = _resolve_entities(kg, contains)
    if resolved.empty:
        logger.info("No PTFI data to merge after resolution.")
        return

    attestations = kg.attestations.create(_attestation_rows(kg, resolved))
    triplets = _create_triplets(kg, resolved, attestations)
    logger.info(
        "Merged %d PTFI attestations, %d triplets.", len(attestations), len(triplets)
    )


def _resolve_entities(kg: KnowledgeGraph, contains: pd.DataFrame) -> pd.DataFrame:
    """Attach the food and chemical entity ids each edge resolves to.

    The adapter's native ids are not on the entities, so map each side back
    through what resolution did store: the sample id for foods, and for
    chemicals whichever public id keys it (``pubchem:5192``, ``chebi:26607``).
    """
    food_lookup = explode_external_ids(kg.entities._entities, "ptfi")
    chemical_lookup = _chemical_lookup(kg.entities._entities)
    if food_lookup.empty or chemical_lookup.empty:
        return pd.DataFrame()

    df = contains.copy()
    df["_sample_id"] = df["raw_attrs"].map(lambda a: a.get("sample_id", ""))
    df["_chemical_key"] = df["raw_attrs"].map(lambda a: a.get("chemical_key", ""))

    df = df.merge(
        food_lookup, left_on="_sample_id", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_head_id", "candidates": "head_candidates"}
    )
    df = df.merge(
        chemical_lookup, left_on="_chemical_key", right_on="native_id", how="inner"
    ).drop(columns=["native_id"])
    df = df.rename(
        columns={"foodatlas_id": "_tail_id", "candidates": "tail_candidates"}
    )
    return _prefer_named_chemical(df, kg.entities._entities)


def _chemical_lookup(entities: pd.DataFrame) -> pd.DataFrame:
    """``<source>:<id>`` → entity, over every public id a PTFI chemical can
    be keyed by."""
    frames = []
    for key in CHEMICAL_KEYS:
        lookup = explode_external_ids(entities, key.external_ids_key)
        lookup["native_id"] = key.source + ":" + lookup["native_id"].map(_int_str)
        frames.append(lookup)
    return pd.concat(frames, ignore_index=True)


def _prefer_named_chemical(df: pd.DataFrame, entities: pd.DataFrame) -> pd.DataFrame:
    """Narrow an ambiguous chemical key to the entity PTFI actually named.

    1,125 PubChem CIDs in the KG map to more than one chemical entity — a ChEBI
    class alongside the compound (``formate`` / ``monocarboxylic acid anion``),
    or an IUPAC name alongside the common one. PTFI names the chemical it
    measured, so prefer the candidate whose ``common_name`` matches and drop the
    rest. Where no candidate matches the two entities are genuinely duplicates
    of one compound (``4-coumaric acid`` / ``trans-4-coumaric acid``), which PTFI
    cannot settle — keep every candidate so the ambiguity stays visible.
    """
    ambiguous = df["tail_candidates"].map(len) > 1
    if not ambiguous.any():
        return df

    names = entities["common_name"].astype(str).str.casefold()
    df = df.copy()
    df["_ptfi_name"] = (
        df["raw_attrs"].map(lambda a: str(a.get("chemical_name", ""))).str.casefold()
    )
    df["_name_match"] = df["_tail_id"].map(names) == df["_ptfi_name"]

    group = df.groupby(["_sample_id", "_chemical_key"])["_name_match"]
    keep = df["_name_match"] | ~group.transform("any")
    dropped = int((~keep).sum())
    if dropped:
        logger.info("PTFI: narrowed %d ambiguous chemical rows by name match.", dropped)
    return df[keep].drop(columns=["_ptfi_name", "_name_match"])


def _attestation_rows(kg: KnowledgeGraph, resolved: pd.DataFrame) -> pd.DataFrame:
    """Build the attestation frame, creating each edge's evidence record first.

    The reference is the evidence dict the adapter carried on the edge, stored
    verbatim: the evidence store content-hashes it, so the handful of distinct
    PTFI records (one per panel) collapse to one row each.
    """
    df = resolved.copy()
    df["source_type"] = SOURCE
    df["reference"] = df["raw_attrs"].map(lambda a: json.dumps(a["evidence"]))
    df["evidence_id"] = kg.evidence.create(df[["source_type", "reference"]]).index

    df["source"] = SOURCE
    # The sample id is the head's raw name: it is what PTFI actually measured,
    # and it keeps one attestation per sample after foods collapse into groups.
    df["head_name_raw"] = df["_sample_id"]
    df["tail_name_raw"] = df["raw_attrs"].map(lambda a: a.get("chemical_name", ""))
    df["conc_value"] = df["raw_attrs"].map(lambda a: a.get("conc_value"))
    df["conc_unit"] = df["raw_attrs"].map(lambda a: a.get("conc_unit", ""))
    df["conc_value_raw"] = df["conc_value"].astype(str)
    df["conc_unit_raw"] = df["conc_unit"]
    df["validated"] = True
    return df


def _create_triplets(
    kg: KnowledgeGraph, resolved: pd.DataFrame, attestations: pd.DataFrame
) -> pd.DataFrame:
    triplet_input = resolved[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = RelationshipType.CONTAINS
    return kg.triplets.create(triplet_input)


def _int_str(value: object) -> str:
    """Normalize a numeric id to a plain integer string (``5192.0`` -> ``5192``)."""
    return str(value).split(".")[0]
