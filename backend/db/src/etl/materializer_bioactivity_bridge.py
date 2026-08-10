"""Shared evidence join behind the bioactivity-disease inference views.

Two MVs are built from the same walk through the graph:

    r6 (chemical measured for bioactivity) --attestation_ids--> bm measurements
    bm (Active) --source_assay_id--> assay
    assay --bridge--> disease (MeSH -> KG disease via external_ids.ctd)

``mv_chemical_disease_bioactivity`` collapses the assay away and keeps
(chemical, disease). ``mv_disease_bioactivity`` keeps the assay's *bioactivity*
so a disease can be described by the activities that actually bridge to it.
Both start from :func:`build_bridge_evidence`, so the two views can never
disagree about which assays link what.
"""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def build_bridge_evidence(conn: Connection) -> tuple[pd.DataFrame, dict[str, str]]:
    """One row per (chemical, bridging assay, active measurement, disease).

    Returns the evidence frame and a ``foodatlas_id -> common_name`` map for
    labelling. The frame is empty when either side of the bridge is missing,
    which callers should treat as "nothing to materialize".
    """
    bridge = pd.read_sql(
        text(
            "SELECT disease_mesh_id, source_assay_id, relationship,"
            " bioactivity_disease_metadata_id FROM base_bioactivity_disease"
        ),
        conn,
    )
    chem_assay = chemical_active_assays(conn)
    if bridge.empty or chem_assay.empty:
        return pd.DataFrame(), {}

    entities = pd.read_sql(
        text(
            "SELECT foodatlas_id, entity_type, common_name, external_ids"
            " FROM base_entities"
        ),
        conn,
    )
    name_map = dict(
        zip(entities["foodatlas_id"], entities["common_name"], strict=False)
    )

    evidence = chem_assay.merge(bridge, on="source_assay_id", how="inner")
    evidence["disease_id"] = evidence["disease_mesh_id"].map(disease_mesh_map(entities))
    return evidence.dropna(subset=["disease_id"]), name_map


def chemical_active_assays(conn: Connection) -> pd.DataFrame:
    """(chemical_id, source_assay_id, bm) for each ACTIVE measurement of an r6 edge."""
    r6 = pd.read_sql(
        text(
            "SELECT head_id, attestation_ids FROM base_triplets"
            " WHERE relationship_id = 'r6'"
        ),
        conn,
    )
    r6 = r6.explode("attestation_ids").dropna(subset=["attestation_ids"])
    r6 = r6.rename(columns={"head_id": "chemical_id", "attestation_ids": "bm"})
    active = pd.read_sql(
        text(
            "SELECT bioactivity_metadata_id AS bm, source_assay_id"
            " FROM base_attestations_bioactivity"
            " WHERE reported_activity_outcome = 'Active'"
        ),
        conn,
    )
    merged = r6.merge(active, on="bm", how="inner")
    return merged[["chemical_id", "source_assay_id", "bm"]].drop_duplicates()


def disease_mesh_map(entities: pd.DataFrame) -> dict[str, str]:
    """MeSH id (from disease external_ids.ctd) → disease foodatlas_id."""
    out: dict[str, str] = {}
    diseases = entities[entities["entity_type"] == "disease"]
    pairs = zip(diseases["foodatlas_id"], diseases["external_ids"], strict=False)
    for fid, ext in pairs:
        for mesh in (ext.get("ctd", []) if isinstance(ext, dict) else []) or []:
            out.setdefault(str(mesh), fid)
    return out


def assay_bioactivity_map(conn: Connection) -> pd.DataFrame:
    """(source_assay_id, bioactivity_id) pairs — an assay can carry several.

    ``base_bioassays.bioactivity_ids`` stores concept codes (``E300003``), not
    entity ids; the join goes through ``external_ids.bioactivity_concept``.
    Assays whose concept has no entity are dropped rather than guessed at.
    """
    assays = pd.read_sql(
        text("SELECT source_assay_id, bioactivity_ids FROM base_bioassays"), conn
    )
    assays = assays.explode("bioactivity_ids").dropna(subset=["bioactivity_ids"])
    if assays.empty:
        return pd.DataFrame(columns=["source_assay_id", "bioactivity_id"])

    entities = pd.read_sql(
        text(
            "SELECT foodatlas_id, external_ids FROM base_entities"
            " WHERE entity_type = 'bioactivity'"
        ),
        conn,
    )
    assays["bioactivity_id"] = assays["bioactivity_ids"].map(
        bioactivity_concept_map(entities)
    )
    unmapped = assays["bioactivity_id"].isna().sum()
    if unmapped:
        logger.info("%d assay-bioactivity links had no entity (dropped).", unmapped)
    assays = assays.dropna(subset=["bioactivity_id"])
    return assays[["source_assay_id", "bioactivity_id"]].drop_duplicates()


def bioactivity_concept_map(entities: pd.DataFrame) -> dict[str, str]:
    """Concept code (``E300003``) → bioactivity foodatlas_id."""
    out: dict[str, str] = {}
    pairs = zip(entities["foodatlas_id"], entities["external_ids"], strict=False)
    for fid, ext in pairs:
        concepts = (
            ext.get("bioactivity_concept", []) if isinstance(ext, dict) else []
        ) or []
        for concept in concepts:
            out.setdefault(str(concept), fid)
    return out
