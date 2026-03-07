"""Step 3: Merge CTD disease data into the knowledge graph."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .constants import (
    CTD_DIRECTEVIDENCE,
    CTD_DIRECTEVIDENCE_MAPPING,
    CTD_PUBMED_IDS,
    FA_ID,
    PMCID,
    PUBMED_IDS,
)
from .disease_entities import create_disease_entities, get_max_entity_id
from .pmid_mapping import get_or_create_pmid_mapping
from .processing import extract_pubmed_ids, filter_ctd_chemdis, load_ctd_data

if TYPE_CHECKING:
    from ...constructor.knowledge_graph import KnowledgeGraph
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def build_chemical_id_map(entities: pd.DataFrame) -> dict[str, list[str]]:
    """Map MeSH chemical IDs to FoodAtlas entity IDs.

    Args:
        entities: Entity DataFrame (chemical type, with ``external_ids``).

    Returns:
        Dict mapping MeSH ID string to list of entity ID strings.
    """
    chemicals = entities[entities["entity_type"] == "chemical"]
    chem_map: dict[str, list[str]] = {}
    for entity_id, row in chemicals.iterrows():
        for mesh_id in row["external_ids"].get("mesh", []):
            mesh_key = str(mesh_id)
            if mesh_key not in chem_map:
                chem_map[mesh_key] = []
            chem_map[mesh_key].append(str(entity_id))
    return chem_map


def build_disease_id_map(entities: pd.DataFrame) -> dict[str, list[str]]:
    """Map CTD disease IDs (MESH:/OMIM:) to FoodAtlas entity IDs.

    Args:
        entities: Entity DataFrame (disease type, with ``external_ids``).

    Returns:
        Dict mapping prefixed disease ID to list of entity ID strings.
    """
    diseases = entities[entities["entity_type"] == "disease"]
    dis_map: dict[str, list[str]] = {}
    for entity_id, row in diseases.iterrows():
        ext = row["external_ids"]
        if "mesh" in ext:
            for mesh_id in ext["mesh"]:
                key = f"MESH:{mesh_id}"
                dis_map[key] = [str(entity_id)]
        if "omim" in ext:
            for omim_id in ext["omim"]:
                key = f"OMIM:{omim_id}"
                dis_map[key] = [str(entity_id)]
    return dis_map


def create_disease_triplets_metadata(
    ctd_chemdis: pd.DataFrame,
    fa_entities: pd.DataFrame,
    pmid_to_pmcid: dict[int, int],
    max_triplet_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create disease triplets and metadata from CTD chemical-disease data.

    Args:
        ctd_chemdis: Filtered CTD chemical-disease DataFrame.
        fa_entities: Full entities DataFrame (indexed by foodatlas_id).
        pmid_to_pmcid: PMID to PMCID mapping.
        max_triplet_id: Current max triplet ID for generating new IDs.

    Returns:
        Tuple of (triplets DataFrame, metadata DataFrame).
    """
    chem_map = build_chemical_id_map(fa_entities)
    dis_map = build_disease_id_map(fa_entities)

    ctd_chemdis = ctd_chemdis.copy()
    ctd_chemdis["head_id"] = ctd_chemdis["ChemicalID"].apply(
        lambda x: chem_map.get(x, [])
    )
    ctd_chemdis["tail_id"] = ctd_chemdis["DiseaseID"].apply(
        lambda x: dis_map.get(x, [])
    )

    ctd_chemdis = ctd_chemdis.explode("head_id").explode("tail_id")
    ctd_chemdis = ctd_chemdis.dropna(subset=["head_id", "tail_id"]).reset_index(
        drop=True
    )

    ctd_chemdis["relationship_id"] = ctd_chemdis[CTD_DIRECTEVIDENCE].apply(
        lambda x: CTD_DIRECTEVIDENCE_MAPPING[x]
    )

    ctd_chemdis = ctd_chemdis.explode(CTD_PUBMED_IDS).reset_index(drop=True)

    ctd_chemdis["metadata_ids"] = [[f"md{i + 1}"] for i in range(len(ctd_chemdis))]

    metadata = pd.DataFrame()
    metadata[FA_ID] = ctd_chemdis["metadata_ids"].apply(lambda x: x[0])
    metadata["source"] = "ctd"
    metadata["reference"] = ctd_chemdis[CTD_PUBMED_IDS].apply(
        lambda x: {
            k: v
            for k, v in {
                PUBMED_IDS: x,
                PMCID: pmid_to_pmcid.get(int(x)) if pd.notnull(x) else None,
            }.items()
            if v is not None
        }
    )
    metadata["entity_linking_method"] = "id_matching"
    metadata["_chemical_name"] = ctd_chemdis["ChemicalID"].apply(lambda x: f"MESH:{x}")
    metadata["_disease_name"] = ctd_chemdis["DiseaseID"]

    triplets = (
        ctd_chemdis.groupby(["head_id", "relationship_id", "tail_id"])
        .agg(metadata_ids=("metadata_ids", lambda x: [i for sub in x for i in sub]))
        .reset_index()
    )

    def _extract_min_id(ids: list[str]) -> int:
        nums = []
        for i in ids:
            match = re.search(r"\d+", i)
            if match:
                nums.append(int(match.group()))
        return min(nums) if nums else 0

    triplets["_sort_key"] = triplets["metadata_ids"].apply(_extract_min_id)
    triplets = triplets.sort_values(by="_sort_key").reset_index(drop=True)
    triplets = triplets.drop(columns=["_sort_key"])
    triplets[FA_ID] = [f"t{max_triplet_id + i + 1}" for i in range(len(triplets))]

    return triplets, metadata


def merge_ctd(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Merge CTD disease data into the knowledge graph.

    Three-step pipeline:
    1. Filter raw CTD data
    2. Map PMIDs to PMCIDs
    3. Create disease entities, triplets, and metadata

    Args:
        kg: Loaded KnowledgeGraph instance.
        settings: KGCSettings with data paths and NCBI email.
    """
    ctd_dir = Path(settings.data_dir) / "CTD"
    integration_dir = Path(settings.integration_dir)

    ctd_chemdis = load_ctd_data(ctd_dir, dataset="chemdis")
    ctd_diseases = load_ctd_data(ctd_dir, dataset="disease")

    ctd_chemdis = filter_ctd_chemdis(ctd_chemdis, kg.entities)
    pubmed_ids = sorted(extract_pubmed_ids(ctd_chemdis))

    pmid_to_pmcid = get_or_create_pmid_mapping(
        integration_dir, pubmed_ids, settings.ncbi_email
    )

    entities_df = kg.entities._entities.reset_index()
    max_eid = get_max_entity_id(entities_df)
    entities_df = create_disease_entities(
        entities_df, ctd_diseases, ctd_chemdis, max_eid
    )

    max_tid = 0
    if not kg.triplets._triplets.empty:
        max_tid = int(kg.triplets._triplets.index.str.slice(1).astype(int).max())

    triplets, metadata = create_disease_triplets_metadata(
        ctd_chemdis, entities_df.set_index(FA_ID), pmid_to_pmcid, max_tid
    )

    triplets_df = triplets[
        ["foodatlas_id", "head_id", "relationship_id", "tail_id", "metadata_ids"]
    ]
    kg.triplets._triplets = pd.concat(
        [
            kg.triplets._triplets,
            triplets_df.set_index("foodatlas_id"),
        ]
    )
    kg.triplets._curr_tid = (
        kg.triplets._triplets.index.str.slice(1).astype(int).max() + 1
    )

    logger.info(
        "Merged %d CTD triplets and %d metadata rows.", len(triplets), len(metadata)
    )
