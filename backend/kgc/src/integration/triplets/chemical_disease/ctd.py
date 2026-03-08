"""Merge CTD disease triplets and metadata into the knowledge graph."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ...entities.disease.constants import (
    CTD_DIRECTEVIDENCE,
    CTD_DIRECTEVIDENCE_MAPPING,
    CTD_PUBMED_IDS,
    FA_ID,
    PMCID,
    PMID_PMCID_REQUEST_URL,
    PUBMED_IDS,
)
from ...entities.disease.loaders import (
    extract_pubmed_ids,
    filter_ctd_chemdis,
    load_ctd_chemdis,
)

if TYPE_CHECKING:
    from ....constructor.knowledge_graph import KnowledgeGraph
    from ....models.settings import KGCSettings

logger = logging.getLogger(__name__)

PMID_TO_PMCID_FILENAME = "CTD_pubmed_ids_to_pmcid.csv"
BATCH_SIZE = 200


def fetch_pmid_to_pmcid(
    pubmed_ids: list[int],
    email: str,
    tool_name: str = "foodatlas",
) -> pd.DataFrame:
    """Fetch PMID→PMCID mappings from NCBI in batches.

    Args:
        pubmed_ids: List of integer PubMed IDs.
        email: Contact email for the NCBI API.
        tool_name: Tool identifier for the NCBI API.

    Returns:
        DataFrame with ``pmid`` and ``pmcid`` columns.
    """
    middle_url = f"?tool={tool_name}&email={email}&ids="
    dfs: list[pd.DataFrame] = []

    for i in range(0, len(pubmed_ids), BATCH_SIZE):
        batch = pubmed_ids[i : i + BATCH_SIZE]
        ids_str = ",".join(str(pid) for pid in batch)
        url = PMID_PMCID_REQUEST_URL + middle_url + ids_str
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.error("NCBI request failed with status %d", response.status_code)
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        records = soup.find_all("record")
        pmid_list = [record.get("pmid") for record in records]
        pmcid_list = [record.get("pmcid") for record in records]
        dfs.append(pd.DataFrame({PUBMED_IDS: pmid_list, PMCID: pmcid_list}))

    if not dfs:
        return pd.DataFrame(columns=[PUBMED_IDS, PMCID])

    result = pd.concat(dfs, ignore_index=True)
    return result.sort_values(by=PUBMED_IDS).reset_index(drop=True)


def load_pmid_to_pmcid(mapping_path: Path) -> dict[int, int]:
    """Load a PMID→PMCID CSV mapping, returning only valid entries.

    Args:
        mapping_path: Path to CSV with ``pmid`` and ``pmcid`` columns.

    Returns:
        Dict mapping integer PMID to integer PMCID.
    """
    df = pd.read_csv(mapping_path)
    df = df.dropna(how="all").reset_index(drop=True)
    df[PMCID] = df[PMCID].apply(
        lambda x: x.split("PMC")[1] if pd.notnull(x) and "PMC" in str(x) else None
    )
    df = df[df[PMCID].notnull()].reset_index(drop=True)
    df[PMCID] = df[PMCID].astype(int)
    df[PUBMED_IDS] = df[PUBMED_IDS].astype(int)
    return dict(zip(df[PUBMED_IDS], df[PMCID], strict=False))


def get_or_create_pmid_mapping(
    cache_dir: Path,
    pubmed_ids: list[int],
    email: str,
) -> dict[int, int]:
    """Load cached PMID→PMCID mapping or create it from NCBI.

    Args:
        cache_dir: Directory to cache the mapping CSV.
        pubmed_ids: PubMed IDs to map (used only when creating).
        email: NCBI contact email.

    Returns:
        Dict mapping integer PMID to integer PMCID.
    """
    cache_path = cache_dir / PMID_TO_PMCID_FILENAME
    if cache_path.exists():
        logger.info("Loading cached PMID→PMCID mapping from %s", cache_path)
        return load_pmid_to_pmcid(cache_path)

    logger.info("Fetching PMID→PMCID mapping from NCBI for %d IDs...", len(pubmed_ids))
    df = fetch_pmid_to_pmcid(pubmed_ids, email)
    cache_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False)
    return load_pmid_to_pmcid(cache_path)


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


def merge_ctd_triplets(kg: KnowledgeGraph, settings: KGCSettings) -> None:
    """Create disease triplets and metadata from CTD data.

    Requires disease entities to already be present in the KG (via
    ``initialization.disease.init_entities.append_diseases_from_ctd``).

    Args:
        kg: Loaded KnowledgeGraph instance with disease entities.
        settings: KGCSettings with data paths and NCBI email.
    """
    cache_dir = Path(settings.data_cleaning_dir)

    ctd_chemdis = load_ctd_chemdis(settings)
    ctd_chemdis = filter_ctd_chemdis(ctd_chemdis, kg.entities)
    pubmed_ids = sorted(extract_pubmed_ids(ctd_chemdis))

    pmid_to_pmcid = get_or_create_pmid_mapping(
        cache_dir, pubmed_ids, settings.ncbi_email
    )

    max_tid = 0
    if not kg.triplets._triplets.empty:
        max_tid = int(kg.triplets._triplets.index.str.slice(1).astype(int).max())

    triplets, metadata = create_disease_triplets_metadata(
        ctd_chemdis,
        kg.entities._entities,
        pmid_to_pmcid,
        max_tid,
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
