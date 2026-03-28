"""Query external data sources (NCBI Taxonomy, PubChem).

All query functions accept a ``KGCSettings`` instance for configuration
injection — no module-level side effects.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from Bio import Entrez
from tqdm import tqdm

from ..utils.constants import get_lookup_key_by_id
from ..utils.json_io import read_json, write_json
from .cache import (
    incremental_save,
    load_cached,
    save_cached,
)

if TYPE_CHECKING:
    from ..models.settings import KGCSettings

logger = logging.getLogger(__name__)

# Biopython stubs type email/api_key as None; cast module to Any for assignment.
_entrez: Any = Entrez

_CACHE_SEARCH_NCBI = "_cached_search_ncbi_taxonomy.json"
_CACHE_FETCH_NCBI = "_cached_fetch_ncbi_taxonomy.json"
_CACHE_CHEMICAL_TERMS = "_cached_chemical_terms.json"
_CACHE_FETCH_PUBCHEM = "_cached_fetch_pubchem_compound.json"
_BATCH_SIZE = 100


def _configure_entrez(settings: KGCSettings) -> None:
    """Set Entrez credentials from settings (no file reads)."""
    _entrez.email = settings.ncbi_email
    if settings.ncbi_api_key:
        _entrez.api_key = settings.ncbi_api_key


def _load_lookup_tables(path_kg: Path) -> tuple[dict, dict]:
    """Load food and chemical lookup tables from the KG directory."""
    luts: list[dict] = []
    for suffix in ["food", "chemical"]:
        lut: dict[str, list[str]] = read_json(path_kg / f"lookup_table_{suffix}.json")
        luts.append(lut)
    return luts[0], luts[1]


def _search_ncbi_taxonomy(
    food_names: list[str],
    cache_dir: Path,
) -> pd.DataFrame:
    """Search NCBI Taxonomy for food names, with per-batch caching."""
    records_search = load_cached(cache_dir, _CACHE_SEARCH_NCBI)

    if not records_search.empty:
        names_searched = set(records_search["search_term"].tolist())
        food_names = [x for x in food_names if x not in names_searched]

    if not food_names:
        return records_search

    batch_rows: list[dict] = []
    for i, name in enumerate(tqdm(food_names, desc="NCBI search")):
        handle = Entrez.esearch(db="taxonomy", term=f'"{name}"')
        record = Entrez.read(handle)
        batch_rows.append(record)

        if i % _BATCH_SIZE == _BATCH_SIZE - 1 or i == len(food_names) - 1:
            batch_start = i - (len(batch_rows) - 1)
            batch_names = food_names[batch_start : i + 1]
            records_search, batch_rows = incremental_save(
                records_search,
                batch_rows,
                batch_names,
                "search_term",
                cache_dir,
                _CACHE_SEARCH_NCBI,
            )

    return records_search


def _fetch_ncbi_taxonomy(
    ncbi_taxon_ids: list[int],
    path_kg: Path | None,
    cache_dir: Path,
) -> pd.DataFrame:
    """Fetch full NCBI Taxonomy records for given taxon IDs."""
    if path_kg is not None:
        lut_food, _ = _load_lookup_tables(path_kg)
        ncbi_taxon_ids = [
            int(x)
            for x in ncbi_taxon_ids
            if get_lookup_key_by_id("ncbi_taxon_id", x) not in lut_food
        ]

    records_fetch = load_cached(cache_dir, _CACHE_FETCH_NCBI)

    ids_not_fetched = ncbi_taxon_ids
    if not records_fetch.empty:
        fetched = set(records_fetch["TaxId"].tolist())
        ids_not_fetched = [x for x in ncbi_taxon_ids if x not in fetched]

    if ids_not_fetched:
        logger.info("Retrieving %d new NCBI Taxonomy IDs.", len(ids_not_fetched))
        handle = Entrez.efetch(db="taxonomy", id=ids_not_fetched, retmode="xml")
        new_records = pd.DataFrame(Entrez.read(handle))
        records_fetch = pd.concat([records_fetch, new_records], ignore_index=True)
        save_cached(records_fetch, cache_dir, _CACHE_FETCH_NCBI)

    records_fetch = records_fetch.query(
        f"TaxId in {ncbi_taxon_ids} "
        "& Division not in ['Bacteria', 'Viruses', 'Unassigned']"
    )
    return records_fetch


def query_ncbi_taxonomy(
    entity_names: list[str],
    path_kg: Path | None,
    path_cache_dir: Path | None,
    *,
    settings: KGCSettings | None = None,
) -> pd.DataFrame:
    """Query NCBI Taxonomy database for food names.

    Args:
        entity_names: Food names to search.
        path_kg: Path to the knowledge graph directory.
        path_cache_dir: Path to the cache directory.
        settings: KGCSettings instance for API credentials.

    Returns:
        DataFrame of matching NCBI Taxonomy records.
    """
    if settings is not None:
        _configure_entrez(settings)
        if path_cache_dir is None:
            path_cache_dir = Path(settings.cache_dir)

    if path_cache_dir is None:
        msg = "cache_dir must be provided via path_cache_dir or settings"
        raise ValueError(msg)

    path_cache_dir.mkdir(parents=True, exist_ok=True)

    records_search = _search_ncbi_taxonomy(entity_names, path_cache_dir)

    if records_search.empty:
        return pd.DataFrame()

    ncbi_taxon_ids = list(
        {item for items in records_search["IdList"].tolist() for item in items}
    )

    if not ncbi_taxon_ids:
        return pd.DataFrame()

    return _fetch_ncbi_taxonomy(ncbi_taxon_ids, path_kg, path_cache_dir)


def _load_pubchem_mapping(cache_dir: Path) -> pd.DataFrame:
    """Load cached chemical name-to-CID mappings."""
    path = cache_dir / _CACHE_CHEMICAL_TERMS
    if path.exists():
        return pd.DataFrame(read_json(path))
    return pd.DataFrame(columns=["name", "cid"])


def _save_pubchem_mapping(df: pd.DataFrame, cache_dir: Path) -> None:
    """Save chemical name-to-CID mappings to cache."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    write_json(cache_dir / _CACHE_CHEMICAL_TERMS, df.to_dict(orient="records"))


def query_pubchem_compound(
    entity_names: list[str],
    path_kg: Path | None,
    path_cache_dir: Path | None,
    *,
    settings: KGCSettings | None = None,
) -> pd.DataFrame:
    """Query PubChem Compound database for chemical names.

    Args:
        entity_names: Chemical names to search.
        path_kg: Path to the knowledge graph directory.
        path_cache_dir: Path to the cache directory.
        settings: KGCSettings instance for API credentials and mapping file.

    Returns:
        DataFrame of matching PubChem Compound records.
    """
    if settings is not None:
        _configure_entrez(settings)
        if path_cache_dir is None:
            path_cache_dir = Path(settings.cache_dir)

    if path_cache_dir is None:
        msg = "cache_dir must be provided via path_cache_dir or settings"
        raise ValueError(msg)

    path_cache_dir.mkdir(parents=True, exist_ok=True)

    searched = _load_pubchem_mapping(path_cache_dir)
    searched_set = set(searched["name"].tolist()) if not searched.empty else set()
    new_names = [x for x in entity_names if x not in searched_set]

    if new_names:
        mapping_file = settings.pubchem_mapping_file if settings else ""
        if not mapping_file:
            logger.error(
                "Found %d new chemical names not in cache. "
                "Provide --pubchem-mapping-file (KGC_PUBCHEM_MAPPING_FILE) "
                "with PubChem ID Exchange results. "
                "See https://pubchem.ncbi.nlm.nih.gov/idexchange/",
                len(new_names),
            )
        else:
            new_search = pd.read_csv(
                mapping_file,
                sep="\t",
                header=None,
                names=["name", "cid"],
                dtype={"cid": "Int64"},
            )
            searched = pd.concat([searched, new_search], ignore_index=True)

    if searched.empty:
        return pd.DataFrame()

    cids = searched.query(f"name in {entity_names}")["cid"].dropna().unique().tolist()

    if path_kg is not None:
        _, lut_chemical = _load_lookup_tables(path_kg)
        cids = [
            x
            for x in cids
            if get_lookup_key_by_id("pubchem_cid", x) not in lut_chemical
        ]

    records_fetch = load_cached(path_cache_dir, _CACHE_FETCH_PUBCHEM)

    cids_not_fetched = cids
    if not records_fetch.empty:
        fetched = set(records_fetch["CID"].tolist())
        cids_not_fetched = [x for x in cids if x not in fetched]

    if cids_not_fetched:
        logger.info("Retrieving %d new PubChem CIDs.", len(cids_not_fetched))
        cids_str = ",".join(str(x) for x in cids_not_fetched)
        handle = Entrez.esummary(db="pccompound", id=cids_str)
        new_records = pd.DataFrame(Entrez.read(handle))
        records_fetch = pd.concat([records_fetch, new_records], ignore_index=True)
        save_cached(records_fetch, path_cache_dir, _CACHE_FETCH_PUBCHEM)

    _save_pubchem_mapping(searched, path_cache_dir)

    if records_fetch.empty:
        return records_fetch

    return records_fetch.query(f"CID in {cids}")
