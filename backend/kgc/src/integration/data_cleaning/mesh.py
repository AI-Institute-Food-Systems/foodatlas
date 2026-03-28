"""Process and clean MeSH descriptor and supplementary data."""

import logging
from pathlib import Path

import pandas as pd
import xmltodict

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_mesh(settings: KGCSettings) -> None:
    """Parse MeSH XML files and save cleaned JSON output."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    desc = _parse_mesh_desc(data_dir / "MeSH" / "desc2024.xml")
    desc.to_parquet(dp_dir / "mesh_desc_cleaned.parquet")
    logger.info("Processed MeSH descriptors: %d.", len(desc))

    supp = _parse_mesh_supp(data_dir / "MeSH" / "supp2024.xml")
    supp.to_parquet(dp_dir / "mesh_supp_cleaned.parquet")
    logger.info("Processed MeSH supplementals: %d.", len(supp))


def _parse_mesh_desc(xml_path: Path) -> pd.DataFrame:
    """Parse MeSH Descriptor XML into a DataFrame."""
    with xml_path.open() as f:
        data = xmltodict.parse(f.read())

    rows = []
    for record in data["DescriptorRecordSet"]["DescriptorRecord"]:
        mesh_id = record["DescriptorUI"]
        name = record["DescriptorName"]["String"]
        tree_numbers = (
            _ensure_list(record["TreeNumberList"]["TreeNumber"])
            if "TreeNumberList" in record
            else []
        )
        synonyms = _extract_synonyms(record)
        rows.append(
            {
                "id": mesh_id,
                "name": name,
                "synonyms": synonyms,
                "tree_numbers": tree_numbers,
            }
        )

    return pd.DataFrame(rows)


def _parse_mesh_supp(xml_path: Path) -> pd.DataFrame:
    """Parse MeSH Supplementary XML into a DataFrame."""
    with xml_path.open() as f:
        data = xmltodict.parse(f.read())

    rows = []
    for record in data["SupplementalRecordSet"]["SupplementalRecord"]:
        mesh_id = record["SupplementalRecordUI"]
        name = record["SupplementalRecordName"]["String"]
        mapped_to = _ensure_list(record["HeadingMappedToList"]["HeadingMappedTo"])
        mapped_to = [x["DescriptorReferredTo"]["DescriptorUI"] for x in mapped_to]
        synonyms = _extract_synonyms(record)
        rows.append(
            {"id": mesh_id, "name": name, "synonyms": synonyms, "mapped_to": mapped_to}
        )

    return pd.DataFrame(rows)


def _extract_synonyms(record: dict) -> list[str]:
    """Extract all term strings from a MeSH concept list."""
    synonyms: list[str] = []
    concepts = _ensure_list(record["ConceptList"]["Concept"])
    for concept in concepts:
        terms = _ensure_list(concept["TermList"]["Term"])
        for term in terms:
            synonyms.append(term["String"])
    return synonyms


def _ensure_list(val: object) -> list:
    """Wrap scalars/dicts in a list; pass through lists."""
    if isinstance(val, list):
        return val
    if isinstance(val, dict | str):
        return [val]
    msg = f"Unknown type: {type(val)}"
    raise ValueError(msg)
