"""Process and clean CDNO ontology data."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_cdno(settings: KGCSettings) -> None:
    """Parse CDNO OWL file and save cleaned data with ChEBI/FDC mappings."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.integration_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    cdno = _parse_cdno_owl(data_dir / "CDNO" / "cdno.owl")
    cdno.to_parquet(dp_dir / "cdno_hierarchy.parquet")

    cdno = cdno[cdno["fdc_nutrient_ids"].apply(len) > 0]
    cdno["chebi_id"] = cdno["chebi_ids"].apply(lambda x: x[0] if x else None)

    cdno["chebi_id"] = cdno["chebi_id"].replace(
        {
            "http://purl.obolibrary.org/obo/CHEBI_80096": (
                "http://purl.obolibrary.org/obo/CHEBI_166888"
            )
        }
    )

    cdno_cleaned = _disambiguate_fdc_ids(cdno)
    cdno_cleaned = cdno_cleaned.reset_index()
    cdno_cleaned = cdno_cleaned.drop_duplicates(subset="index", keep="first")
    cdno_cleaned = cdno_cleaned.set_index("index")

    cdno_cleaned.to_parquet(dp_dir / "cdno_cleaned.parquet")
    logger.info("Processed CDNO: %d entries.", len(cdno_cleaned))


def _parse_cdno_owl(owl_path: Path) -> pd.DataFrame:
    """Parse the CDNO OWL file into a DataFrame."""
    with owl_path.open() as f:
        soup = BeautifulSoup(f, "xml")

    rows = []
    for class_ in soup.find_all("owl:Class"):
        cdno_id = class_.attrs.get("rdf:about")
        if cdno_id is None or class_.find("owl:deprecated"):
            continue

        parents = [
            p.attrs["rdf:resource"]
            for p in class_.find_all("rdfs:subClassOf")
            if p.attrs.get("rdf:resource") is not None
        ]
        fdc_ids = [
            ref.text.split("USDA_fdc_id:")[-1]
            for ref in class_.find_all("oboInOwl:hasDbXref")
            if ref.text.startswith("USDA_fdc_id")
        ]
        chebi_ids = _extract_chebi_ids(class_)
        label_el = class_.find("rdfs:label")
        label = label_el.text if label_el else None

        rows.append(
            {
                "id": cdno_id,
                "label": label,
                "parents": parents,
                "fdc_nutrient_ids": fdc_ids,
                "chebi_ids": chebi_ids,
            }
        )

    return pd.DataFrame(rows).set_index("id")


def _extract_chebi_ids(class_element: Any) -> list[str]:
    """Extract ChEBI IDs from OWL equivalent class definitions."""
    chebi_ids: list[str] = []
    eqs = class_element.find_all("owl:equivalentClass")
    if not eqs:
        return chebi_ids
    for desc in eqs[0].find_all("rdf:Description"):
        about = desc.attrs.get("rdf:about", "")
        if "CHEBI_" in about:
            chebi_ids.append(about)
    return chebi_ids


def _disambiguate_fdc_ids(cdno: pd.DataFrame) -> pd.DataFrame:
    """Ensure each FDC ID maps to exactly one CDNO entry."""
    fdc_ids: set[str] = set()
    for fdc_nutrient_ids in cdno["fdc_nutrient_ids"]:
        fdc_ids.update(fdc_nutrient_ids)

    manual_fixes: dict[str, int] = {
        "1162": 0,
        "1038": 0,
        "1167": 1,
        "1183": 0,
    }

    cleaned_rows = []
    for fdc_id in fdc_ids:
        if fdc_id == "1071":
            continue
        matches = cdno[cdno["fdc_nutrient_ids"].apply(lambda x, fid=fdc_id: fid in x)]
        if len(matches) == 1:
            cleaned_rows.append(matches.iloc[0])
            continue

        matches_with_chebi = matches.dropna(subset=["chebi_id"])
        if len(matches_with_chebi) == 1:
            cleaned_rows.append(matches_with_chebi.iloc[0])
            continue

        if fdc_id in manual_fixes:
            cleaned_rows.append(matches_with_chebi.iloc[manual_fixes[fdc_id]])
        else:
            msg = f"Ambiguous FDC ID: {fdc_id}"
            raise ValueError(msg)

    result = pd.DataFrame(cleaned_rows)
    _validate_fdc_chebi_mapping(result)
    return result


def _validate_fdc_chebi_mapping(cdno_cleaned: pd.DataFrame) -> None:
    """Verify FDC->ChEBI is one-to-one."""
    map_fdc2chebi: dict[str, set[str]] = {}
    for _, row in cdno_cleaned.iterrows():
        for fdc_id in row["fdc_nutrient_ids"]:
            if fdc_id not in map_fdc2chebi:
                map_fdc2chebi[fdc_id] = set()
            if pd.notna(row["chebi_id"]):
                map_fdc2chebi[fdc_id].add(row["chebi_id"])
            else:
                map_fdc2chebi[fdc_id].add(str(row.name))

    for fdc_id, chebi_ids in map_fdc2chebi.items():
        if len(chebi_ids) != 1:
            msg = f"FDC->ChEBI not one-to-one for {fdc_id}: {chebi_ids}"
            raise ValueError(msg)
