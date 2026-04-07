"""Shared formatting utilities for API responses."""

_SOURCE_CONFIG: dict[str, tuple[str, str]] = {
    "foodon": ("FoodOn", ""),
    "chebi": ("ChEBI", "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:{}"),
    "fdc": ("FDC", "https://fdc.nal.usda.gov/fdc-app.html#/food-details/{}"),
    "fdc_nutrient": ("FDC Nutrient", ""),
    "cdno": ("CDNO", ""),
    "ctd": ("CTD", "https://ctdbase.org/detail.go?type=disease&acc={}"),
    "dmd": ("DMD", ""),
    "pubchem_compound": (
        "PubChem",
        "https://pubchem.ncbi.nlm.nih.gov/compound/{}",
    ),
    "mesh": ("MeSH", "https://meshb.nlm.nih.gov/record/ui?ui={}"),
}


def _make_url(raw_id: str | int, template: str) -> str:
    """Build a URL from an ID and template, or return empty string."""
    sid = str(raw_id)
    if not template:
        return sid if sid.startswith("http") else ""
    return template.format(sid)


def format_external_ids(raw: dict | None) -> dict[str, object]:
    """Transform raw external_ids into the frontend-expected structure.

    Input:  {"foodon": ["http://..."], "chebi": [4775]}
    Output: {"foodon": {"display_name": "FoodOn", "ids": [{"id": "...", "url": "..."}]}}
    """
    if not raw or not isinstance(raw, dict):
        return {}
    result: dict[str, object] = {}
    for key, id_list in raw.items():
        if not isinstance(id_list, list):
            continue
        display_name, url_template = _SOURCE_CONFIG.get(key, (key.upper(), ""))
        ids = [{"id": str(rid), "url": _make_url(rid, url_template)} for rid in id_list]
        result[key] = {"display_name": display_name, "ids": ids}
    return result
