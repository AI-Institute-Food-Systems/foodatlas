"""ID prefix mapping and lookup key generation."""

ID_PREFIX_MAPPER = {
    "foodon_id": "FOODON_ID",
    "pubchem_cid": "PubChem_Compound_ID",
    "fdc_nutrient_ids": "FDC_Nutrient_ID",
    "fdc_ids": "FDC_ID",
}

_LOOKUP_BY_ID = "_{}:{}"


def get_lookup_key_by_id(id_type: str, id_value: str | int) -> str:
    if id_type not in ID_PREFIX_MAPPER:
        msg = f"Unknown ID type: {id_type}"
        raise ValueError(msg)
    return _LOOKUP_BY_ID.format(ID_PREFIX_MAPPER[id_type], id_value)
