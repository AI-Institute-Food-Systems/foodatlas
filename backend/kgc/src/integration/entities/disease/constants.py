"""Constants for CTD integration pipeline."""

# Column names used throughout the CTD pipeline
CTD_CHEMICAL_ID = "ChemicalID"
CTD_DISEASE_ID = "DiseaseID"
CTD_DIRECTEVIDENCE = "DirectEvidence"
CTD_PUBMED_IDS = "PubMedIDs"
FA_ID = "foodatlas_id"
PUBMED_IDS = "pmid"
PMCID = "pmcid"
ENTITY_TYPE = "entity_type"
EXTERNAL_IDS = "external_ids"

# CTD alt-ID key mappings (CTD prefix -> internal key)
CTD_ALTID_MAPPING: dict[str, str] = {
    "DO": "diseaseontology",
    "MESH": "mesh",
    "OMIM": "omim",
}

CTD_REVERSE_ALTID_MAPPING: dict[str, str] = {
    "diseaseontology": "DO",
    "mesh": "MESH",
    "omim": "OMIM",
}

# DirectEvidence -> RelationshipType value
CTD_DIRECTEVIDENCE_MAPPING: dict[str, str] = {
    "marker/mechanism": "r3",
    "therapeutic": "r4",
}

# Columns in CTD data that contain pipe-delimited lists
CTD_COLUMNS_WITH_LISTS: list[str] = [
    "OmimIDs",
    "PubMedIDs",
    "ParentIDs",
    "TreeNumbers",
    "ParentTreeNumbers",
    "Synonyms",
    "AltDiseaseIDs",
    "SlimMappings",
]

# NCBI ID converter endpoint
PMID_PMCID_REQUEST_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"

# CTD source filenames
CTD_CHEMDIS_FILENAME = "CTD_chemicals_diseases.csv"
CTD_DISEASE_FILENAME = "CTD_diseases.csv"
CTD_CHEM_FILENAME = "CTD_chemicals.csv"
