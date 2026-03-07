"""Query stubs for external data sources (NCBI Taxonomy, PubChem).

These functions will be fully implemented in a later story.
They are provided here as stubs so that entity creation modules
can import them without circular dependency issues.
"""

from pathlib import Path

import pandas as pd


def query_ncbi_taxonomy(
    entity_names: list[str],
    path_kg: Path | None,
    path_cache_dir: Path | None,
) -> pd.DataFrame:
    msg = "query_ncbi_taxonomy is not yet implemented"
    raise NotImplementedError(msg)


def query_pubchem_compound(
    entity_names: list[str],
    path_kg: Path | None,
    path_cache_dir: Path | None,
) -> pd.DataFrame:
    msg = "query_pubchem_compound is not yet implemented"
    raise NotImplementedError(msg)
