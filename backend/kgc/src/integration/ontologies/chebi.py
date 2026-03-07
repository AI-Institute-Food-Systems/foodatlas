"""Process and clean ChEBI ontology data."""

import logging
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pandas as pd

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


def process_chebi(settings: KGCSettings) -> None:
    """Clean raw ChEBI data and save name->ID lookup and cleaned compounds."""
    data_dir = Path(settings.data_dir)
    dp_dir = Path(settings.integration_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    chebi = _load_chebi(data_dir)

    chebi.at[221398, "NAME"] = "15G256nu"
    chebi.at[224404, "NAME"] = "15G256omicron"
    chebi = chebi.drop(index=194466)

    chebi_synonyms = _load_synonyms(data_dir, chebi.index)
    lut_chemical = _build_name_lut(chebi, chebi_synonyms)

    lut_chemical.to_parquet(dp_dir / "chebi_name_to_id.parquet")
    chebi.to_parquet(dp_dir / "chebi_cleaned.parquet")
    logger.info("Processed ChEBI: %d compounds.", len(chebi))


def _load_chebi(data_dir: Path) -> pd.DataFrame:
    """Load ChEBI compounds, filter to molecular entities only."""
    chebi: pd.DataFrame = pd.read_csv(
        data_dir / "ChEBI" / "compounds.tsv", sep="\t", encoding="latin1"
    ).set_index("ID")
    chebi["NAME"] = chebi["NAME"].str.lower().str.strip()
    chebi = chebi.query("PARENT_ID.isna()").copy()
    chebi = _label_is_chemical_entity(chebi, data_dir)
    chebi["is_molecular_entity"] = chebi["is_molecular_entity"].fillna(False)
    return chebi.query("is_molecular_entity").copy()


def _load_map_is_a(data_dir: Path) -> dict[int, list[int]]:
    """Load ChEBI is_a relationships as parent -> children mapping."""
    triplets: pd.DataFrame = pd.read_csv(
        data_dir / "ChEBI" / "relation.tsv", sep="\t"
    ).query("TYPE in ['is_a']")

    map_is_a: dict[int, list[int]] = {}
    for _, row in triplets.iterrows():
        head, tail = row["FINAL_ID"], row["INIT_ID"]
        if head not in map_is_a:
            map_is_a[head] = []
        map_is_a[head].append(tail)
    return map_is_a


def _label_is_chemical_entity(chebi: pd.DataFrame, data_dir: Path) -> pd.DataFrame:
    """DFS to label molecular entities (descendants of ChEBI:23367)."""
    map_is_a = _load_map_is_a(data_dir)
    molecular_entity = 23367
    visited: dict[int, bool] = {molecular_entity: True}

    def dfs(chebi_id: int) -> bool:
        if chebi_id in visited:
            return visited[chebi_id]
        if chebi_id not in map_is_a:
            return False
        result = any(dfs(parent) for parent in map_is_a[chebi_id])
        visited[chebi_id] = result
        return result

    for cid in chebi.index:
        dfs(cid)
    chebi["is_molecular_entity"] = chebi.index.map(visited)
    return chebi


def _load_synonyms(data_dir: Path, valid_ids: pd.Index) -> pd.DataFrame:
    """Load English ChEBI synonyms filtered to valid compound IDs."""
    synonyms: pd.DataFrame = pd.read_csv(data_dir / "ChEBI" / "names.tsv", sep="\t")
    synonyms = synonyms.dropna(subset=["NAME"])
    synonyms = synonyms.query("LANGUAGE == 'en'").copy()
    synonyms["NAME"] = synonyms["NAME"].str.lower().str.strip()
    return synonyms[synonyms["COMPOUND_ID"].isin(valid_ids)]


def _build_name_lut(chebi: pd.DataFrame, chebi_synonyms: pd.DataFrame) -> pd.DataFrame:
    """Build a name -> ChEBI ID lookup table with star-priority ordering."""
    lut: dict[str, Any] = {}

    for star in (3, 2):
        chebi_star = chebi[chebi["STAR"] == star]
        for cid, row in chebi_star.iterrows():
            if row["NAME"] not in lut:
                lut[row["NAME"]] = [cid]

        syn_star = chebi_synonyms[chebi_synonyms["COMPOUND_ID"].isin(chebi_star.index)]
        for _, row in syn_star.iterrows():
            name = row["NAME"]
            if name not in lut:
                lut[name] = OrderedDict.fromkeys([row["COMPOUND_ID"]])
            elif not isinstance(lut[name], list):
                lut[name][row["COMPOUND_ID"]] = None

        lut = {k: list(OrderedDict.fromkeys(v).keys()) for k, v in lut.items()}

    return pd.DataFrame(lut.items(), columns=["NAME", "CHEBI_ID"])
