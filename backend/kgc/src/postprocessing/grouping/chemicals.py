"""Chemical entity grouping via CDNO and ChEBI ontology hierarchies."""

import json
import logging
from pathlib import Path

import pandas as pd

from ...models.settings import KGCSettings
from ...stores.entity_store import EntityStore

logger = logging.getLogger(__name__)

_CDNO_GROUP_ROOTS: dict[str, list[str]] = {
    "http://purl.obolibrary.org/obo/CDNO_0200464": ["amino acid"],
    "http://purl.obolibrary.org/obo/CDNO_0200040": ["protein"],
    "http://purl.obolibrary.org/obo/CDNO_0200005": ["carbohydrate"],
    "http://purl.obolibrary.org/obo/CDNO_0200035": ["dietary fiber"],
    "http://purl.obolibrary.org/obo/CDNO_0200068": ["lipid"],
    "http://purl.obolibrary.org/obo/CDNO_0200179": ["vitamin"],
    "http://purl.obolibrary.org/obo/CDNO_0200004": ["ash"],
    "http://purl.obolibrary.org/obo/CDNO_0200136": ["mineral nutrient"],
    "http://purl.obolibrary.org/obo/CDNO_0200422": ["organic acid"],
    "http://purl.obolibrary.org/obo/CDNO_0200215": ["plant secondary metabolite"],
    "http://purl.obolibrary.org/obo/CDNO_0200002": ["water"],
}

_CDNO_LABEL_MAP: dict[str, str] = {
    "amino acid": "amino acid and protein",
    "protein": "amino acid and protein",
    "carbohydrate": "carbohydrate (including fiber)",
    "dietary fiber": "carbohydrate (including fiber)",
    "lipid": "lipid",
    "vitamin": "vitamin",
    "mineral nutrient": "mineral (including derivatives)",
    "ash": "mineral (including derivatives)",
}


def generate_chemical_groups_cdno(
    chemicals: pd.DataFrame,
    settings: KGCSettings,
) -> pd.Series:
    """Assign CDNO-based nutrient groups to chemical entities.

    Returns a Series indexed like *chemicals* with list[str] group labels.
    """
    dp_dir = Path(settings.integration_dir)
    cdno: pd.DataFrame = pd.read_parquet(dp_dir / "cdno_hierarchy.parquet")

    cdno["chebi_id"] = cdno["chebi_ids"].apply(lambda x: x[0] if x else None)
    cdno["chebi_id"] = cdno["chebi_id"].replace(
        {
            "http://purl.obolibrary.org/obo/CHEBI_80096": (
                "http://purl.obolibrary.org/obo/CHEBI_166888"
            )
        }
    )

    id2parent: dict[str, list[str]] = dict(_CDNO_GROUP_ROOTS)
    _traverse_cdno_hierarchy(cdno, id2parent)

    chebi2group = _build_chebi_to_group(cdno, id2parent)
    eid2group = _map_entities_to_groups(chemicals, chebi2group)

    return chemicals.index.map(lambda eid: _assign_label(eid, eid2group))


def _traverse_cdno_hierarchy(
    cdno: pd.DataFrame,
    id2parent: dict[str, list[str]],
) -> None:
    """DFS to propagate group labels down the CDNO hierarchy."""

    def dfs(cdno_id: str) -> list[str]:
        if cdno_id in id2parent:
            return id2parent[cdno_id]
        if cdno_id not in cdno.index:
            return []
        results: set[str] = set()
        for parent_id in cdno.loc[cdno_id]["parents"]:
            results.update(dfs(parent_id))
        id2parent[cdno_id] = sorted(results)
        return id2parent[cdno_id]

    for idx in cdno.index:
        dfs(idx)


def _build_chebi_to_group(
    cdno: pd.DataFrame,
    id2parent: dict[str, list[str]],
) -> dict[int, list[str]]:
    """Map ChEBI IDs (int) to CDNO group labels via the hierarchy."""
    cdno_with_groups = cdno.copy()
    cdno_with_groups["cdno_groups"] = cdno_with_groups.index.map(
        lambda x: id2parent.get(x, [])
    )
    cdno_with_groups = cdno_with_groups[cdno_with_groups["cdno_groups"].apply(len) > 0]

    chebi2group: dict[int, list[str]] = {}
    for _, row in cdno_with_groups.iterrows():
        chebi_id = row["chebi_id"]
        if chebi_id and not pd.isna(chebi_id):
            chebi_int = int(str(chebi_id).split("_")[-1])
            chebi2group[chebi_int] = row["cdno_groups"]
    return chebi2group


def _map_entities_to_groups(
    chemicals: pd.DataFrame,
    chebi2group: dict[int, list[str]],
) -> dict[str, list[str]]:
    """Map entity IDs to CDNO groups via their ChEBI external IDs."""
    eid2group: dict[str, list[str]] = {}
    for eid, row in chemicals.iterrows():
        ext = row["external_ids"]
        if "chebi" in ext and ext["chebi"][0] in chebi2group:
            eid2group[str(eid)] = chebi2group[ext["chebi"][0]]
    return eid2group


def _assign_label(eid: str, eid2group: dict[str, list[str]]) -> list[str]:
    """Map raw CDNO group names to display labels."""
    if eid not in eid2group:
        return ["others"]
    return sorted({_CDNO_LABEL_MAP.get(g, "others") for g in eid2group[eid]})


def generate_chemical_groups_chebi(
    chemicals: pd.DataFrame,
    entity_store: EntityStore,
    settings: KGCSettings,
) -> pd.Series:
    """Assign ChEBI-hierarchy groups at depth 1 from the root chemical entity.

    Returns a Series indexed like *chemicals* with list[str] group names.
    """
    kg_dir = Path(settings.kg_dir)
    chemonto = _load_chemical_ontology(kg_dir)

    ht_is_a = _build_is_a_map(chemonto)
    ht_has_child = _invert_is_a(ht_is_a)

    root_eid = _find_root_chemical(entity_store)
    group_eids = _get_group_eids_at_level(root_eid, ht_has_child, level=1)

    return chemicals.apply(
        lambda row: _map_chebi_group(row, ht_is_a, group_eids, entity_store._entities),
        axis=1,
    )


def _load_chemical_ontology(kg_dir: Path) -> pd.DataFrame:
    """Load chemical ontology triplets and filter to ChEBI source."""
    with (kg_dir / "chemical_ontology.json").open() as f:
        records = json.load(f)
    df = pd.DataFrame(records)
    return df[df["source"] == "chebi"]


def _build_is_a_map(ontology: pd.DataFrame) -> dict[str, list[str]]:
    ht: dict[str, list[str]] = {}
    for _, row in ontology.iterrows():
        head = row["head_id"]
        if head not in ht:
            ht[head] = []
        ht[head].append(row["tail_id"])
    return ht


def _invert_is_a(ht_is_a: dict[str, list[str]]) -> dict[str, set[str]]:
    ht_has_child: dict[str, set[str]] = {}
    for child, parents in ht_is_a.items():
        for parent in parents:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(child)
    return ht_has_child


def _find_root_chemical(entity_store: EntityStore) -> str:
    """Find the entity ID for 'chemical entity' (ChEBI root)."""
    ids = entity_store.get_entity_ids("chemical", "chemical entity")
    if not ids:
        msg = "Root 'chemical entity' not found in entity store."
        raise ValueError(msg)
    return ids[0]


def _get_group_eids_at_level(
    root: str,
    ht_has_child: dict[str, set[str]],
    level: int = 1,
) -> list[str]:
    """BFS to get entity IDs at *level* hops from *root*."""
    queue = [root]
    for _ in range(level):
        next_level: list[str] = []
        for current in queue:
            if current in ht_has_child:
                next_level.extend(ht_has_child[current])
            else:
                next_level.append(current)
        queue = list(set(next_level))
    return queue


def _map_chebi_group(
    row: pd.Series,
    ht_is_a: dict[str, list[str]],
    group_eids: list[str],
    all_entities: pd.DataFrame,
) -> list[str]:
    """Traverse ChEBI hierarchy to find which groups this entity belongs to."""
    if "chebi" not in row["external_ids"]:
        return ["unclassified"]

    matched: list[str] = []
    queue = [row.name]
    while queue:
        current = queue.pop(0)
        if current not in ht_is_a:
            continue
        if current in group_eids:
            matched.append(current)
        else:
            queue.extend(ht_is_a[current])

    if not matched:
        return ["unclassified"]

    names = all_entities.loc[
        all_entities.index.intersection(matched), "common_name"
    ].tolist()
    return sorted(set(names)) if names else ["unclassified"]
