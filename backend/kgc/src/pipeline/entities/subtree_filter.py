"""Stage 2: Ontology subtree filtering using DFS."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ...config.corrections import OntologyRoots

logger = logging.getLogger(__name__)


def filter_sources(
    sources: dict[str, dict[str, pd.DataFrame]],
    roots: OntologyRoots,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Apply subtree and domain filters to Phase 1 DataFrames.

    Args:
        sources: ``{source_id: {"nodes": df, "edges": df, ...}}``.
        roots: Ontology root IDs from corrections config.

    Returns:
        The same dict with filtered DataFrames.
    """
    _filter_foodon(sources, roots)
    _filter_chebi(sources, roots)
    _filter_ctd(sources)
    _filter_cdno(sources)
    return sources


def _compute_descendants(
    edges: pd.DataFrame,
    root_id: str,
    edge_type: str = "is_a",
) -> set[str]:
    """DFS to find all nodes that descend from *root_id* via *edge_type*."""
    is_a = edges[edges["edge_type"] == edge_type]
    parent_map: dict[str, list[str]] = {}
    for _, row in is_a.iterrows():
        child = row["head_native_id"]
        parent = row["tail_native_id"]
        if child not in parent_map:
            parent_map[child] = []
        parent_map[child].append(parent)

    visited: dict[str, bool] = {root_id: True}

    def dfs(node_id: str) -> bool:
        if node_id in visited:
            return visited[node_id]
        if node_id not in parent_map:
            visited[node_id] = False
            return False
        result = any(dfs(p) for p in parent_map[node_id])
        visited[node_id] = result
        return result

    all_nodes = set(is_a["head_native_id"]) | set(is_a["tail_native_id"])
    for node_id in all_nodes:
        dfs(node_id)

    return {nid for nid, is_desc in visited.items() if is_desc}


def _filter_foodon(
    sources: dict[str, dict[str, pd.DataFrame]],
    roots: OntologyRoots,
) -> None:
    foodon = sources.get("foodon")
    if foodon is None:
        return
    nodes: pd.DataFrame = foodon["nodes"]
    edges: pd.DataFrame = foodon["edges"]

    is_food = _compute_descendants(edges, roots.foodon_is_food)
    is_organism = _compute_descendants(edges, roots.foodon_is_organism)
    keep = is_food | is_organism

    before = len(nodes)
    nodes = nodes[nodes["native_id"].isin(keep)].copy()
    nodes["is_food"] = nodes["native_id"].isin(is_food)
    nodes["is_organism"] = nodes["native_id"].isin(is_organism)
    foodon["nodes"] = nodes

    keep_ids = set(nodes["native_id"])
    edges = edges[
        edges["head_native_id"].isin(keep_ids) | edges["tail_native_id"].isin(keep_ids)
    ].copy()
    foodon["edges"] = edges

    logger.info("FoodOn: %d → %d nodes (food/organism filter).", before, len(nodes))


def _filter_chebi(
    sources: dict[str, dict[str, pd.DataFrame]],
    roots: OntologyRoots,
) -> None:
    chebi = sources.get("chebi")
    if chebi is None:
        return
    nodes: pd.DataFrame = chebi["nodes"]
    edges: pd.DataFrame = chebi["edges"]

    mol_entity = _compute_descendants(edges, str(roots.chebi_molecular_entity))
    before = len(nodes)
    nodes = nodes[nodes["native_id"].isin(mol_entity)].copy()
    chebi["nodes"] = nodes
    logger.info("ChEBI: %d → %d nodes (molecular entity filter).", before, len(nodes))


def _filter_ctd(sources: dict[str, dict[str, pd.DataFrame]]) -> None:
    ctd = sources.get("ctd")
    if ctd is None:
        return
    edges: pd.DataFrame = ctd["edges"]
    before = len(edges)
    chemdis = edges[edges["edge_type"] == "chemical_disease_association"]
    direct = chemdis[
        chemdis["raw_attrs"].apply(lambda x: bool(x.get("direct_evidence")))
    ]
    other_edges = edges[edges["edge_type"] != "chemical_disease_association"]
    ctd["edges"] = pd.concat([direct, other_edges], ignore_index=True)
    after = len(ctd["edges"])
    logger.info("CTD: %d → %d edges (direct evidence).", before, after)


def _filter_cdno(sources: dict[str, dict[str, pd.DataFrame]]) -> None:
    cdno = sources.get("cdno")
    if cdno is None:
        return
    nodes: pd.DataFrame = cdno["nodes"]
    xrefs = cdno.get("xrefs", pd.DataFrame())
    before = len(nodes)

    if xrefs.empty:
        cdno["nodes"] = nodes.iloc[0:0].copy()
    else:
        fdc_xrefs = xrefs[xrefs["target_source"] == "fdc_nutrient"]
        ids_with_fdc = set(fdc_xrefs["native_id"])
        cdno["nodes"] = nodes[nodes["native_id"].isin(ids_with_fdc)].copy()

    after = len(cdno["nodes"])
    logger.info("CDNO: %d → %d nodes (FDC nutrient).", before, after)
