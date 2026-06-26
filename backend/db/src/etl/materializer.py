"""Materialize denormalized API tables from base tables."""

import json
import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .bulk_insert import bulk_copy, truncate_tables
from .materializer_composition import materialize_food_chemical_composition
from .materializer_correlation import materialize_chemical_disease_correlation

logger = logging.getLogger(__name__)
MV_TABLES = [
    "mv_food_entities",
    "mv_chemical_entities",
    "mv_disease_entities",
    "mv_food_chemical_composition",
    "mv_chemical_disease_correlation",
]


def refresh_all(conn: Connection) -> None:
    """Truncate and re-populate all materialized API tables."""
    truncate_tables(conn, MV_TABLES)
    logger.info("Building entity views...")
    _materialize_entity_views(conn)
    logger.info("Building food-chemical composition...")
    materialize_food_chemical_composition(conn)
    logger.info("Building chemical-disease correlation...")
    materialize_chemical_disease_correlation(conn)
    conn.commit()


def _materialize_entity_views(conn: Connection) -> None:
    """Compute mv_food_entities, mv_chemical_entities, mv_disease_entities."""
    entities = pd.read_sql(text("SELECT * FROM base_entities"), conn)
    triplets = pd.read_sql(text("SELECT * FROM base_triplets"), conn)
    attestations = pd.read_sql(
        text("SELECT head_candidates, tail_candidates FROM base_attestations"), conn
    )

    sibling_map = _build_sibling_map(attestations)
    name_map = entities.set_index("foodatlas_id")["common_name"].to_dict()

    r1 = triplets[triplets["relationship_id"] == "r1"]
    r3r4 = triplets[triplets["relationship_id"].isin(["r3", "r4"])]

    food_ids = set(r1["head_id"])
    foods = entities[
        (entities["entity_type"] == "food") & (entities["foodatlas_id"].isin(food_ids))
    ].copy()
    foods["food_classification"] = foods["attributes"].apply(
        lambda a: a.get("food_groups", []) if isinstance(a, dict) else []
    )
    foods["ambiguity_siblings"] = _render_siblings_col(
        foods["foodatlas_id"], sibling_map, name_map
    )
    _insert_mv_entities(
        conn, "mv_food_entities", foods, ["food_classification", "ambiguity_siblings"]
    )

    # Include chemicals from food composition (r1), disease correlations (r3/r4),
    # and their IS_A ancestors (so ancestor pages have metadata).
    r2 = triplets[triplets["relationship_id"] == "r2"]
    disease_chem_ids = set(r3r4["head_id"])
    ancestor_ids = _collect_ancestors(r2, disease_chem_ids, entities)
    chem_ids = set(r1["tail_id"]) | disease_chem_ids | ancestor_ids
    chemicals = entities[
        (entities["entity_type"] == "chemical")
        & (entities["foodatlas_id"].isin(chem_ids))
    ].copy()
    chemicals["chemical_classification"] = chemicals["attributes"].apply(
        lambda a: a.get("chemical_groups", []) if isinstance(a, dict) else []
    )
    chemicals["flavor_descriptors"] = chemicals["attributes"].apply(
        lambda a: a.get("flavor_descriptors", []) if isinstance(a, dict) else []
    )
    chemicals["ambiguity_siblings"] = _render_siblings_col(
        chemicals["foodatlas_id"], sibling_map, name_map
    )
    _insert_mv_entities(
        conn,
        "mv_chemical_entities",
        chemicals,
        ["chemical_classification", "flavor_descriptors", "ambiguity_siblings"],
    )

    relevant_disease_ids = set(r3r4["tail_id"])
    diseases = entities[
        (entities["entity_type"] == "disease")
        & (entities["foodatlas_id"].isin(relevant_disease_ids))
    ].copy()
    diseases["ambiguity_siblings"] = _render_siblings_col(
        diseases["foodatlas_id"], sibling_map, name_map
    )
    _insert_mv_entities(conn, "mv_disease_entities", diseases, ["ambiguity_siblings"])

    logger.info(
        "Entity views: %d foods, %d chemicals, %d diseases",
        len(foods),
        len(chemicals),
        len(diseases),
    )


def _collect_ancestors(
    r2: pd.DataFrame, seed_ids: set[str], entities: pd.DataFrame
) -> set[str]:
    """Return all chemical ancestors of seed_ids via IS_A (r2) triplets.

    All r2 triplets use natural direction: head=child, tail=parent.
    """
    chem_ids_all = set(entities[entities["entity_type"] == "chemical"]["foodatlas_id"])
    chem_r2 = r2[r2["head_id"].isin(chem_ids_all) & r2["tail_id"].isin(chem_ids_all)]
    parents_of: dict[str, set[str]] = {}
    for _, row in chem_r2.iterrows():
        parents_of.setdefault(row["head_id"], set()).add(row["tail_id"])

    ancestors: set[str] = set()
    for node in seed_ids:
        stack = list(parents_of.get(node, set()))
        while stack:
            parent = stack.pop()
            if parent not in ancestors:
                ancestors.add(parent)
                stack.extend(parents_of.get(parent, set()))
    return ancestors


def _insert_mv_entities(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    extra_cols: list[str],
) -> None:
    """Insert entity DataFrame into a materialized view table."""
    base_cols = [
        "foodatlas_id",
        "entity_type",
        "common_name",
        "scientific_name",
        "synonyms",
        "external_ids",
    ]
    bulk_copy(conn, table_name, df, base_cols + extra_cols)


def _build_sibling_map(attestations: pd.DataFrame) -> dict[str, set[str]]:
    """Build a unified siblings map from attestation candidates.

    Candidates appear on both head (r1 foods / r3r4 chemicals) and tail
    (r1 chemicals / r3r4 diseases) positions. The co-occurrence semantics
    are identical across positions: two entities that resolved from the
    same raw name are ambiguous. We merge both columns into one adjacency
    graph, then symmetrize via connected components so every entity in an
    entangled cluster sees every other. Entity-type invariants hold
    automatically because the LUT returns same-type candidates per raw
    name, so clusters never cross entity types.
    """
    combined = pd.concat(
        [attestations["head_candidates"], attestations["tail_candidates"]],
        ignore_index=True,
    )
    return _components_from_candidates(combined)


def _components_from_candidates(
    candidates_col: pd.Series,
) -> dict[str, set[str]]:
    """Build adjacency from pairwise co-occurrence, then expand to components."""
    adjacency: dict[str, set[str]] = defaultdict(set)
    for cands in candidates_col:
        if not hasattr(cands, "__len__") or len(cands) <= 1:
            continue
        ids = list(cands)
        for i in ids:
            adjacency[i].update(x for x in ids if x != i)

    siblings: dict[str, set[str]] = {}
    seen: set[str] = set()
    for start in adjacency:
        if start in seen:
            continue
        # BFS over the connected component
        component: set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in component:
                continue
            component.add(node)
            stack.extend(adjacency.get(node, set()))
        for node in component:
            siblings[node] = component - {node}
        seen |= component
    return siblings


def _render_siblings_col(
    foodatlas_ids: pd.Series,
    sibling_map: dict[str, set[str]],
    name_map: dict[str, str],
) -> list[str]:
    """Render each entity's sibling list as a JSON string ready for bulk_copy.

    Produces JSON strings because bulk_copy routes dicts/lists through a
    different serializer; pre-stringifying avoids ambiguity at write time.
    """
    out: list[str] = []
    for eid in foodatlas_ids:
        siblings = sibling_map.get(eid, set())
        payload = [
            {"foodatlas_id": sid, "common_name": name_map.get(sid, sid)}
            for sid in sorted(siblings)
        ]
        out.append(json.dumps(payload))
    return out
