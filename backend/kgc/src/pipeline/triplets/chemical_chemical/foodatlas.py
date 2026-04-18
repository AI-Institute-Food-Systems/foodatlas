"""FoodAtlas-curated chemical ontology bridge triplets.

This module is the single home for hand-curated is_a edges between chemical
entities that no upstream source provides. Each bridge is declared in
``config/foodatlas_classifications.yaml`` so the code stays data-driven and
new bridges can be added without touching Python.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import yaml

from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_SOURCE = "foodatlas"
_REL_ID = "r2"

_CLASSIFICATIONS_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "foodatlas_classifications.yaml"
)


def _load_bridges(path: Path = _CLASSIFICATIONS_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        msg = f"foodatlas_classifications.yaml not found at {path}"
        raise FileNotFoundError(msg)
    with path.open() as f:
        raw = yaml.safe_load(f) or {}
    return list(raw.get("is_a", []))


def _resolve(
    lookups: dict[str, pd.DataFrame],
    source: str,
    native_id: str,
    kg: KnowledgeGraph,
) -> str | None:
    if source not in lookups:
        lookups[source] = explode_external_ids(kg.entities._entities, source)
    lookup = lookups[source]
    match = lookup[lookup["native_id"] == str(native_id)]
    if match.empty:
        return None
    return str(match.iloc[0]["foodatlas_id"])


def merge_chemical_ontology_foodatlas(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],  # noqa: ARG001
) -> None:
    """Emit hand-curated is_a triplets declared in the classifications YAML."""
    bridges = _load_bridges()
    if not bridges:
        return

    lookups: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for bridge in bridges:
        child_src = bridge["child"]["source"]
        child_nid = bridge["child"]["native_id"]
        parent_src = bridge["parent"]["source"]
        parent_nid = bridge["parent"]["native_id"]
        note = bridge.get("note", "")

        child_fa = _resolve(lookups, child_src, child_nid, kg)
        parent_fa = _resolve(lookups, parent_src, parent_nid, kg)
        if child_fa is None or parent_fa is None:
            logger.warning(
                "FoodAtlas bridge unresolved: %s:%s is_a %s:%s "
                "(child_fa=%s, parent_fa=%s)",
                child_src,
                child_nid,
                parent_src,
                parent_nid,
                child_fa,
                parent_fa,
            )
            continue
        if child_fa == parent_fa:
            continue

        # Natural is_a direction: head=child, tail=parent.
        rows.append(
            {
                "_head_id": child_fa,
                "_tail_id": parent_fa,
                "source_type": _SOURCE,
                "reference": json.dumps(
                    {
                        "rule": "curated_bridge",
                        "note": note,
                        "child": {
                            "source": child_src,
                            "native_id": str(child_nid),
                        },
                        "parent": {
                            "source": parent_src,
                            "native_id": str(parent_nid),
                        },
                    }
                ),
                "source": _SOURCE,
                "head_name_raw": f"{child_src}:{child_nid}",
                "tail_name_raw": f"{parent_src}:{parent_nid}",
            }
        )

    if not rows:
        logger.info("No FoodAtlas bridge triplets emitted.")
        return

    df = pd.DataFrame(rows)
    ev_result = kg.evidence.create(df[["source_type", "reference"]])
    df["evidence_id"] = ev_result.index
    attestations = kg.attestations.create(df)

    triplet_input = df[["_head_id", "_tail_id"]].copy()
    triplet_input.columns = pd.Index(["head_id", "tail_id"])
    triplet_input.index = attestations.index
    triplet_input["relationship_id"] = _REL_ID
    triplets = kg.triplets.create(triplet_input)

    logger.info("Created %d FoodAtlas bridge triplets.", len(triplets))
