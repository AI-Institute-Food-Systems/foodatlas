"""Build chemical ontology triplets (is_a) from DMD molecule classifications.

DMD provides a ``Molecule Classification`` column that labels each molecule
as Peptide, Protein, Complex Lipid, Biogenic Amine, etc. ChEBI proper would
eventually classify these structurally, but most DMD peptides in particular
have no ChEBI xref and would otherwise sit unclassified. This merger emits
``is_a`` triplets with ``source=foodatlas`` from each DMD molecule to the
corresponding ChEBI class entity.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ..utils import explode_external_ids

if TYPE_CHECKING:
    from ...knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

_SOURCE = "foodatlas"
_REL_ID = "r2"

# DMD classification label → ChEBI native_id of the class entity that the
# DMD molecule should be declared an is_a of. Only classifications with a
# real ChEBI class are listed; adding a new row here is all it takes to
# start emitting triplets for that class.
_CLASSIFICATION_TO_CHEBI: dict[str, str] = {
    "Peptide": "16670",  # peptide (CHEBI:16670)
}


def merge_chemical_ontology_dmd(
    kg: KnowledgeGraph,
    sources: dict[str, dict[str, pd.DataFrame]],
) -> None:
    """Generate is_a triplets from DMD ``Molecule Classification`` labels."""
    dmd = sources.get("dmd")
    if dmd is None:
        return

    nodes = dmd.get("nodes", pd.DataFrame())
    if nodes.empty:
        logger.info("No DMD nodes.")
        return

    dmd_lookup = explode_external_ids(kg.entities._entities, "dmd")
    if dmd_lookup.empty:
        logger.info("No DMD entity mappings.")
        return

    chebi_lookup = explode_external_ids(kg.entities._entities, "chebi")
    if chebi_lookup.empty:
        logger.info("No ChEBI entity mappings.")
        return

    chebi_native_to_fa = dict(
        zip(chebi_lookup["native_id"], chebi_lookup["foodatlas_id"], strict=False)
    )

    rows: list[dict[str, object]] = []
    for label, chebi_native in _CLASSIFICATION_TO_CHEBI.items():
        parent_fa = chebi_native_to_fa.get(chebi_native)
        if parent_fa is None:
            msg = (
                f"ChEBI class {chebi_native} (for DMD classification {label!r}) "
                f"not found in entity store — ChEBI ingest may be broken."
            )
            raise ValueError(msg)

        matches = nodes[
            nodes["raw_attrs"].apply(
                lambda x, lbl=label: (
                    isinstance(x, dict)
                    and lbl in (x.get("molecule_classification") or [])
                )
            )
        ]
        if matches.empty:
            continue

        merged = matches.merge(
            dmd_lookup,
            left_on="native_id",
            right_on="native_id",
            how="inner",
        )
        if merged.empty:
            logger.info("DMD %s: no resolved entities.", label)
            continue

        # Drop child==parent self-loops just in case.
        merged = merged[merged["foodatlas_id"] != parent_fa]

        for _, row in merged.iterrows():
            # Natural is_a direction: head=child (DMD molecule), tail=parent.
            rows.append(
                {
                    "_head_id": str(row["foodatlas_id"]),
                    "_tail_id": parent_fa,
                    "source_type": _SOURCE,
                    "reference": json.dumps(
                        {
                            "rule": "dmd_molecule_classification",
                            "value": label,
                            "chebi_parent": chebi_native,
                        }
                    ),
                    "source": _SOURCE,
                    "head_name_raw": f"dmd:{row['native_id']}",
                    "tail_name_raw": f"chebi:{chebi_native}",
                }
            )

        logger.info("DMD classification %s: %d triplets queued.", label, len(merged))

    if not rows:
        logger.info("No DMD chemical-ontology triplets emitted.")
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

    logger.info("Created %d DMD chemical ontology triplets.", len(triplets))
