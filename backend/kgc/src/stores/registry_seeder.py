"""Seed an EntityRegistry from a previous KG entities TSV."""

from __future__ import annotations

import ast
import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

    from .entity_registry import EntityRegistry

logger = logging.getLogger(__name__)

RegistryPair = tuple[str, str, bool]
"""(registry_source, native_id, is_primary)."""


def _str_int(val: Any) -> str:
    return str(int(val))


def _mesh_to_ctd(val: Any) -> str:
    return f"MESH:{val}"


# (entity_type, external_ids_key) → (registry_source, transform, is_primary)
_SOURCE_MAP: dict[tuple[str, str], tuple[str, Any, bool]] = {
    ("food", "foodon"): ("foodon", str, True),
    ("food", "fdc"): ("fdc", _str_int, False),
    ("chemical", "chebi"): ("chebi", _str_int, True),
    ("chemical", "cdno"): ("cdno", str, False),
    ("chemical", "fdc_nutrient"): ("fdc_nutrient", _str_int, False),
    ("chemical", "dmd"): ("dmd", str, True),
    ("disease", "mesh"): ("ctd", _mesh_to_ctd, True),
}


def extract_registry_pairs(
    entity_type: str,
    external_ids: dict[str, list],
) -> list[RegistryPair]:
    """Extract ``(source, native_id, is_primary)`` tuples from one entity row.

    Only keys present in ``_SOURCE_MAP`` for the given *entity_type* are used.
    The first value per key becomes primary; the rest become aliases.
    """
    pairs: list[RegistryPair] = []
    for ext_key, values in external_ids.items():
        entry = _SOURCE_MAP.get((entity_type, ext_key))
        if entry is None:
            continue
        source, transform, is_primary = entry
        for i, val in enumerate(values):
            native_id = transform(val)
            pairs.append((source, native_id, is_primary and i == 0))
    return pairs


def seed_registry(registry: EntityRegistry, tsv_path: Path) -> int:
    """Populate *registry* from a previous-KG entities TSV.

    Reads the TSV row by row, extracts ``(source, native_id)`` pairs via
    :func:`extract_registry_pairs`, and registers them in the registry.

    Returns the number of mappings added.
    """
    df = pd.read_csv(tsv_path, sep="\t")
    added = 0
    skipped = 0

    for _, row in df.iterrows():
        fa_id = str(row["foodatlas_id"])
        entity_type = str(row["entity_type"])

        try:
            external_ids = ast.literal_eval(str(row["external_ids"]))
        except (ValueError, SyntaxError):
            skipped += 1
            continue

        if not isinstance(external_ids, dict) or not external_ids:
            continue

        if "_placeholder_to" in external_ids:
            # Placeholder entities point to real entities.  Register
            # their source IDs as aliases of the first target so the
            # pipeline enriches the target instead of creating a new entity.
            targets = external_ids["_placeholder_to"]
            if targets:
                target_id = str(targets[0])
                real_ext = {
                    k: v for k, v in external_ids.items() if k != "_placeholder_to"
                }
                for source, native_id, _ in extract_registry_pairs(
                    entity_type, real_ext
                ):
                    try:
                        registry.register_alias(source, native_id, target_id)
                        added += 1
                    except ValueError:
                        skipped += 1
            continue

        pairs = extract_registry_pairs(entity_type, external_ids)
        for source, native_id, is_primary in pairs:
            try:
                if is_primary:
                    registry.register(source, native_id, fa_id)
                else:
                    registry.register_alias(source, native_id, fa_id)
                added += 1
            except ValueError:
                skipped += 1

    # Ensure next_eid is above ALL old entity IDs, including those
    # without registerable external_ids (e.g. flavors, chemicals with
    # only unrecognised sources).  Without this, new entity IDs can
    # collide with old IDs that have no registry entry.
    max_old_eid = int(df["foodatlas_id"].str.slice(1).astype(int).max())
    registry._max_eid = max(registry._max_eid, max_old_eid)

    if skipped:
        logger.warning("Registry seeding: skipped %d entries.", skipped)
    logger.info(
        "Registry seeded: %d mappings from %s (next_eid=%d).",
        added,
        tsv_path.name,
        registry.next_eid,
    )
    return added
