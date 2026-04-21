"""Stage checkpoints — save/restore KG state between pipeline stages."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from ..stores.schema import (
    DIR_INTERMEDIATE,
    FILE_ATTESTATIONS,
    FILE_ATTESTATIONS_AMBIGUOUS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_REGISTRY,
    FILE_RELATIONSHIPS,
    FILE_TRIPLETS,
)

logger = logging.getLogger(__name__)

DIR_CHECKPOINTS = "checkpoints"

_CHECKPOINT_FILES = [
    FILE_ENTITIES,
    FILE_TRIPLETS,
    FILE_EVIDENCE,
    FILE_ATTESTATIONS,
    FILE_ATTESTATIONS_AMBIGUOUS,
    FILE_RELATIONSHIPS,
    FILE_REGISTRY,
    FILE_LUT_FOOD,
    FILE_LUT_CHEMICAL,
]


def save_checkpoint(kg_dir: str | Path, stage_name: str) -> None:
    """Copy KG data files to ``checkpoints/{stage_name}/``."""
    kg = Path(kg_dir)
    dest = kg / DIR_CHECKPOINTS / stage_name
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    (dest / DIR_INTERMEDIATE).mkdir(exist_ok=True)

    copied = 0
    for rel_path in _CHECKPOINT_FILES:
        src = kg / rel_path
        if src.exists():
            shutil.copy2(src, dest / rel_path)
            copied += 1
    logger.info("Checkpoint '%s': saved %d files to %s.", stage_name, copied, dest)


def load_checkpoint(kg_dir: str | Path, stage_name: str) -> bool:
    """Restore KG data files from ``checkpoints/{stage_name}/``.

    Returns True if checkpoint was loaded, False if not found.
    """
    kg = Path(kg_dir)
    src_dir = kg / DIR_CHECKPOINTS / stage_name
    if not src_dir.exists():
        logger.warning("No checkpoint '%s' found — skipping restore.", stage_name)
        return False

    (kg / DIR_INTERMEDIATE).mkdir(exist_ok=True)

    restored = 0
    for rel_path in _CHECKPOINT_FILES:
        src = src_dir / rel_path
        if src.exists():
            shutil.copy2(src, kg / rel_path)
            restored += 1
    logger.info(
        "Checkpoint '%s': restored %d files from %s.", stage_name, restored, src_dir
    )
    return True
