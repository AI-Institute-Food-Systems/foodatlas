"""IERunner — expand KG with information-extraction metadata."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from ...stores.schema import DIR_DIAGNOSTICS
from ...utils.orphans import write_orphans_jsonl
from ...utils.timing import log_duration
from ..checkpoint import load_checkpoint, save_checkpoint
from ..knowledge_graph import KnowledgeGraph
from ..triplets.ambiguity import write_ambiguous_attestations
from .loader import load_ie_raw
from .report import write_unresolved_report
from .resolver import resolve_ie_metadata

if TYPE_CHECKING:
    from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)


class IERunner:
    """Orchestrate the IE stage: load, resolve, expand, save."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(self) -> None:
        """Load IE metadata, resolve entities, expand KG, and save."""
        kg_dir = Path(self._settings.kg_dir)
        with log_duration("Load checkpoint (triplets)", logger):
            load_checkpoint(kg_dir, "triplets")

        # Clear diagnostics from previous run.
        diag_dir = kg_dir / DIR_DIAGNOSTICS
        if diag_dir.exists():
            shutil.rmtree(diag_dir)

        with log_duration("Load KnowledgeGraph", logger):
            kg = KnowledgeGraph(self._settings)
        with log_duration("IE expansion", logger):
            self._expand(kg)
        with log_duration("Save KG", logger):
            kg.save()
        with log_duration("Write ambiguous attestations", logger):
            write_ambiguous_attestations(kg.attestations, kg_dir)
        with log_duration("Write orphan entities", logger):
            out = kg_dir / DIR_DIAGNOSTICS / "kgc_orphans.jsonl"
            count = write_orphans_jsonl(
                kg.entities._entities, kg.triplets._triplets, out
            )
            logger.info("Wrote %d orphan entities to %s", count, out)
        with log_duration("Save checkpoint (ie)", logger):
            save_checkpoint(kg_dir, "ie")
        logger.info("IE stage complete.")

    def _discover_ie_files(self) -> list[tuple[str, Path]]:
        """Scan ie_raw_dir for extraction files with model metadata.

        Each subdirectory should contain ``extraction_predicted.tsv``
        and optionally ``run_info.json`` with a ``model`` key.
        Returns a list of ``(method, path)`` pairs.
        """
        ie_raw_dir = self._settings.ie_raw_dir
        if not ie_raw_dir:
            return []
        raw_dir = Path(ie_raw_dir)
        if not raw_dir.is_dir():
            logger.warning("ie_raw_dir not found: %s", raw_dir)
            return []

        entries: list[tuple[str, Path]] = []
        for tsv in sorted(raw_dir.glob("*/extraction_predicted.json")):
            info_file = tsv.parent / "run_info.json"
            if info_file.exists():
                info: dict[str, str] = json.loads(info_file.read_text())
                method = info["model"]
            else:
                method = tsv.parent.name
                logger.warning(
                    "No run_info.json in %s — using dir name as method.",
                    tsv.parent,
                )
            entries.append((method, tsv))

        logger.info("Discovered %d IE extraction files in %s.", len(entries), raw_dir)
        return entries

    def _expand(self, kg: KnowledgeGraph) -> None:
        """Integrate IE-extracted metadata with lookup-only resolution."""
        kg_dir = Path(self._settings.kg_dir)
        entries = self._discover_ie_files()

        if not entries:
            logger.info("No IE files found — skipping expansion.")
            return

        for method, path in entries:
            logger.info("Processing IE file: %s (method=%s)", path, method)
            with log_duration(f"Load IE raw: {path.name}", logger):
                metadata = load_ie_raw(path, kg_dir, method=method)
            if metadata.empty:
                logger.info("IE loader produced no rows — skipping.")
                continue

            with log_duration("Resolve IE metadata", logger):
                result = resolve_ie_metadata(metadata, kg.entities)

            if not result.resolved.empty:
                with log_duration("Add IE triplets to KG", logger):
                    n_triplets = kg.add_triplets_from_resolved_ie(
                        result.resolved,
                    )
                result.stats["triplets_created"] = n_triplets

            with log_duration("Write unresolved report", logger):
                write_unresolved_report(
                    result.unresolved_food,
                    result.unresolved_chemical,
                    metadata,
                    kg_dir,
                )
