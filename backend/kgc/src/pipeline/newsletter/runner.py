"""Newsletter runner — emit the weekly KG-diff statistics as newsletter.json.

Diffs the freshly-built KG against the latest ``data/PreviousFAKG`` snapshot,
computes the headline numbers + food highlights + leaderboards (statistics.py),
curates highlight chemical names with an LLM agent (curation.py), and writes the
presentation-ready stats payload (format.py) to ``<kg_dir>/newsletter.json``.
``scripts/render_newsletter.py`` turns that JSON + the HTML template into the
final digest.
"""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ...stores.schema import FILE_ENTITIES
from ...utils.snapshots import latest_snapshot
from .curation import clean_chemical_names, curate_chemicals
from .format import build_payload
from .papers import fetch_paper_meta
from .statistics import build_stats

if TYPE_CHECKING:
    from ...models.settings import KGCSettings, NewsletterStageConfig
    from .statistics import NewsletterStats, PaperRank

logger = logging.getLogger(__name__)


class NewsletterRunner:
    """Emit newsletter.json from the current KG vs the previous snapshot."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings
        self._cfg: NewsletterStageConfig = settings.pipeline.stages.newsletter

    def run(self) -> None:
        kg_dir = Path(self._settings.kg_dir)
        previous = latest_snapshot(self._settings.data_dir)
        if previous is None:
            msg = (
                f"No previous KG snapshot under {self._settings.data_dir}/PreviousFAKG"
                " — cannot build the newsletter diff."
            )
            raise FileNotFoundError(msg)

        logger.info("Newsletter: diffing %s against %s", kg_dir, previous)
        stats = build_stats(
            kg_dir,
            previous,
            top_n=self._cfg.top_n,
            candidate_n=self._cfg.candidate_pool,
            paper_count=self._cfg.paper_count,
        )

        if self._cfg.curate:
            stats = self._curate(stats, kg_dir)
        else:
            stats = replace(stats, highlights=stats.highlights[: self._cfg.top_n])

        stats = self._resolve_papers(stats, kg_dir)
        paper_meta = fetch_paper_meta(
            [p.pmcid for p in stats.papers], self._cfg.bioc_cache_dir
        )
        out_path = kg_dir / "newsletter.json"
        payload = build_payload(stats, _today(), paper_meta)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info(
            "Newsletter stats written to %s (%d new food-chemical associations).",
            out_path,
            stats.new_food_chemical,
        )

    def _curate(self, stats: NewsletterStats, kg_dir: Path) -> NewsletterStats:
        # Food names are literal foodatlas common_names (statistics._canonical);
        # only the chemical lists are LLM-cleaned. Curate the whole candidate
        # pool, drop foods that come out empty, rank by # real compounds, keep
        # top_n — so the displayed count always matches the chemical list.
        entities = pd.read_parquet(kg_dir / FILE_ENTITIES)
        logger.info("Curating highlight chemicals (%s)...", self._cfg.curate_model)
        curated = curate_chemicals(
            stats.highlights,
            entities,
            model=self._cfg.curate_model,
            max_chemicals=self._cfg.max_chemicals,
            max_workers=self._cfg.max_workers,
        )
        return replace(stats, highlights=_top_by_compounds(curated, self._cfg.top_n))

    def _resolve_papers(self, stats: NewsletterStats, kg_dir: Path) -> NewsletterStats:
        # Resolve each card's chemical ids to curated names (same cleanup as the
        # highlights so names match across sections), recount the distinct
        # surviving associations, drop cards that curate to nothing, and keep the
        # top paper_count by clean count.
        if not stats.papers:
            return stats
        entities = pd.read_parquet(kg_dir / FILE_ENTITIES)
        chem_ids = list({cid for p in stats.papers for _, cid in p.associations})
        if self._cfg.curate:
            logger.info(
                "Curating %d card chemicals (%s)...",
                len(chem_ids),
                self._cfg.curate_model,
            )
            names = clean_chemical_names(
                chem_ids,
                entities,
                model=self._cfg.curate_model,
                max_workers=self._cfg.max_workers,
            )
        else:
            raw = dict(
                zip(entities["foodatlas_id"], entities["common_name"], strict=False)
            )
            names = {cid: raw.get(cid) for cid in chem_ids}

        resolved = [self._resolve_one(p, names) for p in stats.papers]
        nonempty = [p for p in resolved if p.new_associations > 0]
        top = sorted(nonempty, key=lambda p: -p.new_associations)
        return replace(stats, papers=top[: self._cfg.paper_count])

    def _resolve_one(self, paper: PaperRank, names: dict) -> PaperRank:
        clean: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for food, chem_id in paper.associations:
            chem = names.get(chem_id)
            if not chem or (food, chem) in seen:
                continue
            seen.add((food, chem))
            clean.append((food, chem))
        return replace(
            paper,
            new_associations=len(clean),
            associations=clean[: self._cfg.paper_chips],
        )


def _top_by_compounds(highlights: list, top_n: int) -> list:
    """Drop empties, rank by # distinct real compounds, cap to top_n."""
    real = [h for h in highlights if h.new_chemicals]
    return sorted(real, key=lambda h: -len(h.new_chemicals))[:top_n]


def _today() -> str:
    return datetime.now(tz=UTC).strftime("%B %-d, %Y")


def _dedup_against(highlights: list, shown: list) -> list:
    """Drop highlights whose food already appears in ``shown`` (Angle A)."""
    shown_ids = {h.food_id for h in shown}
    return [h for h in highlights if h.food_id not in shown_ids]
