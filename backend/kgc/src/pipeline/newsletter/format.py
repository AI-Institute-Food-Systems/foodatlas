"""Build the newsletter statistics payload (a JSON-ready dict).

The NEWSLETTER stage serializes this to ``newsletter.json``;
``scripts/render_newsletter.py`` ingests it into ``data/newsletter.html`` (a
Jinja2 template) to produce the HTML digest. Keeping the data (here) separate
from presentation (template) lets the look change without touching the pipeline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .papers import PaperMeta
    from .statistics import FoodHighlight, FoodRank, NewsletterStats, PaperRank

# /food/<foodatlas_id> redirects to the canonical name page (frontend middleware).
_FOOD_URL = "https://www.foodatlas.ai/food/{food_id}"
_DOI_URL = "https://doi.org/{doi}"
_PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/"


def build_payload(
    stats: NewsletterStats,
    date: str,
    paper_meta: dict[str, PaperMeta] | None = None,
) -> dict:
    """Presentation-ready statistics for the newsletter template."""
    cur, prev = stats.current, stats.previous
    meta = paper_meta or {}
    return {
        "date": date,
        "new_associations": cur.associations - prev.associations,
        "new_food_chemical": stats.new_food_chemical,
        "new_papers": stats.new_papers,
        "foods_touched": stats.foods_touched,
        "chemicals_touched": stats.chemicals_touched,
        "atlas": [
            _metric("Associations", cur.associations, prev.associations),
            _metric("Foods", cur.foods, prev.foods),
            _metric("Chemicals", cur.chemicals, prev.chemicals),
            _metric("Diseases", cur.diseases, prev.diseases),
            _metric("Publications", cur.publications, prev.publications),
        ],
        "highlights": [_highlight(h) for h in stats.highlights],
        "papers": [_paper(p, meta.get(p.pmcid)) for p in stats.papers],
        "most_characterized": [_rank(r) for r in stats.most_characterized],
        "health_linked": [_rank(r) for r in stats.health_linked],
    }


def _metric(label: str, current: int, previous: int) -> dict:
    return {"label": label, "value": current, "delta": current - previous}


def _highlight(h: FoodHighlight) -> dict:
    return {
        "name": h.food_name,
        "url": _FOOD_URL.format(food_id=h.food_id),
        "count": len(h.new_chemicals),
        "chemicals": h.new_chemicals,
    }


def _paper(p: PaperRank, meta: PaperMeta | None) -> dict:
    title = meta.title if meta and meta.title else f"PMC{p.pmcid}"
    url = (
        _DOI_URL.format(doi=meta.doi)
        if meta and meta.doi
        else _PMC_URL.format(pmcid=p.pmcid)
    )
    return {
        "title": title,
        "url": url,
        "count": p.new_associations,
        "associations": [f"{food} → {chem}" for food, chem in p.associations],
    }


def _rank(r: FoodRank) -> dict:
    return {
        "name": r.food_name,
        "url": _FOOD_URL.format(food_id=r.food_id),
        "value": r.value,
    }
