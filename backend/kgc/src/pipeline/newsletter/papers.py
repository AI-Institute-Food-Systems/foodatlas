"""Read paper title + DOI from the local BioC-PMC corpus (the I/O edge).

The "new in the literature" cards show each source paper's real title, but
titles aren't in the KG — they live in the cached BioC JSON at
``{bioc_dir}/PMC{pmcid}.xml``. A missing or title-less file degrades to
``PaperMeta(None, None)`` so a single absent paper never breaks the digest.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PaperMeta:
    title: str | None
    doi: str | None


def fetch_paper_meta(pmcids: list[str], bioc_dir: str | Path) -> dict[str, PaperMeta]:
    base = Path(bioc_dir)
    return {pmcid: _read_meta(base / f"PMC{pmcid}.xml") for pmcid in pmcids}


def _read_meta(path: Path) -> PaperMeta:
    if not path.is_file():
        return PaperMeta(title=None, doi=None)
    documents = json.loads(path.read_text(encoding="utf-8")).get("documents", [])
    for passage in documents[0].get("passages", []) if documents else []:
        infons = passage.get("infons", {})
        if str(infons.get("section_type", "")).upper() == "TITLE":
            return PaperMeta(
                title=(passage.get("text") or "").strip() or None,
                doi=infons.get("article-id_doi"),
            )
    return PaperMeta(title=None, doi=None)
