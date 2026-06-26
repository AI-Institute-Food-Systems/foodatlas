"""Load a paper's BioC text by PMCID and expose it for agentic inspection.

Reads the local BioC-PMC cache (``{cache_dir}/PMC{pmcid}.xml`` — BioC JSON
despite the extension) and falls back to the NCBI BioC OA REST API. The agent
queries the paper through :meth:`Paper.search` / :meth:`Paper.section` rather
than receiving the full text up front.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_BIOC_URL = (
    "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi"
    "/BioC_json/PMC{pmcid}/unicode"
)
_SNIPPET_CHARS = 700
_MAX_HITS = 8


@dataclass(frozen=True)
class Passage:
    text: str
    section: str


class Paper:
    """A parsed BioC paper, searchable by keyword or section."""

    def __init__(self, pmcid: str, passages: list[Passage]) -> None:
        self.pmcid = pmcid
        self._passages = passages

    @property
    def title(self) -> str:
        for p in self._passages:
            if p.section == "TITLE" and p.text:
                return p.text
        return self._passages[0].text if self._passages else ""

    def sections(self) -> list[str]:
        """Distinct section labels in document order."""
        seen: list[str] = []
        for p in self._passages:
            if p.section and p.section not in seen:
                seen.append(p.section)
        return seen

    def search(self, query: str, max_hits: int = _MAX_HITS) -> str:
        """Return passages containing *query* (case-insensitive), as snippets."""
        q = query.strip().lower()
        if not q:
            return "(empty query)"
        hits = [p for p in self._passages if q in p.text.lower()]
        if not hits:
            return f"No passages contain {query!r}."
        out = [f"{len(hits)} passage(s) match {query!r} (showing up to {max_hits}):"]
        for p in hits[:max_hits]:
            out.append(f"[{p.section}] {_clip(p.text, _SNIPPET_CHARS)}")
        return "\n\n".join(out)

    def section(self, name: str) -> str:
        """Return the text of passages whose section label matches *name*."""
        key = name.strip().upper()
        hits = [p.text for p in self._passages if p.section.upper() == key]
        if not hits:
            available = ", ".join(self.sections()) or "(none)"
            return f"No section {name!r}. Available: {available}."
        return _clip("\n\n".join(hits), _SNIPPET_CHARS * 6)


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + " …[truncated]"


def load_paper(pmcid: str, cache_dir: str, *, fetch: bool = True) -> Paper | None:
    """Return a :class:`Paper` for *pmcid*, or None if unavailable."""
    raw = _read_cache(pmcid, cache_dir)
    if raw is None and fetch:
        raw = _fetch(pmcid)
    if raw is None:
        logger.warning("No paper text for PMCID %s", pmcid)
        return None
    return Paper(pmcid, _parse_bioc(raw))


def _read_cache(pmcid: str, cache_dir: str) -> dict | None:
    path = Path(cache_dir) / f"PMC{pmcid}.xml"
    if not path.exists():
        return None
    data: dict = json.loads(path.read_text(encoding="utf-8"))
    return data


def _fetch(pmcid: str) -> dict | None:
    url = _BIOC_URL.format(pmcid=pmcid)
    try:
        # url scheme is fixed by _BIOC_URL constant (https://NCBI); not user-input
        with urllib.request.urlopen(url, timeout=30) as resp:  # nosec B310
            data: dict = json.loads(resp.read().decode("utf-8"))
            return data
    except Exception:
        logger.exception("BioC fetch failed for PMCID %s", pmcid)
        return None


def _parse_bioc(raw: dict) -> list[Passage]:
    documents = raw.get("documents", [])
    passages: list[Passage] = []
    for doc in documents:
        for p in doc.get("passages", []):
            text = (p.get("text") or "").strip()
            if not text:
                continue
            section = str(p.get("infons", {}).get("section_type", "")) or "BODY"
            passages.append(Passage(text=text, section=section))
    return passages
