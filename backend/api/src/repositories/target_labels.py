"""Readable names for the assay target genes on the bioactivity-disease views.

Both views store target genes as prefixed ids (``NCBIGene: 4780``,
``UniProt: Q16236``) because that is what the bridge records. ``NRF2`` means
something to a reader; ``NCBIGene: 4780`` does not, so the API pairs each id
with the label ``mv_assay_target_labels`` holds for it.

Done as a second query rather than a join so the ids and labels can be zipped
in Python and cannot drift out of alignment, and so a missing or empty label
table costs a nicety instead of the rows themselves — the same best-effort
posture ``bioactivity.get_measurements`` takes with ``base_bioassays``.
"""

import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Rows carry up to 50 genes each (the materializer's GENE_CAP) and the whole
# label table is only ~6k rows, so one primary-key lookup covers any page.
_SQL = "SELECT gene_id, label FROM mv_assay_target_labels WHERE gene_id = ANY(:ids)"


async def attach_targets(
    session: AsyncSession,
    rows: list[dict],
    field: str = "target_genes",
) -> None:
    """Add a ``targets`` list of ``{id, label}`` to each row, in place.

    ``label`` is ``None`` for a gene the table doesn't cover; callers render
    the id instead. Rows keep their original ``field`` untouched.
    """
    ids = sorted({gene for row in rows for gene in row.get(field) or []})
    labels = await _lookup(session, ids) if ids else {}
    for row in rows:
        row["targets"] = _dedupe(row.get(field) or [], labels)


def _dedupe(genes: list[str], labels: dict[str, str]) -> list[dict]:
    """One entry per distinct protein, in the order the ids arrived.

    A protein usually reaches us twice — once by Entrez id and once by UniProt
    accession (p53 as both ``NCBIGene: 7157`` and ``UniProt: P04637``). They
    label identically, so showing both spends two slots on one target. Genes
    with no label can only be compared by id.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for gene in genes:
        label = labels.get(gene)
        key = label.casefold() if label else gene
        if key in seen:
            continue
        seen.add(key)
        out.append({"id": gene, "label": label})
    return out


async def _lookup(session: AsyncSession, ids: list[str]) -> dict[str, str]:
    """Gene id → label, empty when the lookup table is absent or unreadable."""
    try:
        result = await session.execute(text(_SQL), {"ids": ids})
    except SQLAlchemyError as exc:
        logger.warning("target label lookup failed, falling back to ids: %s", exc)
        return {}
    return {row._mapping["gene_id"]: row._mapping["label"] for row in result}
