"""Sample the model's final KG triples, grouped by source sentence.

Each sampled item is one sentence plus every (food, chemical, concentration)
triple the model contributed to the KG for it. A fixed seed makes the sample
reproducible across runs.

Note: only sentences that yielded at least one resolved attestation appear in
the KG, so this samples from the model's *successful* extractions. Sentences the
model extracted nothing from are absent — recall is therefore conditional on a
sentence producing at least one KG triple.
"""

from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ...stores.schema import FILE_ATTESTATIONS, FILE_EVIDENCE

logger = logging.getLogger(__name__)

Triple = tuple[str, str, str]  # (food, chemical, concentration)


@dataclass(frozen=True)
class SampledSentence:
    pmcid: str
    sentence: str
    pairs: list[Triple]


def sample_sentences(
    kg_dir: str | Path,
    source_filter: list[str] | None,
    sample_size: int,
    seed: int,
) -> list[SampledSentence]:
    """Return a seeded sample of KG sentences with their (deduped) triples.

    Triples whose attestation source matches any prefix in ``source_filter``
    (None / [] = every source) are pooled per sentence, so the sample reflects
    what the KG asserts regardless of which model produced each triple.
    """
    kg = Path(kg_dir)
    attestations = pd.read_parquet(kg / FILE_ATTESTATIONS)
    evidence = pd.read_parquet(kg / FILE_EVIDENCE)

    rows = _filter_sources(attestations, source_filter)
    if rows.empty:
        logger.warning("No attestations match source_filter %r", source_filter)
        return []

    ev_ref = dict(zip(evidence["evidence_id"], evidence["reference"], strict=False))
    groups = _group_by_sentence(rows, ev_ref)

    keys = sorted(groups)
    chosen = random.Random(seed).sample(keys, min(sample_size, len(keys)))
    return [
        SampledSentence(pmcid=pmcid, sentence=text, pairs=groups[(pmcid, text)])
        for pmcid, text in chosen
    ]


def _filter_sources(
    attestations: pd.DataFrame,
    source_filter: list[str] | None,
) -> pd.DataFrame:
    if not source_filter:
        return attestations
    src = attestations["source"].astype(str)
    mask = pd.Series(data=False, index=attestations.index)
    for prefix in source_filter:
        mask |= src.str.startswith(prefix)
    return attestations[mask]


def _group_by_sentence(
    rows: pd.DataFrame,
    ev_ref: dict[str, str],
) -> dict[tuple[str, str], list[Triple]]:
    """Group triples by sentence, deduping (food, chemical) across sources."""
    groups: dict[tuple[str, str], list[Triple]] = {}
    seen: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for _, row in rows.iterrows():
        ref = ev_ref.get(row["evidence_id"])
        if not ref:
            continue
        meta = json.loads(ref)
        pmcid = str(meta.get("pmcid", ""))
        text = str(meta.get("text", ""))
        if not pmcid or not text:
            continue

        key = (pmcid, text)
        food = str(row["head_name_raw"])
        chemical = str(row["tail_name_raw"])
        dedup = (food.lower(), chemical.lower())
        if dedup in seen.setdefault(key, set()):
            continue
        seen[key].add(dedup)
        groups.setdefault(key, []).append((food, chemical, _conc(row)))
    return groups


def _conc(row: pd.Series) -> str:
    """Return the concentration the model extracted, verbatim (value + unit).

    Only the raw extracted text is used — the KG's normalized ``conc_value``
    (mg/100g) is a pipeline derivation, not part of the model's extraction, so
    it is deliberately excluded from what the judge and the audit see.
    """
    raw_value = str(row.get("conc_value_raw", "") or "")
    raw_unit = str(row.get("conc_unit_raw", "") or "")
    return f"{raw_value} {raw_unit}".strip()
