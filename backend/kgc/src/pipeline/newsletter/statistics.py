"""Newsletter statistics — headline KG numbers and food highlights.

Headline numbers reproduce the AWS DB materializer
(``backend/db/src/etl/materializer_search.py::_materialize_statistics``) so the
weekly newsletter matches the live "atlas at a glance" counts. Everything is
computed over KG parquet (current build + previous snapshot) and reported as a
diff.

Two food "angles" rank foods by their NEW CONTAINS associations:
  A — volume: foods with the most new associations.
  B — ratio:  foods most transformed (new / total), among well-evidenced foods.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from ...stores.schema import (
    FILE_ATTESTATIONS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_TRIPLETS,
)

_R1 = "r1"  # CONTAINS (food -> chemical)
_R2 = "r2"  # IS_A (child -> parent)
_R3R4 = ("r3", "r4")  # chemical -> disease correlation


@dataclass(frozen=True)
class Headline:
    """Atlas-at-a-glance counts for one KG version (materializer parity)."""

    foods: int
    chemicals: int
    diseases: int
    associations: int
    publications: int
    food_chemical: int  # r1 edges
    chemical_disease: int  # scoped r3/r4 edges
    is_a: int  # scoped reachable r2 edges


@dataclass(frozen=True)
class FoodHighlight:
    """A highlighted food (one synonym-merged group) + its new chemicals.

    ``food_id`` / ``food_name`` are the group's canonical entity — the literal
    foodatlas id + common_name used for display and the food-page link.
    """

    food_id: str
    food_name: str
    new_count: int
    total_count: int
    new_chemicals: list[str]
    new_chemical_ids: list[str]


@dataclass(frozen=True)
class FoodRank:
    """A cumulative-leaderboard entry: a (synonym-merged) food and its count."""

    food_id: str
    food_name: str
    value: int


@dataclass(frozen=True)
class PaperRank:
    """A source paper and the NEW food-chemical associations it contributed.

    ``associations`` carry (canonical food name, chemical id); the runner
    resolves each chemical id to a curated name (so cards match the highlights).
    ``new_associations`` is the full count. Title/DOI are read separately from
    the BioC corpus (statistics.py stays parquet-only).
    """

    pmcid: str
    new_associations: int
    associations: list[tuple[str, str]]


@dataclass(frozen=True)
class NewsletterStats:
    """Everything the newsletter renders: headline diff, highlights, leaders."""

    previous: Headline
    current: Headline
    new_food_chemical: int
    foods_touched: int
    chemicals_touched: int
    new_papers: int
    highlights: list[FoodHighlight]  # foods with the most new discoveries
    most_characterized: list[FoodRank]
    health_linked: list[FoodRank]
    papers: list[PaperRank] = field(default_factory=list)  # new in the literature


@dataclass(frozen=True)
class _KG:
    triplets: pd.DataFrame
    entities: pd.DataFrame
    evidence: pd.DataFrame
    attestations: pd.DataFrame


def build_stats(
    current_dir: str | Path,
    previous_dir: str | Path,
    top_n: int = 5,
    candidate_n: int | None = None,
    paper_count: int = 5,
) -> NewsletterStats:
    """Load both KG versions and assemble the full newsletter statistics.

    ``highlights`` is a ``candidate_n`` pool (larger than ``top_n``) so the
    caller can curate, drop empties, and still keep ``top_n`` real rows.
    """
    current = _load_kg(current_dir)
    previous = _load_kg(previous_dir)

    new_pairs = _new_contains_pairs(current.triplets, previous.triplets)
    highlights = _food_highlights(current, new_pairs, top_n, candidate_n)
    most_characterized, health_linked = _food_leaders(current, top_n)

    return NewsletterStats(
        previous=compute_headline(previous),
        current=compute_headline(current),
        new_food_chemical=len(new_pairs),
        foods_touched=len({h for h, _ in new_pairs}),
        chemicals_touched=len({t for _, t in new_pairs}),
        new_papers=_new_paper_count(current.evidence, previous.evidence),
        highlights=highlights,
        most_characterized=most_characterized,
        health_linked=health_linked,
        papers=_top_papers(current, new_pairs, paper_count),
    )


# ---------------------------------------------------------------------------
# Headline numbers — materializer parity
# ---------------------------------------------------------------------------
def compute_headline(kg: _KG) -> Headline:
    """The five atlas-at-a-glance counts for one KG version."""
    triplets = kg.triplets
    r1 = triplets[triplets["relationship_id"] == _R1]
    r2 = triplets[triplets["relationship_id"] == _R2]
    r3r4 = triplets[triplets["relationship_id"].isin(_R3R4)]

    food_ids = set(r1["head_id"])
    chem_ids = set(r1["tail_id"])
    scoped_r3r4 = r3r4[r3r4["head_id"].isin(chem_ids)]
    disease_ids = set(scoped_r3r4["tail_id"])

    types = _type_ids(kg.entities)
    is_a = (
        _count_scoped_r2(r2, food_ids, types.get("food", set()))
        + _count_scoped_r2(r2, chem_ids, types.get("chemical", set()))
        + _count_scoped_r2(r2, disease_ids, types.get("disease", set()))
    )
    return Headline(
        foods=len(food_ids),
        chemicals=len(chem_ids),
        diseases=len(disease_ids),
        associations=len(r1) + len(scoped_r3r4) + is_a,
        publications=_count_publications(kg, chem_ids),
        food_chemical=len(r1),
        chemical_disease=len(scoped_r3r4),
        is_a=is_a,
    )


def _count_scoped_r2(r2: pd.DataFrame, seed_ids: set[str], type_ids: set[str]) -> int:
    """IS_A edges reachable from seed entities up to the ontology root."""
    typed = r2[r2["head_id"].isin(type_ids) & r2["tail_id"].isin(type_ids)]
    parents_of: dict[str, set[str]] = {}
    for head, tail in zip(typed["head_id"], typed["tail_id"], strict=False):
        parents_of.setdefault(head, set()).add(tail)

    reachable = set(seed_ids)
    stack = list(seed_ids)
    while stack:
        for parent in parents_of.get(stack.pop(), set()):
            if parent not in reachable:
                reachable.add(parent)
                stack.append(parent)

    scoped = typed[typed["head_id"].isin(reachable) & typed["tail_id"].isin(reachable)]
    return len(scoped)


def _count_publications(kg: _KG, chem_ids: set[str]) -> int:
    """Distinct pubmed PMCIDs (food-chem) + scoped CTD PMIDs (chem-disease)."""
    evidence = kg.evidence
    pubmed = evidence[evidence["source_type"] == "pubmed"]
    pmcids = _distinct_ref_field(pubmed["reference"], "pmcid")

    triplets = kg.triplets
    scoped = triplets[
        triplets["relationship_id"].isin(_R3R4) & triplets["head_id"].isin(chem_ids)
    ]
    att_ids = _flatten_ids(scoped["attestation_ids"])
    att = kg.attestations[kg.attestations["attestation_id"].isin(att_ids)]
    ctd = evidence[
        evidence["evidence_id"].isin(set(att["evidence_id"]))
        & (evidence["source_type"] == "ctd")
    ]
    pmids = _distinct_ref_field(ctd["reference"], "pmid")
    return len(pmcids) + len(pmids)


# ---------------------------------------------------------------------------
# Food highlights — foods with the most new discoveries
# ---------------------------------------------------------------------------
def _food_highlights(
    current: _KG,
    new_pairs: set[tuple[str, str]],
    top_n: int,
    candidate_n: int | None = None,
) -> list[FoodHighlight]:
    # Return a larger candidate pool so the runner can drop foods that curate to
    # nothing (all generic/artifact) and still keep top_n with real chemicals.
    pool = max(candidate_n or top_n, top_n)
    r1 = current.triplets[current.triplets["relationship_id"] == _R1]
    group_of = _group_foods(current.entities, set(r1["head_id"]))
    name = _name_map(current.entities)

    total_chems: dict[str, set[str]] = {}
    members: dict[str, set[str]] = {}
    for head, tail in zip(r1["head_id"], r1["tail_id"], strict=False):
        group = group_of.get(head, head)
        total_chems.setdefault(group, set()).add(tail)
        members.setdefault(group, set()).add(head)

    new_chems: dict[str, set[str]] = {}
    for head, tail in new_pairs:
        group = group_of.get(head, head)
        new_chems.setdefault(group, set()).add(tail)
        members.setdefault(group, set()).add(head)

    def highlight(group: str) -> FoodHighlight:
        chem_ids = sorted(new_chems[group])
        food_id, food_name = _canonical(members.get(group, {group}), name)
        return FoodHighlight(
            food_id=food_id,
            food_name=food_name,
            new_count=len(chem_ids),
            total_count=len(total_chems.get(group, set())),
            new_chemicals=sorted({name.get(c, c) for c in chem_ids}),
            new_chemical_ids=chem_ids,
        )

    # Candidate pool by new-association volume; the runner re-ranks the curated
    # survivors by distinct real compounds.
    pool_groups = sorted(new_chems, key=lambda g: -len(new_chems[g]))[:pool]
    return [highlight(g) for g in pool_groups]


def _food_leaders(
    current: _KG,
    top_n: int,
) -> tuple[list[FoodRank], list[FoodRank]]:
    """Cumulative leaderboards: most-characterized foods + foods linked to health.

    Foods are synonym-merged (same grouping as the angles). "Characterized" =
    distinct chemicals; "linked to health" = distinct diseases reachable via the
    food's chemicals (``food -> chemical -> disease``, scoped r3/r4).
    """
    triplets = current.triplets
    r1 = triplets[triplets["relationship_id"] == _R1]
    group_of = _group_foods(current.entities, set(r1["head_id"]))
    name = _name_map(current.entities)

    chems_by_group: dict[str, set[str]] = {}
    members: dict[str, set[str]] = {}
    for head, tail in zip(r1["head_id"], r1["tail_id"], strict=False):
        group = group_of.get(head, head)
        chems_by_group.setdefault(group, set()).add(tail)
        members.setdefault(group, set()).add(head)

    chem_diseases = _chem_disease_map(triplets, set(r1["tail_id"]))
    diseases_by_group = {
        group: set().union(*(chem_diseases.get(c, set()) for c in chems))
        if chems
        else set()
        for group, chems in chems_by_group.items()
    }

    def rank(metric: dict[str, set[str]]) -> list[FoodRank]:
        ranks = []
        for group in sorted(metric, key=lambda g: -len(metric[g]))[:top_n]:
            food_id, food_name = _canonical(members.get(group, {group}), name)
            ranks.append(
                FoodRank(food_id=food_id, food_name=food_name, value=len(metric[group]))
            )
        return ranks

    return rank(chems_by_group), rank(diseases_by_group)


def _canonical(member_ids: set[str], name: dict[str, str]) -> tuple[str, str]:
    """Representative (id, common_name) for a merged group.

    Prefer a member whose name has no parenthetical qualifier (so "cowpea" beats
    "cowpea (raw)"), then the shortest name. Names are literal foodatlas
    common_names — never rewritten.
    """
    best = min(
        sorted(member_ids),
        key=lambda m: ("(" in name.get(m, m), len(name.get(m, m))),
    )
    return best, name.get(best, best)


def _chem_disease_map(
    triplets: pd.DataFrame, chem_ids: set[str]
) -> dict[str, set[str]]:
    """chemical -> set of diseases, from r3/r4 edges scoped to covered chemicals."""
    scoped = triplets[
        triplets["relationship_id"].isin(_R3R4) & triplets["head_id"].isin(chem_ids)
    ]
    out: dict[str, set[str]] = {}
    for head, tail in zip(scoped["head_id"], scoped["tail_id"], strict=False):
        out.setdefault(head, set()).add(tail)
    return out


# ---------------------------------------------------------------------------
# New in the literature — papers ranked by new associations contributed
# ---------------------------------------------------------------------------
def _top_papers(
    current: _KG,
    new_pairs: set[tuple[str, str]],
    paper_count: int,
) -> list[PaperRank]:
    """Rank source papers by how many NEW food-chemical associations they added.

    Ranking by novel pairs (current minus previous) rather than raw extraction
    volume keeps review/meta-analysis table-dumps from dominating the cards.
    ``associations`` carry (canonical food name, chemical id) for every new pair;
    the runner curates the chemical ids, recounts the distinct surviving
    associations, drops empty cards, and re-ranks. A ``paper_count`` x2 candidate
    pool leaves room to backfill papers that curate to nothing.
    """
    pairs_by_paper = _new_pairs_by_paper(current, new_pairs)
    r1 = current.triplets[current.triplets["relationship_id"] == _R1]
    food_name = _canonical_food_names(current.entities, set(r1["head_id"]))
    ranked = sorted(pairs_by_paper, key=lambda p: -len(pairs_by_paper[p]))
    return [
        PaperRank(
            pmcid=pmcid,
            new_associations=len(pairs_by_paper[pmcid]),
            associations=[
                (food_name.get(h, h), t) for h, t in sorted(pairs_by_paper[pmcid])
            ],
        )
        for pmcid in ranked[: paper_count * 2]
    ]


def _canonical_food_names(entities: pd.DataFrame, covered: set[str]) -> dict[str, str]:
    """food_id -> synonym-merged canonical common_name (matches highlight foods)."""
    group_of = _group_foods(entities, covered)
    name = _name_map(entities)
    members: dict[str, set[str]] = {}
    for fid, group in group_of.items():
        members.setdefault(group, set()).add(fid)
    return {fid: _canonical(members[group], name)[1] for fid, group in group_of.items()}


def _new_pairs_by_paper(
    current: _KG, new_pairs: set[tuple[str, str]]
) -> dict[str, set[tuple[str, str]]]:
    """Map each source PMCID to the set of new CONTAINS pairs it attests."""
    r1 = current.triplets[current.triplets["relationship_id"] == _R1]
    pair_of_att: dict[str, tuple[str, str]] = {}
    for head, tail, raw in zip(
        r1["head_id"], r1["tail_id"], r1["attestation_ids"], strict=False
    ):
        if (head, tail) not in new_pairs:
            continue
        for att_id in json.loads(raw) if isinstance(raw, str) else raw or []:
            pair_of_att[att_id] = (head, tail)

    att = current.attestations[current.attestations["attestation_id"].isin(pair_of_att)]
    pmcid_of = _evidence_pmcids(current.evidence)
    pairs_by_paper: dict[str, set[tuple[str, str]]] = {}
    for att_id, evidence_id in zip(
        att["attestation_id"], att["evidence_id"], strict=False
    ):
        pmcid = pmcid_of.get(evidence_id)
        if pmcid:
            pairs_by_paper.setdefault(pmcid, set()).add(pair_of_att[att_id])
    return pairs_by_paper


def _evidence_pmcids(evidence: pd.DataFrame) -> dict[str, str]:
    """evidence_id -> PMCID for pubmed evidence (reference JSON ``pmcid`` field)."""
    pubmed = evidence[evidence["source_type"] == "pubmed"]
    out: dict[str, str] = {}
    for evidence_id, ref in zip(pubmed["evidence_id"], pubmed["reference"], strict=False):
        if isinstance(ref, str):
            pmcid = json.loads(ref).get("pmcid")
            if pmcid:
                out[evidence_id] = str(pmcid)
    return out


# Xref tokens that contaminate the synonyms column — bare CURIEs / accessions
# like "chebi:12345" or "ncbitaxon_9031" (URIs/IRIs are caught separately).
_IDENTIFIER_TOKEN = re.compile(r"^[a-z][\w.]*[:_]\d+$")


def _is_identifier(token: str) -> bool:
    """True for xref/ontology tokens (not real names) that must not drive merges."""
    return (
        "://" in token
        or token.startswith("<")
        or "obolibrary" in token
        or bool(_IDENTIFIER_TOKEN.match(token))
    )


def _group_foods(entities: pd.DataFrame, covered: set[str]) -> dict[str, str]:
    """Union-find: merge covered foods that share >=1 real-name synonym."""
    e = entities if "foodatlas_id" in entities.columns else entities.reset_index()
    e = e[e["foodatlas_id"].isin(covered)]
    syn_col = e["synonyms"] if "synonyms" in e.columns else [None] * len(e)

    tokens: dict[str, set[str]] = {}
    for fid, cname, syn in zip(
        e["foodatlas_id"], e["common_name"], syn_col, strict=False
    ):
        toks = _parse_synonyms(syn)
        toks.add(str(cname).strip().lower())
        # Merge only on real names — ontology xrefs (taxonomy/ChEBI URIs, CURIEs)
        # leak into synonyms and wrongly link distinct foods (e.g. chicken egg +
        # chicken both carry the Gallus gallus taxon id).
        tokens[str(fid)] = {t for t in toks if t and not _is_identifier(t)}

    parent = {f: f for f in tokens}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    first_with: dict[str, str] = {}
    for fid, toks in tokens.items():
        for tok in toks:
            other = first_with.setdefault(tok, fid)
            a, b = find(other), find(fid)
            if a != b:
                parent[a] = b
    return {f: find(f) for f in tokens}


def _parse_synonyms(value: object) -> set[str]:
    """Synonyms are stored as a JSON-encoded list string; parse to lower tokens."""
    if value is None:
        return set()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return {value.strip().lower()} if value.strip() else set()
        if isinstance(parsed, list):
            return {str(s).strip().lower() for s in parsed if str(s).strip()}
        return set()
    try:
        return {str(s).strip().lower() for s in value if str(s).strip()}
    except TypeError:
        return set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_kg(kg_dir: str | Path) -> _KG:
    d = Path(kg_dir)
    return _KG(
        triplets=pd.read_parquet(d / FILE_TRIPLETS),
        entities=pd.read_parquet(d / FILE_ENTITIES),
        evidence=pd.read_parquet(d / FILE_EVIDENCE),
        attestations=pd.read_parquet(d / FILE_ATTESTATIONS),
    )


def _new_contains_pairs(
    current: pd.DataFrame, previous: pd.DataFrame
) -> set[tuple[str, str]]:
    cur = current[current["relationship_id"] == _R1]
    prev = previous[previous["relationship_id"] == _R1]
    cur_pairs = set(zip(cur["head_id"], cur["tail_id"], strict=False))
    prev_pairs = set(zip(prev["head_id"], prev["tail_id"], strict=False))
    return cur_pairs - prev_pairs


def _new_paper_count(current_ev: pd.DataFrame, previous_ev: pd.DataFrame) -> int:
    cur = _distinct_ref_field(
        current_ev[current_ev["source_type"] == "pubmed"]["reference"], "pmcid"
    )
    prev = _distinct_ref_field(
        previous_ev[previous_ev["source_type"] == "pubmed"]["reference"], "pmcid"
    )
    return len(cur - prev)


def _type_ids(entities: pd.DataFrame) -> dict[str, set[str]]:
    e = entities if "foodatlas_id" in entities.columns else entities.reset_index()
    return e.groupby("entity_type")["foodatlas_id"].apply(set).to_dict()


def _name_map(entities: pd.DataFrame) -> dict[str, str]:
    e = entities if "foodatlas_id" in entities.columns else entities.reset_index()
    return dict(zip(e["foodatlas_id"], e["common_name"], strict=False))


def _distinct_ref_field(references: pd.Series, ref_field: str) -> set[str]:
    out: set[str] = set()
    for ref in references:
        if not isinstance(ref, str):
            continue
        value = json.loads(ref).get(ref_field)
        if value:
            out.add(str(value))
    return out


def _flatten_ids(attestation_ids: pd.Series) -> set[str]:
    out: set[str] = set()
    for raw in attestation_ids:
        ids = (
            json.loads(raw)
            if isinstance(raw, str)
            else (raw if raw is not None else [])
        )
        out.update(ids)
    return out
