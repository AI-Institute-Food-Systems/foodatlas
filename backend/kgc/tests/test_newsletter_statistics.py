"""Tests for newsletter statistics (materializer-parity headline + angles)."""

import pandas as pd
from src.pipeline.newsletter.statistics import (
    _KG,
    _canonical,
    _count_scoped_r2,
    _food_highlights,
    _food_leaders,
    _group_foods,
    _is_identifier,
    _new_contains_pairs,
    _parse_synonyms,
    build_stats,
    compute_headline,
)

_TRIPLET_COLS = ["head_id", "relationship_id", "tail_id", "attestation_ids"]
_ENTITY_COLS = ["foodatlas_id", "entity_type", "common_name", "synonyms"]
_EVIDENCE_COLS = ["evidence_id", "source_type", "reference"]
_ATTESTATION_COLS = ["attestation_id", "evidence_id"]


def _write_kg(path, *, triplets, entities, evidence, attestations) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(triplets, columns=_TRIPLET_COLS).to_parquet(path / "triplets.parquet")
    pd.DataFrame(entities, columns=_ENTITY_COLS).to_parquet(path / "entities.parquet")
    pd.DataFrame(evidence, columns=_EVIDENCE_COLS).to_parquet(path / "evidence.parquet")
    pd.DataFrame(attestations, columns=_ATTESTATION_COLS).to_parquet(
        path / "attestations.parquet"
    )


def _kg() -> _KG:
    triplets = pd.DataFrame(
        [
            ("f1", "r1", "c1", "[]"),
            ("f1", "r1", "c2", "[]"),
            ("f2", "r1", "c1", "[]"),
            ("c1", "r3", "d1", "[]"),  # scoped: c1 is covered by r1
            ("cX", "r3", "dY", "[]"),  # not scoped: cX absent from r1
            ("f1", "r2", "froot", "[]"),  # IS_A (food)
            ("f2", "r2", "froot", "[]"),
            ("c1", "r2", "croot", "[]"),  # IS_A (chemical)
        ],
        columns=_TRIPLET_COLS,
    )
    entities = pd.DataFrame(
        [
            ("f1", "food", "apple", []),
            ("f2", "food", "pear", []),
            ("froot", "food", "food", []),
            ("c1", "chemical", "vitamin c", []),
            ("c2", "chemical", "quercetin", []),
            ("cX", "chemical", "noise", []),
            ("croot", "chemical", "chemical", []),
            ("d1", "disease", "scurvy", []),
        ],
        columns=_ENTITY_COLS,
    )
    evidence = pd.DataFrame(
        [("e1", "pubmed", '{"pmcid": "PMC1"}')],
        columns=["evidence_id", "source_type", "reference"],
    )
    attestations = pd.DataFrame(
        [("a1", "e1")], columns=["attestation_id", "evidence_id"]
    )
    return _KG(triplets, entities, evidence, attestations)


def test_headline_matches_materializer_definitions() -> None:
    h = compute_headline(_kg())
    assert h.foods == 2  # f1, f2 (covered by r1)
    assert h.chemicals == 2  # c1, c2
    assert h.diseases == 1  # d1 (cX->dY excluded: cX not covered)
    assert h.food_chemical == 3  # r1 edges
    assert h.chemical_disease == 1  # scoped r3 (c1->d1 only)
    assert h.is_a == 3  # f1->froot, f2->froot, c1->croot
    assert h.associations == 3 + 1 + 3
    assert h.publications == 1  # one pubmed pmcid, no scoped ctd


def test_count_scoped_r2_follows_reachability() -> None:
    r2 = pd.DataFrame(
        [("a", "r2", "b", "[]"), ("b", "r2", "c", "[]"), ("x", "r2", "y", "[]")],
        columns=_TRIPLET_COLS,
    )
    # seed {a}; only a/b/c are in-type → reachable {a,b,c}; x->y excluded.
    assert _count_scoped_r2(r2, {"a"}, {"a", "b", "c"}) == 2


def test_new_contains_pairs() -> None:
    cur = _kg().triplets
    prev = pd.DataFrame([("f1", "r1", "c1", "[]")], columns=_TRIPLET_COLS)
    assert _new_contains_pairs(cur, prev) == {("f1", "c2"), ("f2", "c1")}


def test_food_highlights_volume_and_names() -> None:
    kg = _kg()
    new_pairs = {("f1", "c2"), ("f2", "c1")}
    highlights = _food_highlights(kg, new_pairs, top_n=5)
    by_food = {h.food_name: h for h in highlights}
    assert by_food["apple"].new_count == 1
    assert by_food["apple"].total_count == 2  # f1 -> c1, c2
    assert by_food["apple"].new_chemicals == ["quercetin"]  # c2's name
    assert by_food["pear"].new_chemicals == ["vitamin c"]  # c1's name


def test_food_dedup_merges_shared_synonym_variants() -> None:
    triplets = pd.DataFrame(
        [
            ("m1", "r1", "c1", "[]"),  # melon -> vitamin c
            ("m2", "r1", "c1", "[]"),  # melon (raw) -> vitamin c (same fact)
            ("m2", "r1", "c2", "[]"),  # melon (raw) -> quercetin
        ],
        columns=_TRIPLET_COLS,
    )
    entities = pd.DataFrame(
        [
            ("m1", "food", "melon", '["melon", "cantaloupe"]'),
            ("m2", "food", "melon (raw)", '["melon, raw", "melon"]'),  # shares "melon"
            ("c1", "chemical", "vitamin c", "[]"),
            ("c2", "chemical", "quercetin", "[]"),
        ],
        columns=_ENTITY_COLS,
    )
    kg = _KG(
        triplets,
        entities,
        pd.DataFrame(columns=["evidence_id", "source_type", "reference"]),
        pd.DataFrame(columns=["attestation_id", "evidence_id"]),
    )

    highlights = _food_highlights(kg, {("m2", "c2")}, top_n=5)

    assert len(highlights) == 1  # m1 and m2 merged into one food
    h = highlights[0]
    assert h.food_id == "m1"  # canonical: "melon" beats "melon (raw)"
    assert h.food_name == "melon"
    assert h.total_count == 2  # union: vitamin c + quercetin
    assert h.new_count == 1
    assert h.new_chemicals == ["quercetin"]


def test_canonical_prefers_unqualified_then_shortest() -> None:
    name = {"a": "cowpea (raw)", "b": "cowpea", "c": "black-eyed pea"}
    # "cowpea" and "black-eyed pea" have no qualifier; shortest wins.
    assert _canonical({"a", "b", "c"}, name) == ("b", "cowpea")
    # Only qualified names available -> shortest qualified.
    assert _canonical({"a"}, name) == ("a", "cowpea (raw)")


def test_parse_synonyms_json_list_lowercased() -> None:
    assert _parse_synonyms('["A", "B c"]') == {"a", "b c"}


def test_parse_synonyms_python_list_and_none() -> None:
    assert _parse_synonyms(["X", "y"]) == {"x", "y"}
    assert _parse_synonyms(None) == set()


def test_parse_synonyms_non_json_string() -> None:
    assert _parse_synonyms("plain name") == {"plain name"}


def test_is_identifier_flags_xrefs_not_names() -> None:
    assert _is_identifier("<http://purl.obolibrary.org/obo/ncbitaxon_9031>")
    assert _is_identifier("ncbitaxon_9031")
    assert _is_identifier("chebi:12345")
    assert not _is_identifier("chicken egg")
    assert not _is_identifier("vitamin c")
    assert not _is_identifier("omega-3")
    assert not _is_identifier("e300")


def test_group_foods_does_not_merge_on_shared_xref() -> None:
    # chicken egg + chicken share only the Gallus gallus taxon id -> must stay
    # separate (the bug). They share no real-name synonym.
    entities = pd.DataFrame(
        [
            (
                "a",
                "food",
                "chicken egg",
                '["egg", "<http://purl.obolibrary.org/obo/ncbitaxon_9031>"]',
            ),
            ("b", "food", "chicken", '["chicken", "ncbitaxon_9031"]'),
        ],
        columns=_ENTITY_COLS,
    )
    groups = _group_foods(entities, {"a", "b"})
    assert groups["a"] != groups["b"]


def test_group_foods_merges_shared_synonym() -> None:
    entities = pd.DataFrame(
        [
            ("a", "food", "melon", '["melon"]'),
            ("b", "food", "melon (raw)", '["melon"]'),  # shares "melon"
            ("c", "food", "apple", '["apple"]'),
        ],
        columns=_ENTITY_COLS,
    )
    groups = _group_foods(entities, {"a", "b", "c"})
    assert groups["a"] == groups["b"]  # merged
    assert groups["c"] != groups["a"]  # separate


def test_food_leaders_most_characterized_and_health() -> None:
    triplets = pd.DataFrame(
        [
            ("f1", "r1", "c1", "[]"),
            ("f1", "r1", "c2", "[]"),
            ("f1", "r1", "c3", "[]"),  # apple: 3 chemicals
            ("f2", "r1", "c1", "[]"),  # pear: 1 chemical
            ("c1", "r3", "d1", "[]"),
            ("c2", "r4", "d2", "[]"),  # c3 -> no disease
        ],
        columns=_TRIPLET_COLS,
    )
    entities = pd.DataFrame(
        [
            ("f1", "food", "apple", "[]"),
            ("f2", "food", "pear", "[]"),
            ("c1", "chemical", "vit c", "[]"),
            ("c2", "chemical", "quercetin", "[]"),
            ("c3", "chemical", "lutein", "[]"),
            ("d1", "disease", "scurvy", "[]"),
            ("d2", "disease", "other", "[]"),
        ],
        columns=_ENTITY_COLS,
    )
    kg = _KG(
        triplets,
        entities,
        pd.DataFrame(columns=_EVIDENCE_COLS),
        pd.DataFrame(columns=_ATTESTATION_COLS),
    )

    characterized, health = _food_leaders(kg, top_n=5)
    assert characterized[0].food_name == "apple"
    assert characterized[0].value == 3  # c1, c2, c3
    assert health[0].food_name == "apple"
    assert health[0].value == 2  # d1 via c1, d2 via c2


def test_build_stats_integration(tmp_path) -> None:
    current = tmp_path / "kg"
    previous = tmp_path / "prev"
    _write_kg(
        current,
        triplets=[
            ("f1", "r1", "c1", "[]"),
            ("f1", "r1", "c2", "[]"),
            ("f2", "r1", "c1", "[]"),
            ("c1", "r3", "d1", '["a1"]'),  # scoped chem-disease, ctd-attested
        ],
        entities=[
            ("f1", "food", "apple", "[]"),
            ("f2", "food", "pear", "[]"),
            ("c1", "chemical", "vitamin c", "[]"),
            ("c2", "chemical", "quercetin", "[]"),
            ("d1", "disease", "scurvy", "[]"),
        ],
        evidence=[
            ("e1", "pubmed", '{"pmcid": "PMC1"}'),
            ("e2", "pubmed", '{"pmcid": "PMC2"}'),
            ("e3", "ctd", '{"pmid": "111"}'),
        ],
        attestations=[("a1", "e3")],
    )
    _write_kg(
        previous,
        triplets=[("f1", "r1", "c1", "[]")],
        entities=[
            ("f1", "food", "apple", "[]"),
            ("c1", "chemical", "vitamin c", "[]"),
        ],
        evidence=[("e1", "pubmed", '{"pmcid": "PMC1"}')],
        attestations=[],
    )
    stats = build_stats(current, previous, top_n=5)

    assert stats.current.associations == 4  # r1(3) + scoped r3(1) + is_a(0)
    assert stats.previous.associations == 1
    assert stats.current.publications == 3  # PMC1, PMC2 + ctd pmid 111
    assert stats.previous.publications == 1
    assert stats.new_food_chemical == 2  # (f1,c2), (f2,c1)
    assert stats.new_papers == 1  # PMC2 is new
    assert stats.most_characterized[0].food_name == "apple"  # 2 chemicals


def test_food_highlights_candidate_pool_returns_more_than_top_n() -> None:
    triplets = pd.DataFrame(
        [
            ("f1", "r1", "c1", "[]"),
            ("f2", "r1", "c2", "[]"),
            ("f3", "r1", "c3", "[]"),
            ("f4", "r1", "c4", "[]"),
        ],
        columns=_TRIPLET_COLS,
    )
    entities = pd.DataFrame(
        [
            ("f1", "food", "aaa", "[]"),
            ("f2", "food", "bbb", "[]"),
            ("f3", "food", "ccc", "[]"),
            ("f4", "food", "ddd", "[]"),
            ("c1", "chemical", "x1", "[]"),
            ("c2", "chemical", "x2", "[]"),
            ("c3", "chemical", "x3", "[]"),
            ("c4", "chemical", "x4", "[]"),
        ],
        columns=_ENTITY_COLS,
    )
    kg = _KG(
        triplets,
        entities,
        pd.DataFrame(columns=_EVIDENCE_COLS),
        pd.DataFrame(columns=_ATTESTATION_COLS),
    )
    new_pairs = {("f1", "c1"), ("f2", "c2"), ("f3", "c3"), ("f4", "c4")}

    a_default = _food_highlights(kg, new_pairs, top_n=2)
    a_pool = _food_highlights(kg, new_pairs, top_n=2, candidate_n=4)
    assert len(a_default) == 2  # candidate_n defaults to top_n
    assert len(a_pool) == 4  # larger pool returned for later trimming
