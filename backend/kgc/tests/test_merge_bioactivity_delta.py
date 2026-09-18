"""Contract tests for ``scripts/merge_bioactivity_delta.py`` on a synthetic 3-way."""

import importlib
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
m = importlib.import_module("merge_bioactivity_delta")

R = m.RESERVED_FROM


def _ent(i: int, etype: str = "chemical") -> dict:
    return {
        "foodatlas_id": f"e{i}",
        "entity_type": etype,
        "common_name": f"n{i}",
        "scientific_name": "",
        "synonyms": "[]",
        "external_ids": "{}",
        "attributes": "{}",
    }


def _att(aid: str, ev: str) -> dict:
    return {
        "attestation_id": aid,
        "evidence_id": ev,
        "source": "lit2kg",
        "head_name_raw": "h",
        "tail_name_raw": "t",
        "conc_value": float("nan"),
        "conc_unit": "",
        "conc_value_raw": "",
        "conc_unit_raw": "",
        "food_part": "",
        "food_processing": "",
        "filter_score": float("nan"),
        "validated": False,
        "validated_correct": True,
        "head_candidates": ["e1"],
        "tail_candidates": ["e2"],
    }


def _tri(h: str, r: str, t: str, ids: list[str]) -> dict:
    return {
        "head_id": h,
        "relationship_id": r,
        "tail_id": t,
        "source": "",
        "attestation_ids": json.dumps(ids),
    }


def _ev(eid: str) -> dict:
    return {"evidence_id": eid, "source_type": "pubmed", "reference": "{}"}


def _tables(entities, registry, triplets, attestations, ambiguous, evidence):
    return {
        "entities": pd.DataFrame(entities),
        "entity_registry": pd.DataFrame(
            registry, columns=["source", "native_id", "foodatlas_id"]
        ),
        "triplets": pd.DataFrame(triplets),
        "attestations": pd.DataFrame(attestations, columns=list(_att("x", "y"))),
        "attestations_ambiguous": pd.DataFrame(ambiguous, columns=list(_att("x", "y"))),
        "evidence": pd.DataFrame(evidence),
    }


@pytest.fixture
def three_way():
    """ancestor; base = +1 triplet/att/ev + appends; delta = +entity/r6 + appends."""
    ents = [_ent(1, "food"), _ent(2), _ent(3)]
    reg = [("s", "1", "e1"), ("s", "2", "e2"), ("s", "3", "e3")]
    shared = ("e1", "r1", "e2")
    anc = _tables(
        ents,
        reg,
        [_tri(*shared, ["at1"])],
        [_att("at1", "ev1")],
        [_att("am1", "ev1")],
        [_ev("ev1")],
    )
    base = _tables(
        ents,
        reg,
        [_tri(*shared, ["at1", "at2"]), _tri("e1", "r1", "e3", ["at3"])],
        [_att("at1", "ev1"), _att("at2", "ev1"), _att("at3", "ev2")],
        [_att("am1", "ev1"), _att("am2", "ev2")],
        [_ev("ev1"), _ev("ev2")],
    )
    delta = _tables(
        [*ents, _ent(R, "bioactivity")],
        [*reg, ("bio", "x", f"e{R}")],
        [_tri(*shared, ["at1", "bio1"]), _tri("e2", "r6", f"e{R}", ["bm1"])],
        [_att("at1", "ev1"), _att("bio1", "ev1")],
        [_att("am1", "ev1")],
        [_ev("ev1")],
    )
    return base, anc, delta


def test_merge_unions_both_sides(three_way, tmp_path):
    base, anc, delta = three_way
    out, unioned = m.merge(base, anc, delta)

    assert len(out["entities"]) == 4
    assert len(out["entity_registry"]) == 4
    assert len(out["triplets"]) == 3
    assert len(out["attestations"]) == 4
    assert len(out["attestations_ambiguous"]) == 2
    assert len(out["evidence"]) == 2
    assert unioned == 1

    tri = out["triplets"].set_index(["head_id", "relationship_id", "tail_id"])
    assert json.loads(tri.at[("e1", "r1", "e2"), "attestation_ids"]) == [
        "at1",
        "at2",
        "bio1",
    ]
    assert json.loads(tri.at[("e1", "r1", "e3"), "attestation_ids"]) == ["at3"]
    assert json.loads(tri.at[("e2", "r6", f"e{R}"), "attestation_ids"]) == ["bm1"]

    bio = pd.DataFrame({"bioactivity_metadata_id": ["bm1"]})
    bio.to_parquet(tmp_path / "attestations_bioactivity.parquet", index=False)
    m.verify(base, anc, delta, out, tmp_path)


def test_write_takes_relationships_from_delta(three_way, tmp_path):
    base, anc, delta = three_way
    base_dir, delta_dir, out_dir = tmp_path / "b", tmp_path / "d", tmp_path / "o"
    base_dir.mkdir()
    delta_dir.mkdir()
    rels = pd.DataFrame({"foodatlas_id": ["r1", "r6"], "name": ["contains", "m"]})
    for f in m.FROM_DELTA:
        rels.to_parquet(delta_dir / f"{f}.parquet", index=False)
    (base_dir / "CHANGELOG.md").write_text("base changelog")
    pd.DataFrame({"x": [1]}).to_parquet(base_dir / "trust_signals.parquet")

    out, _ = m.merge(base, anc, delta)
    m.write(out, base_dir, delta_dir, out_dir)

    assert pd.read_parquet(out_dir / "relationships.parquet").equals(rels)
    assert (out_dir / "CHANGELOG.md").read_text() == "base changelog"
    assert (out_dir / "trust_signals.parquet").exists()
    assert len(pd.read_parquet(out_dir / "triplets.parquet")) == 3


def test_verify_rejects_changed_shared_row(three_way, tmp_path):
    base, anc, delta = three_way
    delta["entities"].loc[0, "common_name"] = "renamed"
    out, _ = m.merge(base, anc, delta)
    with pytest.raises(SystemExit, match="entities: base and delta disagree"):
        m.verify(base, anc, delta, out, tmp_path)


def test_verify_rejects_reserved_id_range(three_way, tmp_path):
    base, anc, delta = three_way
    base["entities"] = pd.concat(
        [base["entities"], pd.DataFrame([_ent(R + 5)])], ignore_index=True
    )
    out, _ = m.merge(base, anc, delta)
    with pytest.raises(SystemExit, match="reaches the bioactivity range"):
        m.verify(base, anc, delta, out, tmp_path)


def test_verify_rejects_delta_id_below_range(three_way, tmp_path):
    base, anc, delta = three_way
    delta["entities"].loc[3, "foodatlas_id"] = "e99"
    out, _ = m.merge(base, anc, delta)
    with pytest.raises(SystemExit, match="below e"):
        m.verify(base, anc, delta, out, tmp_path)


def test_verify_rejects_dangling_attestation_ref(three_way, tmp_path):
    base, anc, delta = three_way
    out, _ = m.merge(base, anc, delta)
    # no attestations_bioactivity.parquet in tmp_path → "bm1" is unresolved
    with pytest.raises(SystemExit, match="dangling attestation ids"):
        m.verify(base, anc, delta, out, tmp_path)


def test_verify_rejects_dropped_attestation_ids(three_way, tmp_path):
    base, anc, delta = three_way
    base["triplets"].loc[0, "attestation_ids"] = json.dumps(["at2"])
    out, _ = m.merge(base, anc, delta)
    with pytest.raises(SystemExit, match="base dropped attestation ids"):
        m.verify(base, anc, delta, out, tmp_path)
