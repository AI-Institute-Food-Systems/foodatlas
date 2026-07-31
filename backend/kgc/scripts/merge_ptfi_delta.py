"""Merge the PTFI delta into the built KG parquet (dedup foods on FoodOn IRI).

PTFI ships as a pre-resolved delta with hardcoded ids (``e227381+``) that collide
with the bioactivity concepts the build minted in the same range, and its foods
duplicate existing FoodAtlas foods. This script reconciles both:

* **Dedup** — a PTFI food whose FoodOn IRI already maps to an existing FoodAtlas
  food *is* that food: its rows are re-pointed onto the existing id and the
  duplicate entity is dropped (257 of 300).
* **Re-base** — every id PTFI actually mints (43 new foods + 1,022 chemicals) is
  reassigned above the post-build max ``M`` (``e227402…``), clearing the concept
  range. References to pre-existing entities (``≤ e227380``) are left untouched.
* **Collapse** — PTFI emits one ``contains`` row per sample measurement, so a
  (food, chemical) pair recurs; the KG keeps one triplet per pair with all
  attestations, so pairs are collapsed (attestation ids unioned) and pairs that
  already exist in the KG have their attestations merged in, not duplicated.

Idempotent: the pristine post-build parquet is snapshotted to ``outputs/kg/_pre_ptfi``
on first run and always read from there, so re-running re-derives the same result.
"""

from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path

import pandas as pd

_KGC = Path(__file__).resolve().parent.parent  # backend/kgc
KG = _KGC / "outputs" / "kg"
PTFI = Path("/mnt/share/kaichixie/FA_monorepo/new_datasets/DataForKG/ptfi")
BACKUP = KG / "_pre_ptfi"
FILES = ("entities", "triplets", "attestations", "evidence")
PRISTINE_MAX_ID = 227401  # expected post-build max entity id (guards against re-merge)


# --------------------------------------------------------------------------- IO
def load_pristine_kg() -> dict[str, pd.DataFrame]:
    """Read the four KG tables from the pristine snapshot (creating it once)."""
    BACKUP.mkdir(exist_ok=True)
    if not (BACKUP / "entities.parquet").exists():
        current_max = _max_id(pd.read_parquet(KG / "entities.parquet")["foodatlas_id"])
        if current_max != PRISTINE_MAX_ID:
            msg = f"outputs/kg max id is e{current_max}, expected e{PRISTINE_MAX_ID} — refusing to snapshot a non-pristine KG"
            raise SystemExit(msg)
        for f in FILES:
            shutil.copy2(KG / f"{f}.parquet", BACKUP / f"{f}.parquet")
    return {f: pd.read_parquet(BACKUP / f"{f}.parquet") for f in FILES}


def load_ptfi_delta() -> dict[str, pd.DataFrame]:
    return {f: pd.read_csv(PTFI / f"{f}_ptfi_delta.csv") for f in FILES}


# ------------------------------------------------------------------- id mapping
def build_id_map(
    kg_entities: pd.DataFrame, ptfi_entities: pd.DataFrame
) -> tuple[dict[str, str], list[str], set[str]]:
    """Return (old→final id map, kept-old-ids in order, dropped-dup old-ids)."""
    foodon_to_food = _foodon_index(kg_entities)
    m = _max_id(kg_entities["foodatlas_id"])

    id_map: dict[str, str] = {}
    kept: list[str] = []
    dropped: set[str] = set()
    next_num = m + 1
    for old, etype, ext in zip(
        ptfi_entities["foodatlas_id"],
        ptfi_entities["entity_type"],
        ptfi_entities["external_ids"],
    ):
        existing = _dedup_target(etype, ext, foodon_to_food)
        if existing is not None:
            id_map[old] = existing
            dropped.add(old)
        else:
            id_map[old] = f"e{next_num}"
            kept.append(old)
            next_num += 1
    return id_map, kept, dropped


def _dedup_target(etype, ext, foodon_to_food) -> str | None:
    """The existing food id a PTFI food collapses onto (by FoodOn IRI), or None."""
    if etype != "food":
        return None
    for iri in _as_dict(ext).get("foodon", []) or []:
        if iri in foodon_to_food:
            return foodon_to_food[iri]
    return None


def _foodon_index(kg_entities: pd.DataFrame) -> dict[str, str]:
    foods = kg_entities[kg_entities["entity_type"] == "food"]
    index: dict[str, str] = {}
    for fid, ext in zip(foods["foodatlas_id"], foods["external_ids"]):
        for iri in _as_dict(ext).get("foodon", []) or []:
            index.setdefault(iri, fid)
    return index


# ----------------------------------------------------------------- table merges
def merge_entities(
    kg: pd.DataFrame, ptfi: pd.DataFrame, id_map: dict[str, str], kept: list[str]
) -> pd.DataFrame:
    keep = ptfi[ptfi["foodatlas_id"].isin(set(kept))].copy()
    keep["foodatlas_id"] = keep["foodatlas_id"].map(id_map)
    keep["scientific_name"] = keep["scientific_name"].fillna("")
    return pd.concat([kg, keep[kg.columns]], ignore_index=True)


def merge_triplets(
    kg: pd.DataFrame, ptfi: pd.DataFrame, id_map: dict[str, str]
) -> tuple[pd.DataFrame, dict]:
    remap = _remapper(id_map)
    pt = ptfi.copy()
    pt["head_id"] = pt["head_id"].map(remap)
    pt["tail_id"] = pt["tail_id"].map(remap)
    pt["_att"] = pt["attestation_ids"].map(_parse_list)

    collapsed = (
        pt.groupby(["head_id", "tail_id"])["_att"]
        .apply(lambda s: sorted({a for lst in s for a in lst}))
        .reset_index()
    )

    kg = kg.copy()
    r1 = kg[kg["relationship_id"] == "r1"]
    key_to_idx = {(h, t): i for i, h, t in zip(r1.index, r1["head_id"], r1["tail_id"])}

    new_rows, merged, added = [], 0, 0
    for head, tail, att in zip(
        collapsed["head_id"], collapsed["tail_id"], collapsed["_att"]
    ):
        idx = key_to_idx.get((head, tail))
        if idx is not None:
            union = sorted(set(_parse_list(kg.at[idx, "attestation_ids"])) | set(att))
            kg.at[idx, "attestation_ids"] = json.dumps(union)
            merged += 1
        else:
            new_rows.append(
                {
                    "head_id": head,
                    "relationship_id": "r1",
                    "tail_id": tail,
                    "source": "ptfi",
                    "attestation_ids": json.dumps(att),
                }
            )
            added += 1
    out = pd.concat([kg, pd.DataFrame(new_rows, columns=kg.columns)], ignore_index=True)
    return out, {"collapsed_pairs": len(collapsed), "merged": merged, "added": added}


def merge_attestations(
    kg: pd.DataFrame, ptfi: pd.DataFrame, id_map: dict[str, str]
) -> pd.DataFrame:
    remap = _remapper(id_map)
    pa = ptfi.copy()
    # Native list<string> columns → write Python lists (not JSON strings).
    pa["head_candidates"] = pa["head_candidates"].map(
        lambda c: [remap(x) for x in _parse_list(c)]
    )
    pa["tail_candidates"] = pa["tail_candidates"].map(
        lambda c: [remap(x) for x in _parse_list(c)]
    )
    for col in ("validated", "validated_correct"):
        pa[col] = pa[col].map(_to_bool)
    # KG stores these as strings; PTFI may have parsed some (e.g. conc_value_raw)
    # as floats — coerce to str so the concatenated column stays large_string.
    for col in ("head_name_raw", "tail_name_raw", "conc_unit", "conc_value_raw",
                "conc_unit_raw", "food_part", "food_processing", "source"):
        if col in pa.columns:
            pa[col] = pa[col].fillna("").astype(str)
    return pd.concat([kg, pa[kg.columns]], ignore_index=True)


def merge_evidence(kg: pd.DataFrame, ptfi: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([kg, ptfi[kg.columns]], ignore_index=True)


# ----------------------------------------------------------------- verification
def verify(kg, out, ptfi, id_map, kept, dropped, stats) -> None:
    ents, tris, atts, evs = out["entities"], out["triplets"], out["attestations"], out["evidence"]

    assert len(dropped) == 257, f"expected 257 dedup foods, got {len(dropped)}"
    assert len(ents) == len(kg["entities"]) + len(kept), "entity count delta wrong"
    assert ents["foodatlas_id"].is_unique, "duplicate foodatlas_id after merge"

    trip_key = tris["head_id"] + "|" + tris["relationship_id"] + "|" + tris["tail_id"]
    assert trip_key.is_unique, "duplicate (head,rel,tail) after merge"
    assert stats["added"] + stats["merged"] == stats["collapsed_pairs"]

    assert atts["attestation_id"].is_unique, "duplicate attestation_id after merge"
    kg_att_ids = set(kg["attestations"]["attestation_id"])
    ptfi_att_ids = set(ptfi["attestations"]["attestation_id"])
    assert not (kg_att_ids & ptfi_att_ids), "PTFI attestation_id collides with KG"

    ent_ids = set(ents["foodatlas_id"])
    new_tri = tris.tail(stats["added"])
    missing = (set(new_tri["head_id"]) | set(new_tri["tail_id"])) - ent_ids
    assert not missing, f"new triplet refs missing from entities: {list(missing)[:5]}"

    all_ref_att = {a for c in tris["attestation_ids"] for a in _parse_list(c)}
    assert ptfi_att_ids <= set(atts["attestation_id"]), "PTFI attestations not all loaded"
    assert ptfi_att_ids <= all_ref_att, "some PTFI attestations unreferenced by triplets"

    ev_ids = set(evs["evidence_id"])
    assert set(ptfi["evidence"]["evidence_id"]) <= ev_ids, "PTFI evidence not appended"


# ---------------------------------------------------------------------- helpers
def _remapper(id_map):
    return lambda x: id_map.get(str(x), str(x))


def _parse_list(cell) -> list[str]:
    if isinstance(cell, (list, tuple)):
        return [str(x) for x in cell]
    if hasattr(cell, "tolist"):
        return [str(x) for x in cell.tolist()]
    if isinstance(cell, str) and cell.strip():
        try:
            v = ast.literal_eval(cell)
        except (ValueError, SyntaxError):
            try:
                v = json.loads(cell)
            except json.JSONDecodeError:
                v = [cell]
        return [str(x) for x in (v if isinstance(v, (list, tuple)) else [v])]
    return []


def _as_dict(cell) -> dict:
    if isinstance(cell, dict):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            return json.loads(cell)
        except json.JSONDecodeError:
            return {}
    return {}


def _to_bool(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "t", "yes")
    return bool(v)


def _max_id(series) -> int:
    return max(int(s[1:]) for s in series if isinstance(s, str) and s[1:].isdigit())


def _report(kg, out, id_map, kept, dropped, stats) -> None:
    print("PTFI merge report")
    print(f"  dedup (foods collapsed onto existing): {len(dropped)}")
    print(f"  new entities minted:                   {len(kept)}  "
          f"(re-based e{PRISTINE_MAX_ID+1}..e{PRISTINE_MAX_ID+len(kept)})")
    for f in FILES:
        print(f"  {f:12s} {len(kg[f]):>8d} -> {len(out[f]):>8d}  (+{len(out[f])-len(kg[f])})")
    print(f"  contains edges: {stats['added']} new + {stats['merged']} merged "
          f"into existing = {stats['collapsed_pairs']} pairs")


def main() -> int:
    kg = load_pristine_kg()
    ptfi = load_ptfi_delta()
    id_map, kept, dropped = build_id_map(kg["entities"], ptfi["entities"])

    tris, stats = merge_triplets(kg["triplets"], ptfi["triplets"], id_map)
    out = {
        "entities": merge_entities(kg["entities"], ptfi["entities"], id_map, kept),
        "triplets": tris,
        "attestations": merge_attestations(kg["attestations"], ptfi["attestations"], id_map),
        "evidence": merge_evidence(kg["evidence"], ptfi["evidence"]),
    }
    verify(kg, out, ptfi, id_map, kept, dropped, stats)
    for f in FILES:
        out[f].to_parquet(KG / f"{f}.parquet", index=False)
    _report(kg, out, id_map, kept, dropped, stats)
    print("PTFI merge complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
