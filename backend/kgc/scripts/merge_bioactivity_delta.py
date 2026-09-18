"""Three-way merge of the bioactivity delta onto a newer KGC run.

The KGC pipeline on ``kgc-production`` does not emit the bioactivity parquet, so
every biweekly run (``base``) lacks what ``staging-bioactivity`` (``delta``) added
on top of the run it was cut from (``ancestor``). This script applies
``delta - ancestor`` onto ``base``:

* **keyed tables** (entities, registry, attestations, ambiguous, evidence):
  ``base | (delta - ancestor)`` by id — the two sides add disjoint records.
* **triplets**: keyed on ``(head, relationship, tail)``; delta-only rows are
  appended and, for pairs both sides touched, ``attestation_ids`` is unioned
  (the only shared-record change either side makes is appending to that list).
* **relationships + bioactivity tables**: taken from ``delta`` (r5/r6 live there).
* **trust_signals / newsletter / CHANGELOG**: taken from ``base``.

``verify()`` hard-fails on anything that would make the merge lossy: a record
with the same key but different content on two inputs, an entity-id range
collision, dangling attestation/evidence refs, or a row count that is not
``base + delta - ancestor``. Retire this script once KGC emits bioactivity.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from _merge_common import _max_id, _parse_list

_KGC = Path(__file__).resolve().parent.parent  # backend/kgc
PREVIOUS = _KGC / "data" / "PreviousFAKG"
DEFAULT_ANCESTOR = PREVIOUS / "20260727T100828Z"
DEFAULT_DELTA = PREVIOUS / "staging-bioactivity"

KEYED: dict[str, list[str]] = {
    "entities": ["foodatlas_id"],
    "entity_registry": ["source", "native_id", "foodatlas_id"],  # aliases: 1 native → n
    "attestations": ["attestation_id"],
    "attestations_ambiguous": ["attestation_id"],
    "evidence": ["evidence_id"],
}
TRIPLET_KEY = ["head_id", "relationship_id", "tail_id"]
MERGED = (*KEYED, "triplets")
FROM_DELTA = (
    "relationships",
    "attestations_bioactivity",
    "bioassays",
    "bioactivity_disease",
    "bioactivity_disease_targets",
    "food_chemical_efficacy",
)
FROM_BASE = ("trust_signals.parquet", "newsletter.json", "CHANGELOG.md")
# First id the bioactivity build minted (21 concepts, PTFI re-based above them).
RESERVED_FROM = 227381
ATT_STR_COLS = (
    "head_name_raw",
    "tail_name_raw",
    "conc_unit",
    "conc_value_raw",
    "conc_unit_raw",
    "food_part",
    "food_processing",
    "source",
)

Tables = dict[str, pd.DataFrame]


# --------------------------------------------------------------------------- IO
def load(kg_dir: Path) -> Tables:
    return {f: pd.read_parquet(kg_dir / f"{f}.parquet") for f in MERGED}


def newest_run(root: Path = PREVIOUS) -> Path:
    runs = sorted(p for p in root.iterdir() if p.is_dir() and p.name[:1].isdigit())
    if not runs:
        raise SystemExit(f"no timestamped run under {root}; run pull-from-s3.sh")
    return runs[-1]


# ---------------------------------------------------------------------- merging
def _key(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    k = df[cols[0]].astype(str)
    for c in cols[1:]:
        k = k + "|" + df[c].astype(str)
    return k


def _append(base: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if new.empty:
        return base.copy()
    return pd.concat([base, new[base.columns]], ignore_index=True)


def merge_keyed(base, anc, delta, cols: list[str]) -> pd.DataFrame:
    """``base | (delta - ancestor)`` keyed on ``cols``."""
    new = delta[~_key(delta, cols).isin(set(_key(anc, cols)))]
    return _append(base, new)


def merge_attestations(base, anc, delta) -> pd.DataFrame:
    new = delta[~delta["attestation_id"].isin(set(anc["attestation_id"]))].copy()
    # Keep string columns string-typed so the concatenated column stays
    # large_string even if a slice came back all-null / numeric.
    for col in ATT_STR_COLS:
        if col in new.columns:
            new[col] = new[col].fillna("").astype(str)
    return _append(base, new)


def merge_triplets(base, anc, delta) -> tuple[pd.DataFrame, int]:
    """Append delta-only pairs; union ``attestation_ids`` on pairs both sides have."""
    bkey, akey, dkey = (_key(t, TRIPLET_KEY) for t in (base, anc, delta))
    delta_att = dict(zip(dkey, delta["attestation_ids"], strict=True))
    ids, unioned = [], 0
    for k, cur in zip(bkey, base["attestation_ids"], strict=True):
        ext = delta_att.get(k)
        if ext is None or ext == cur:
            ids.append(cur)
            continue
        have = set(_parse_list(cur))
        union = have | set(_parse_list(ext))
        if union == have:
            ids.append(cur)
        else:
            ids.append(json.dumps(sorted(union)))
            unioned += 1
    out = base.copy()
    out["attestation_ids"] = ids
    return _append(out, delta[~dkey.isin(set(akey))]), unioned


def merge(base: Tables, anc: Tables, delta: Tables) -> tuple[Tables, int]:
    out: Tables = {}
    for f, cols in KEYED.items():
        if f.startswith("attestations"):
            out[f] = merge_attestations(base[f], anc[f], delta[f])
        else:
            out[f] = merge_keyed(base[f], anc[f], delta[f], cols)
    out["triplets"], unioned = merge_triplets(
        base["triplets"], anc["triplets"], delta["triplets"]
    )
    return out, unioned


# ----------------------------------------------------------------- verification
def _content(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Row content indexed by key, list cells canonicalised, for equality checks."""
    body = df.drop(columns=cols)
    parts = [pd.Series("", index=df.index)]
    for c in body.columns:
        s = body[c]
        first = s.dropna().iloc[0] if s.notna().any() else None
        if isinstance(first, (np.ndarray, list)):
            s = s.map(lambda v: json.dumps(_parse_list(v)))
        parts.append(s.astype(str).fillna(""))  # pandas 3: NaN.astype(str) is NA
    joined = parts[0]
    for s in parts[1:]:
        joined = joined + s + "|"
    return pd.Series(joined.to_numpy(), index=_key(df, cols))


def _check_shared_content(name: str, cols: list[str], inputs: Tables) -> None:
    contents = {n: _content(df, cols) for n, df in inputs.items()}
    names = list(contents)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            shared = contents[a].index.intersection(contents[b].index)
            diff = contents[a][shared] != contents[b][shared]
            if diff.any():
                k = diff[diff].index[:3].tolist()
                raise SystemExit(f"{name}: {a} and {b} disagree on shared rows {k}")


def _check_triplets(base, anc, delta) -> None:
    """Shared pairs must agree on ``source``; attestation lists are append-only."""
    _check_shared_content(
        "triplets",
        TRIPLET_KEY,
        {
            n: t[[*TRIPLET_KEY, "source"]]
            for n, t in (("base", base), ("ancestor", anc), ("delta", delta))
        },
    )
    anc_att = dict(zip(_key(anc, TRIPLET_KEY), anc["attestation_ids"], strict=True))
    for side, t in (("base", base), ("delta", delta)):
        for k, cur in zip(_key(t, TRIPLET_KEY), t["attestation_ids"], strict=True):
            old = anc_att.get(k)
            if old is None or old == cur:
                continue
            if not set(_parse_list(old)) <= set(_parse_list(cur)):
                raise SystemExit(f"triplets: {side} dropped attestation ids on {k}")


def _check_id_ranges(base_ent, anc_ent, delta_ent) -> None:
    base_max = _max_id(base_ent["foodatlas_id"])
    if base_max >= RESERVED_FROM:
        raise SystemExit(
            f"base max entity id e{base_max} reaches the bioactivity range "
            f"(e{RESERVED_FROM}+); the delta can no longer be applied by id"
        )
    only = delta_ent[~delta_ent["foodatlas_id"].isin(set(anc_ent["foodatlas_id"]))]
    nums = [int(s[1:]) for s in only["foodatlas_id"] if s[1:].isdigit()]
    if nums and min(nums) < RESERVED_FROM:
        raise SystemExit(
            f"delta-only entity id e{min(nums)} below e{RESERVED_FROM}: "
            "not the bioactivity delta this script was written for"
        )


def _check_refs(out: Tables, delta_dir: Path) -> None:
    att_ids = set(out["attestations"]["attestation_id"])
    bio = delta_dir / "attestations_bioactivity.parquet"
    if bio.exists():  # r5/r6 reference measurement ids, not attestation ids
        att_ids |= set(pd.read_parquet(bio)["bioactivity_metadata_id"])
    referenced = {a for c in out["triplets"]["attestation_ids"] for a in _parse_list(c)}
    dangling = referenced - att_ids
    if dangling:
        sample = sorted(dangling)[:5]
        raise SystemExit(f"{len(dangling)} dangling attestation ids: {sample}")
    ev_ids = set(out["evidence"]["evidence_id"])
    missing = set(out["attestations"]["evidence_id"]) - ev_ids
    if missing:
        raise SystemExit(f"{len(missing)} dangling evidence ids: {sorted(missing)[:5]}")


def _check_counts(base: Tables, anc: Tables, delta: Tables, out: Tables) -> None:
    for f in MERGED:
        want = len(base[f]) + len(delta[f]) - len(anc[f])
        if len(out[f]) != want:
            raise SystemExit(f"{f}: {len(out[f])} rows, expected {want}")
        cols = TRIPLET_KEY if f == "triplets" else KEYED[f]
        if not _key(out[f], cols).is_unique:
            raise SystemExit(f"{f}: duplicate keys after merge")


def verify(base: Tables, anc: Tables, delta: Tables, out: Tables, delta_dir: Path):
    for f, cols in KEYED.items():
        _check_shared_content(
            f, cols, {"base": base[f], "ancestor": anc[f], "delta": delta[f]}
        )
    _check_triplets(base["triplets"], anc["triplets"], delta["triplets"])
    _check_id_ranges(base["entities"], anc["entities"], delta["entities"])
    _check_counts(base, anc, delta, out)
    _check_refs(out, delta_dir)


# ---------------------------------------------------------------------- output
def write(out: Tables, base_dir: Path, delta_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for f, df in out.items():
        df.to_parquet(out_dir / f"{f}.parquet", index=False)
    for f in FROM_DELTA:
        shutil.copy2(delta_dir / f"{f}.parquet", out_dir / f"{f}.parquet")
    for name in FROM_BASE:
        if (base_dir / name).exists():
            shutil.copy2(base_dir / name, out_dir / name)


def _report(base: Tables, anc: Tables, delta: Tables, out: Tables, unioned: int):
    print("bioactivity delta merge report")
    for f in MERGED:
        print(
            f"  {f:22s} {len(base[f]):>8d} -> {len(out[f]):>8d}  "
            f"(+{len(out[f]) - len(base[f])})"
        )
    base_delta = sum(len(base[f]) - len(anc[f]) for f in MERGED)
    bio_delta = sum(len(delta[f]) - len(anc[f]) for f in MERGED)
    print(f"  base - ancestor: {base_delta} rows; delta - ancestor: {bio_delta} rows")
    print(f"  triplets with unioned attestation_ids: {unioned}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Apply the bioactivity delta (delta - ancestor) onto a KGC run."
    )
    p.add_argument("--base", type=Path, help="newest run (default: newest pulled)")
    p.add_argument("--ancestor", type=Path, default=DEFAULT_ANCESTOR)
    p.add_argument("--delta", type=Path, default=DEFAULT_DELTA)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args(argv)
    base_dir = a.base or newest_run()
    print(f"base={base_dir}\nancestor={a.ancestor}\ndelta={a.delta}")

    base, anc, delta = load(base_dir), load(a.ancestor), load(a.delta)
    out, unioned = merge(base, anc, delta)
    verify(base, anc, delta, out, a.delta)
    write(out, base_dir, a.delta, a.out)
    _report(base, anc, delta, out, unioned)
    print(f"merged KG written to {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
