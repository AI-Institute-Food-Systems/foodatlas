"""Convert the PTFI delta into external-id-keyed source data for PTFIAdapter.

The delta FoodAtlas received (``*_ptfi_delta.csv``) is *post-resolution*: it
carries hardcoded ``e227381+`` entity ids and cites 573 pre-existing FoodAtlas
chemicals by entity id instead of describing them. Entity ids are a build
artifact, so a source keyed on them cannot be re-ingested — the root reason
PTFI needed a post-build merge with an id re-base.

This de-references those ids **once**, against a KG snapshot, and writes source
files keyed only on external identifiers:

* ``ptfi_foods.csv``        — one row per PTFI food (FoodOn IRI + GGB sample ids)
* ``ptfi_chemicals.csv``    — one row per chemical (PubChem CID + InChIKey)
* ``ptfi_measurements.csv`` — one row per (sample, chemical) abundance
* ``ptfi_evidence.json``    — the dataset provenance the adapter attaches

From then on the adapter reads only these files, and entity resolution does the
FoodOn-IRI dedup and id minting natively every run, so PTFI ids stay stable
with no merge step.

Why the delta and not PTFI's raw release: the raw files identify analytes only
by an internal ``MET_PTF…`` code plus a formula-retention label, with no
PubChem CID or InChIKey anywhere (``vocabulary_encoding_scheme`` is empty for
all 27,713 analytes), and they reproduce only ~3,086 of the delta's 17,045
measurements — the ``*_reference_chemicals_combined`` input the delta was built
from was never delivered. The delta is the only copy of the identification.

Run once; the output is committed source data. Re-run only when PTFI delivers a
new delta.

Usage:
    uv run python scripts/derive_ptfi_source.py \
        --delta-dir ../../new_datasets/DataForKG/ptfi \
        --kg-snapshot data/PreviousFAKG/20260915T081827Z
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger("derive_ptfi_source")

_DELTA_FILES = ("entities", "triplets", "attestations", "evidence")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_arguments()

    delta = load_delta(args.delta_dir)
    kg_chemicals = load_kg_chemicals(args.kg_snapshot)

    cited = dereference_cited_chemicals(delta, kg_chemicals)
    foods = build_foods(delta["entities"])
    chemicals = build_chemicals(delta["entities"], cited)
    measurements = build_measurements(delta, cited)

    write_outputs(args.out_dir, foods, chemicals, measurements, delta["evidence"])
    report(foods, chemicals, measurements, args.out_dir)
    return 0


# --------------------------------------------------------------------------- IO
def load_delta(delta_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        name: pd.read_csv(delta_dir / f"{name}_ptfi_delta.csv") for name in _DELTA_FILES
    }


def load_kg_chemicals(snapshot: Path) -> pd.DataFrame:
    """Chemical entities from a KG snapshot, indexed by foodatlas_id."""
    entities = pd.read_parquet(snapshot / "entities.parquet")
    chemicals = entities[entities["entity_type"] == "chemical"]
    return chemicals.set_index("foodatlas_id")


def write_outputs(
    out_dir: Path,
    foods: pd.DataFrame,
    chemicals: pd.DataFrame,
    measurements: pd.DataFrame,
    evidence: pd.DataFrame,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    foods.to_csv(out_dir / "ptfi_foods.csv", index=False)
    chemicals.to_csv(out_dir / "ptfi_chemicals.csv", index=False)
    measurements.to_csv(out_dir / "ptfi_measurements.csv", index=False)
    reference = _as_dict(evidence["reference"].iloc[0])
    (out_dir / "ptfi_evidence.json").write_text(json.dumps(reference, indent=2) + "\n")


# ------------------------------------------------------- chemical de-referencing
def dereference_cited_chemicals(
    delta: dict[str, pd.DataFrame], kg_chemicals: pd.DataFrame
) -> dict[str, dict[str, str]]:
    """Map each pre-existing chemical entity id the delta cites to its CID.

    These are chemicals PTFI measured that FoodAtlas already had, so the delta
    cites an entity id rather than describing them. Fails loudly on any id
    absent from the snapshot or carrying no PubChem CID — silently dropping one
    would lose real measurements.
    """
    described = set(delta["entities"]["foodatlas_id"])
    cited = set(delta["triplets"]["tail_id"]) - described

    resolved: dict[str, dict[str, str]] = {}
    unresolved: list[str] = []
    for entity_id in sorted(cited, key=lambda e: int(e[1:])):
        if entity_id not in kg_chemicals.index:
            unresolved.append(f"{entity_id} (absent from the snapshot)")
            continue
        row = kg_chemicals.loc[entity_id]
        external = _as_dict(row["external_ids"])
        cids = external.get("pubchem_compound") or external.get("pubchem_cid") or []
        if not cids:
            unresolved.append(f"{entity_id} ({row['common_name']}): no PubChem CID")
            continue
        inchikeys = external.get("inchikey") or []
        resolved[entity_id] = {
            "cid": _cid_str(cids[0]),
            "inchikey": str(inchikeys[0]) if inchikeys else "",
            "common_name": str(row["common_name"]),
        }

    if unresolved:
        msg = (
            f"{len(unresolved)} cited chemical(s) cannot be keyed by CID:\n  "
            + "\n  ".join(unresolved[:20])
        )
        raise SystemExit(msg)
    logger.info("De-referenced %d cited chemicals to PubChem CIDs.", len(resolved))
    return resolved


# --------------------------------------------------------------- source building
def build_foods(entities: pd.DataFrame) -> pd.DataFrame:
    """One row per PTFI food, keyed on its FoodOn IRI and GGB sample ids."""
    foods = entities[entities["entity_type"] == "food"]
    rows = []
    for _, row in foods.iterrows():
        external = _as_dict(row["external_ids"])
        sample_ids = sorted(str(s) for s in external.get("ptfi", []))
        rows.append(
            {
                "ptfi_sample_ids": json.dumps(sample_ids),
                "foodon_iri": _first(external.get("foodon", [])),
                "common_name": str(row["common_name"]),
                "scientific_name": _blank_if_na(row["scientific_name"]),
                "synonyms": json.dumps(_as_list(row["synonyms"])),
                "food_groups": json.dumps(
                    _as_dict(row["attributes"]).get("food_groups", [])
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("ptfi_sample_ids", ignore_index=True)


def build_chemicals(
    entities: pd.DataFrame, cited: dict[str, dict[str, str]]
) -> pd.DataFrame:
    """One row per chemical PTFI measured — both described and pre-existing.

    ``ptfi_id`` is empty for the pre-existing ones: the delta never carried a
    PTFI metabolite id for them, only the entity id just de-referenced.
    """
    described = entities[entities["entity_type"] == "chemical"]
    rows = []
    for _, row in described.iterrows():
        external = _as_dict(row["external_ids"])
        rows.append(
            {
                "cid": _cid_str(_first(external.get("pubchem_compound", []))),
                "ptfi_id": _first(external.get("ptfi", [])),
                "inchikey": _first(external.get("inchikey", [])),
                "common_name": str(row["common_name"]),
            }
        )
    rows.extend(
        {
            "cid": info["cid"],
            "ptfi_id": "",
            "inchikey": info["inchikey"],
            "common_name": info["common_name"],
        }
        for info in cited.values()
    )
    chemicals = pd.DataFrame(rows)
    missing = int((chemicals["cid"] == "").sum())
    if missing:
        msg = f"{missing} chemical row(s) have no CID and cannot be keyed"
        raise SystemExit(msg)
    return chemicals.drop_duplicates(subset="cid").sort_values(
        "cid", ignore_index=True, key=lambda s: s.astype(int)
    )


def build_measurements(
    delta: dict[str, pd.DataFrame], cited: dict[str, dict[str, str]]
) -> pd.DataFrame:
    """One row per (sample, chemical) abundance, keyed on GGB id and CID."""
    cid_by_entity = _entity_to_cid(delta["entities"], cited)
    attestations = delta["attestations"].set_index("attestation_id")

    rows = []
    for tail_id, attestation_ids in zip(
        delta["triplets"]["tail_id"],
        delta["triplets"]["attestation_ids"],
        strict=False,
    ):
        cid = cid_by_entity.get(tail_id, "")
        if not cid:
            msg = f"triplet tail {tail_id} has no CID mapping"
            raise SystemExit(msg)
        for attestation_id in _as_list(attestation_ids):
            attestation = attestations.loc[attestation_id]
            rows.append(
                {
                    "sample_id": str(attestation["head_name_raw"]),
                    "cid": cid,
                    "chemical_name": str(attestation["tail_name_raw"]),
                    "conc_value": attestation["conc_value"],
                    "conc_unit": str(attestation["conc_unit"]),
                }
            )
    return pd.DataFrame(rows).sort_values(
        ["sample_id", "cid"], ignore_index=True, kind="stable"
    )


def _entity_to_cid(
    entities: pd.DataFrame, cited: dict[str, dict[str, str]]
) -> dict[str, str]:
    """Entity id -> CID, covering both the described and the cited chemicals.

    This map exists only to rewrite the delta's triplet tails; the entity ids
    do not survive into the derived source.
    """
    described = entities[entities["entity_type"] == "chemical"]
    by_entity = {
        entity_id: _cid_str(_first(_as_dict(external).get("pubchem_compound", [])))
        for entity_id, external in zip(
            described["foodatlas_id"], described["external_ids"], strict=False
        )
    }
    by_entity.update({entity_id: info["cid"] for entity_id, info in cited.items()})
    return {k: v for k, v in by_entity.items() if v}


# ---------------------------------------------------------------------- helpers
def _as_dict(cell: object) -> dict:
    if isinstance(cell, dict):
        return cell
    if isinstance(cell, str) and cell.strip():
        try:
            return json.loads(cell)
        except json.JSONDecodeError:
            return {}
    return {}


def _as_list(cell: object) -> list:
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str) and cell.strip():
        for parse in (json.loads, ast.literal_eval):
            try:
                value = parse(cell)
            except (ValueError, SyntaxError):
                continue
            return value if isinstance(value, list) else []
    return []


def _first(values: list) -> str:
    return str(values[0]) if values else ""


def _cid_str(value: object) -> str:
    """Normalize a CID to a plain integer string (``75142.0`` -> ``75142``)."""
    return "" if value in ("", None) else str(value).split(".")[0]


def _blank_if_na(value: object) -> str:
    return "" if pd.isna(value) else str(value)


def report(
    foods: pd.DataFrame,
    chemicals: pd.DataFrame,
    measurements: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Summarize the derived source, flagging entities no measurement touches.

    The orphan counts matter: PTFI catalogues far more chemicals than it
    detects in any sample, so most described chemicals carry no measurement and
    would enter the KG as entities with nothing to show.
    """
    measured_samples = set(measurements["sample_id"])
    measured_cids = set(measurements["cid"])
    foods_measured = sum(
        1 for ids in foods["ptfi_sample_ids"] if measured_samples & set(json.loads(ids))
    )
    chemicals_measured = int(chemicals["cid"].isin(measured_cids).sum())

    logger.info("PTFI source written to %s:", out_dir)
    logger.info(
        "  foods        %5d  (%d with a FoodOn IRI; %d measured, %d unmeasured)",
        len(foods),
        int((foods["foodon_iri"] != "").sum()),
        foods_measured,
        len(foods) - foods_measured,
    )
    logger.info(
        "  chemicals    %5d  (%d PTFI-described, %d pre-existing; "
        "%d measured, %d unmeasured)",
        len(chemicals),
        int((chemicals["ptfi_id"] != "").sum()),
        int((chemicals["ptfi_id"] == "").sum()),
        chemicals_measured,
        len(chemicals) - chemicals_measured,
    )
    logger.info(
        "  measurements %5d  over %d samples x %d chemicals",
        len(measurements),
        measurements["sample_id"].nunique(),
        measurements["cid"].nunique(),
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-dir", type=Path, required=True)
    parser.add_argument(
        "--kg-snapshot",
        type=Path,
        required=True,
        help="KG snapshot dir holding entities.parquet (cited ids resolve here)",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("data/PTFI"))
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
