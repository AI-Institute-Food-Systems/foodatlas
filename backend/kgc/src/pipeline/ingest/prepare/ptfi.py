"""PTFI delta → the external-id-keyed source ``PTFIAdapter`` reads.

The delta (``*_ptfi_delta.csv``) is post-resolution: it carries hardcoded entity
ids for the foods and chemicals it describes, and cites several hundred more
pre-existing FoodAtlas chemicals by entity id alone. Those ids are build
artifacts, so this module de-references them once, against the KG snapshot the
delta was built on, and writes:

* ``ptfi_foods.csv``        — one row per PTFI food (FoodOn IRI + GGB sample ids)
* ``ptfi_chemicals.csv``    — one row per chemical, keyed CID → ChEBI → FDC nutrient
* ``ptfi_measurements.csv`` — one row per attestation: sample, chemical, value,
  unit, and the evidence record it came from
* ``ptfi_evidence.csv``     — the delta's evidence records, verbatim

Why the delta and not PTFI's raw release: the raw files identify analytes only
by an internal ``MET_PTF…`` code, with no CID or InChIKey anywhere; the delta
is the only copy of the identification.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from ..ptfi_keys import CHEMICAL_KEYS
from .cells import as_dict, as_list, blank_if_na, first, int_str
from .snapshot import ids_of_type

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

logger = logging.getLogger(__name__)

DELTA_FILES = ("entities", "triplets", "attestations", "evidence")
CHEMICAL_COLUMNS = [
    "chemical_key",
    *(key.column for key in CHEMICAL_KEYS),
    "ptfi_id",
    "inchikey",
    "common_name",
]

ChemicalRow = dict[str, str]
"""One ``ptfi_chemicals.csv`` row: ``CHEMICAL_COLUMNS`` → value ("" if absent)."""


@dataclass
class PtfiSource:
    foods: pd.DataFrame
    chemicals: pd.DataFrame
    measurements: pd.DataFrame
    evidence: pd.DataFrame

    def write(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        self.foods.to_csv(out_dir / "ptfi_foods.csv", index=False)
        self.chemicals.to_csv(out_dir / "ptfi_chemicals.csv", index=False)
        self.measurements.to_csv(out_dir / "ptfi_measurements.csv", index=False)
        self.evidence.to_csv(out_dir / "ptfi_evidence.csv", index=False)


def load_delta(delta_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        name: pd.read_csv(delta_dir / f"{name}_ptfi_delta.csv") for name in DELTA_FILES
    }


def prepare_ptfi(delta: dict[str, pd.DataFrame], snapshot: pd.DataFrame) -> PtfiSource:
    """Validate the delta against *snapshot* and build the four source tables."""
    cited = dereference_cited_chemicals(delta, snapshot)
    attestations = attestation_rows(delta)
    log_name_agreement(attestations, set(cited), snapshot)

    by_entity = {**described_chemicals(delta["entities"]), **cited}
    foods = build_foods(delta["entities"])
    measurements = build_measurements(attestations, by_entity, delta["evidence"])
    check_samples_have_foods(foods, measurements)
    return PtfiSource(
        foods, build_chemicals(by_entity), measurements, delta["evidence"]
    )


# ------------------------------------------------------------- chemical identity
def described_chemicals(entities: pd.DataFrame) -> dict[str, ChemicalRow]:
    """Chemicals the delta describes itself, by the delta's entity id."""
    described = entities[entities["entity_type"] == "chemical"]
    return {
        row["foodatlas_id"]: chemical_row(
            as_dict(row["external_ids"]), row["common_name"]
        )
        for _, row in described.iterrows()
    }


def dereference_cited_chemicals(
    delta: dict[str, pd.DataFrame], snapshot: pd.DataFrame
) -> dict[str, ChemicalRow]:
    """Chemicals the delta cites by entity id alone, looked up in *snapshot*.

    Fails on any id that is not a chemical there or carries none of the keys in
    ``CHEMICAL_KEYS`` — silently dropping one would lose real measurements.
    """
    described = set(delta["entities"]["foodatlas_id"])
    cited = sorted(set(delta["triplets"]["tail_id"]) - described, key=_eid_order)
    chemicals = ids_of_type(snapshot, "chemical")

    rows: dict[str, ChemicalRow] = {}
    problems: list[str] = []
    for entity_id in cited:
        if entity_id not in chemicals:
            problems.append(f"{entity_id}: not a chemical in the snapshot")
            continue
        entity = snapshot.loc[entity_id]
        row = chemical_row(entity["external_ids"], entity["common_name"])
        if not row["chemical_key"]:
            problems.append(
                f"{entity_id} ({row['common_name']}): no CID, ChEBI or FDC id"
            )
            continue
        rows[entity_id] = row
    if problems:
        msg = f"{len(problems)} cited chemical(s) cannot be keyed:\n  " + "\n  ".join(
            problems[:20]
        )
        raise ValueError(msg)
    logger.info("De-referenced %d cited chemicals.", len(rows))
    return rows


def chemical_row(external_ids: Mapping[str, list], common_name: object) -> ChemicalRow:
    row = {
        key.column: int_str(first(external_ids.get(key.external_ids_key, [])))
        for key in CHEMICAL_KEYS
    }
    row["chemical_key"] = chemical_key(row)
    row["ptfi_id"] = first(external_ids.get("ptfi", []))
    row["inchikey"] = first(external_ids.get("inchikey", []))
    row["common_name"] = str(common_name)
    return row


def chemical_key(ids: Mapping[str, str]) -> str:
    """``<source>:<id>`` for the first key column with a value; "" when none."""
    for key in CHEMICAL_KEYS:
        if ids.get(key.column):
            return f"{key.source}:{ids[key.column]}"
    return ""


def log_name_agreement(
    attestations: pd.DataFrame, cited: set[str], snapshot: pd.DataFrame
) -> None:
    """How often the name PTFI gave a cited chemical is the snapshot entity's
    name or a synonym. Low agreement means the delta was built against a
    different KG than *snapshot*, and its cited ids point at other chemicals.
    (Reference-panel analyte codes such as ``ION_IRON`` never match.)"""
    pairs = attestations[attestations["tail_id"].isin(cited)]
    pairs = pairs[["tail_id", "tail_name_raw"]].drop_duplicates()
    names = {
        entity_id: {
            str(snapshot.at[entity_id, "common_name"]).casefold(),
            *(str(s).casefold() for s in snapshot.at[entity_id, "synonyms"]),
        }
        for entity_id in set(pairs["tail_id"])
    }
    agrees = [
        str(name).casefold() in names[entity_id]
        for entity_id, name in zip(
            pairs["tail_id"], pairs["tail_name_raw"], strict=False
        )
    ]
    logger.info(
        "Cited chemical names: %d of %d (entity, name) pairs match the snapshot.",
        sum(agrees),
        len(agrees),
    )
    for (_, pair), agree in zip(pairs.iterrows(), agrees, strict=False):
        if not agree:
            logger.debug("  %s: PTFI %r", pair["tail_id"], pair["tail_name_raw"])


# --------------------------------------------------------------- source building
def build_foods(entities: pd.DataFrame) -> pd.DataFrame:
    """One row per PTFI food, keyed on its FoodOn IRI and GGB sample ids."""
    foods = entities[entities["entity_type"] == "food"]
    rows = []
    for _, row in foods.iterrows():
        external = as_dict(row["external_ids"])
        rows.append(
            {
                "ptfi_sample_ids": json.dumps(sorted(str(s) for s in external["ptfi"])),
                "foodon_iri": first(external.get("foodon", [])),
                "common_name": str(row["common_name"]),
                "scientific_name": blank_if_na(row["scientific_name"]),
                "synonyms": json.dumps(as_list(row["synonyms"])),
                "food_groups": json.dumps(
                    as_dict(row["attributes"]).get("food_groups", [])
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("ptfi_sample_ids", ignore_index=True)


def build_chemicals(by_entity: Mapping[str, ChemicalRow]) -> pd.DataFrame:
    """One row per distinct chemical key, described and cited alike."""
    chemicals = pd.DataFrame(list(by_entity.values()), columns=CHEMICAL_COLUMNS)
    unkeyed = chemicals[chemicals["chemical_key"] == ""]
    if not unkeyed.empty:
        msg = f"{len(unkeyed)} described chemical(s) have no key: " + ", ".join(
            unkeyed["common_name"].head(10)
        )
        raise ValueError(msg)
    return chemicals.drop_duplicates("chemical_key").sort_values(
        "chemical_key", ignore_index=True
    )


def attestation_rows(delta: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Every attestation joined to the triplet that cites it (``tail_id``)."""
    triplets = delta["triplets"][["tail_id", "attestation_ids"]].copy()
    triplets["attestation_id"] = triplets["attestation_ids"].map(as_list)
    cited = triplets.explode("attestation_id").drop(columns=["attestation_ids"])
    rows = cited.merge(
        delta["attestations"], on="attestation_id", how="left", validate="one_to_one"
    )
    missing = rows["evidence_id"].isna()
    if missing.any():
        msg = f"{int(missing.sum())} attestation id(s) cited by triplets are absent"
        raise ValueError(msg)
    return rows


def build_measurements(
    attestations: pd.DataFrame,
    by_entity: Mapping[str, ChemicalRow],
    evidence: pd.DataFrame,
) -> pd.DataFrame:
    """One row per attestation, its chemical replaced by that chemical's key."""
    unknown_tails = set(attestations["tail_id"]) - set(by_entity)
    if unknown_tails:
        msg = f"{len(unknown_tails)} triplet tail(s) are not chemicals the delta covers"
        raise ValueError(msg)
    measurements = pd.DataFrame(
        {
            "sample_id": attestations["head_name_raw"].astype(str),
            "chemical_key": attestations["tail_id"].map(
                lambda e: by_entity[e]["chemical_key"]
            ),
            "chemical_name": attestations["tail_name_raw"].astype(str),
            "conc_value": attestations["conc_value"],
            "conc_unit": attestations["conc_unit"].astype(str),
            "evidence_id": attestations["evidence_id"].astype(str),
        }
    )
    unknown_evidence = set(measurements["evidence_id"]) - set(evidence["evidence_id"])
    if unknown_evidence:
        msg = f"attestations cite unknown evidence ids: {sorted(unknown_evidence)[:10]}"
        raise ValueError(msg)
    return measurements.sort_values(
        ["sample_id", "chemical_key"], ignore_index=True, kind="stable"
    )


def check_samples_have_foods(foods: pd.DataFrame, measurements: pd.DataFrame) -> None:
    """Every measured sample must belong to a food group, or the adapter would
    drop its measurements on the floor."""
    grouped = {s for ids in foods["ptfi_sample_ids"] for s in json.loads(ids)}
    orphans = sorted(set(measurements["sample_id"]) - grouped)
    if orphans:
        msg = f"{len(orphans)} sample id(s) belong to no PTFI food: {orphans[:10]}"
        raise ValueError(msg)


def _eid_order(entity_id: str) -> int:
    return int(entity_id[1:])
