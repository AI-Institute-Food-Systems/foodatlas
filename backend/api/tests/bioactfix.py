"""Bioactivity fixture rows for the Postgres harness, as the ETL stores them."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession


def measurement(
    *,
    evidence_type: str = "in vitro",
    evidence_source: str = "experimental",
    endpoint: str = "IC50",
    unit: str = "uM",
    value: float = 1.0,
) -> dict:
    """One entry of the `measurements` JSONB array.

    Only the keys the filters read. `evidence_source` is matched by the
    SQL with ``LIKE 'exp%'`` / ``'pred%'`` / ``'comp%'`` and by Python
    with ``str.startswith``, so values here are chosen to exercise both
    the matching and the non-matching side of that.
    """
    return {
        "evidence_type": evidence_type,
        "evidence_source": evidence_source,
        "endpoint": endpoint,
        "unit": unit,
        "value": value,
    }


def _kind(source: str | None) -> str:
    s = (source or "").lower()
    if s.startswith("exp"):
        return "experimental"
    if s.startswith(("pred", "comp")):
        return "predicted"
    return ""


def assay_key(m: dict) -> str:
    """The ETL's ``assay_keys`` entry for one measurement."""
    return "\x1f".join(
        (
            m.get("evidence_type") or "",
            m.get("unit") or "",
            _kind(m.get("evidence_source")),
            m.get("endpoint") or "",
        )
    )


def chem_bioactivity_row(
    *,
    bioactivity: str,
    chemical: str,
    chem_id: str,
    measurements: Sequence[dict],
    hidden: Sequence[dict] = (),
    measurement_count: int | None = None,
    experimental_count: int | None = None,
    predicted_count: int | None = None,
    evidence_types: Sequence[str] | None = None,
    units: Sequence[str] | None = None,
) -> dict:
    """An MV row as the ETL would store it.

    ``measurements`` is the MV's display sample; ``hidden`` holds the pair's
    other measurements, past the sample cap. The stored counts and arrays
    cover both, as in the ETL; override them only to model drift. Load with
    :func:`load_bioactivity`, which also writes every measurement to the base
    tables that the per-row "N assays" count reads.
    """
    full = [*measurements, *hidden]
    sources = [(m.get("evidence_source") or "").lower() for m in full]
    if experimental_count is None:
        experimental_count = sum(s.startswith("exp") for s in sources)
    if predicted_count is None:
        predicted_count = sum(s.startswith(("pred", "comp")) for s in sources)
    if evidence_types is None:
        evidence_types = sorted({m["evidence_type"] for m in full} - {"", None})
    if units is None:
        units = sorted({m["unit"] for m in full} - {"", None})
    assay_keys = sorted({assay_key(m) for m in full})
    return {
        "bioactivity_name": bioactivity,
        "bioactivity_foodatlas_id": f"b-{bioactivity}",
        "chemical_name": chemical,
        "chemical_foodatlas_id": chem_id,
        "measurement_count": (
            len(full) if measurement_count is None else measurement_count
        ),
        "experimental_count": experimental_count,
        "predicted_count": predicted_count,
        "active_count": 0,
        "inactive_count": 0,
        "unspecified_count": 0,
        "inconclusive_count": 0,
        "n_foods": 0,
        "evidence_types": list(evidence_types),
        "units": list(units),
        "assay_keys": assay_keys,
        "measurements": list(measurements),
        "all_measurements": full,
    }


def food_bioactivity_row(
    *, bioactivity: str, food: str, food_id: str, **kwargs: Any
) -> dict:
    """A ``mv_food_bioactivity`` row; same arguments as chem_bioactivity_row."""
    row = chem_bioactivity_row(
        bioactivity=bioactivity, chemical=food, chem_id=food_id, **kwargs
    )
    row["food_name"] = row.pop("chemical_name")
    row["food_foodatlas_id"] = row.pop("chemical_foodatlas_id")
    return row


_MV_COLS = {
    "mv_chemical_bioactivity": (
        "bioactivity_name",
        "bioactivity_foodatlas_id",
        "chemical_name",
        "chemical_foodatlas_id",
        "measurement_count",
        "active_count",
        "inactive_count",
        "unspecified_count",
        "inconclusive_count",
        "experimental_count",
        "predicted_count",
        "n_foods",
        "evidence_types",
        "units",
        "assay_keys",
    ),
    "mv_food_bioactivity": (
        "food_name",
        "food_foodatlas_id",
        "bioactivity_name",
        "bioactivity_foodatlas_id",
        "measurement_count",
        "experimental_count",
        "predicted_count",
        "evidence_types",
        "units",
        "assay_keys",
    ),
}
_HEAD_COL = {
    "mv_chemical_bioactivity": ("chemical_foodatlas_id", "r6"),
    "mv_food_bioactivity": ("food_foodatlas_id", "r5"),
}


async def load_bioactivity(
    session: AsyncSession, rows: Sequence[dict], mv: str = "mv_chemical_bioactivity"
) -> None:
    """Insert MV rows plus their triplets and every measurement."""
    cols = _MV_COLS[mv]
    head_col, rel = _HEAD_COL[mv]
    insert_mv = text(
        f"INSERT INTO {mv} ({', '.join(cols)}, measurements)"
        f" VALUES ({', '.join(':' + c for c in cols)}, CAST(:measurements AS JSONB))"
    )
    for r in rows:
        await session.execute(
            insert_mv,
            {
                **{c: r[c] for c in cols},
                "measurements": json.dumps(r["measurements"]),
            },
        )
        head, tail = r[head_col], r["bioactivity_foodatlas_id"]
        ids = [f"{head}|{tail}|{i}" for i in range(len(r["all_measurements"]))]
        await session.execute(
            text(
                "INSERT INTO base_triplets (head_id, relationship_id, tail_id,"
                " attestation_ids) VALUES (:h, :rel, :t, :ids)"
            ),
            {"h": head, "rel": rel, "t": tail, "ids": ids},
        )
        for bm, m in zip(ids, r["all_measurements"], strict=True):
            await session.execute(
                text(
                    "INSERT INTO base_attestations_bioactivity"
                    " (bioactivity_metadata_id, reported_activity_outcome,"
                    " evidence_endpoint_type, potency_value, potency_unit,"
                    " evidence_source, evidence_type) VALUES (:bm, :outcome,"
                    " :endpoint, :value, :unit, :source, :etype)"
                ),
                {
                    "bm": bm,
                    "outcome": m.get("outcome", "Active"),
                    "endpoint": m.get("endpoint"),
                    "value": m.get("value"),
                    "unit": m.get("unit"),
                    "source": m.get("evidence_source"),
                    "etype": m.get("evidence_type"),
                },
            )
    await session.commit()
