"""HOTFIX 2026-06-26 — REMOVE WHEN upstream endpoint/unit cleanup lands.

Surgical normaliser for ``base_attestations_bioactivity`` rows whose
``evidence_endpoint_type`` and ``potency_unit`` columns carry data
quality issues that haven't yet been fixed in KGC ingest. Tracked in
memory ``bioactivity-endpoint-unit-cleanup``; Kaichi confirmed he'll
clean these upstream — until then this module masks the noise from the
frontend.

Rules (per Kaichi 2026-06-26):

- A. Unit aliases — fold variants to one canonical:
     * ``ug.mL-1``, ``ug ml-1``, ``ug/ml`` → ``ug/mL``
     * ``uM``, ``MICROMOLAR``, ``microM``, ``µM``, ``μM`` → ``uM``
- B. Empty/garbage unit (``None`` literal or empty string) → ``"None"``
     so downstream isn't confused by NULLs.
- C. Drop rows whose endpoint is a leaked assay name (single-source
     strings that aren't endpoints at all).
- D. Drop rows whose endpoint is a generic outcome metric, not a
     potency endpoint. Per Kaichi these can happen depending on target,
     but the user wants them dropped from the bioactivity view until
     they're segregated upstream.
- E. Endpoint variants — leave alone (Kaichi will review case by case).
- F. Numeric IC/EC/CC suffixes — leave alone (legit % inhibition
     cutoffs).

To remove this hotfix:
1. Delete this file.
2. Remove the three imports + call sites in ``bioactivity.py`` (search
   for ``hotfix``).
3. Re-enable the endpoint·unit filter UI per memory
   ``bioactivity-endpoint-unit-cleanup``.
4. Update / delete that memory.
"""

from __future__ import annotations

# Unit normalisation (case-insensitive lookup, value is canonical form).
_UNIT_ALIASES: dict[str, str] = {
    "ug.ml-1": "ug/mL",
    "ug ml-1": "ug/mL",
    "ug/ml": "ug/mL",
    "um": "uM",
    "micromolar": "uM",
    "microm": "uM",
    "µm": "uM",
    "μm": "uM",  # different Greek mu codepoint (U+03BC vs U+00B5)
}

# Endpoint strings that are actually assay-name leakage. Drop the row.
_LEAKED_ASSAY_ENDPOINTS: frozenset[str] = frozenset(
    {
        "LUCIFERASE INFECTION ASSAY - IC50",
        "HEPG2TOX ASSAY - CC50",
        "LUCIFERASE EXPRESSION CONTROL - IC50",
        "Maximum test concentration that did not exhibit cytotoxicity",
        "ED50 / ED50 (taxol)",
    }
)

# Endpoints that are outcome metrics, not potency endpoints. Drop the
# row. Comparison is case-insensitive on the trimmed value.
_OUTCOME_NOT_ENDPOINT: frozenset[str] = frozenset(
    s.lower()
    for s in (
        "Mitotic index",
        "Cell cycle",
        "Drug uptake",
        "Phosphorylation rate",
        "T-cell",
        "Optimal concentration",
        "Effective concentration",
        "Average",
        "Cytotoxicity",
        "Concentration",
        "Stability",
        "Cell toxicity",
        "Cytotoxic endpoint",
        "Tumor size",
        "Growth inhibition",
        "Toxicity",
        "Dose",
        "Inhibition",
        "Diameter",
        "Relative",
        "Selectivity index",
        "IP",
    )
)


def normalize_unit(unit: str | None) -> str:
    """Fold unit variants to canonical form. Empty/garbage → ``"None"``."""
    if unit is None:
        return "None"
    stripped = unit.strip()
    if not stripped or stripped.upper() == "NONE":
        return "None"
    return _UNIT_ALIASES.get(stripped.lower(), stripped)


def should_drop(endpoint: str | None) -> bool:
    """True for endpoint values that should be hidden until upstream cleans."""
    if endpoint is None:
        return False
    stripped = endpoint.strip()
    if stripped in _LEAKED_ASSAY_ENDPOINTS:
        return True
    return stripped.lower() in _OUTCOME_NOT_ENDPOINT


def clean_measurements(measurements: list[dict] | None) -> list[dict]:
    """Drop bad-endpoint rows and normalise units in the survivors.

    Returns a new list — never mutates the input.
    """
    if not measurements:
        return []
    out: list[dict] = []
    for m in measurements:
        if should_drop(m.get("endpoint")):
            continue
        cleaned = dict(m)
        cleaned["unit"] = normalize_unit(m.get("unit"))
        out.append(cleaned)
    return out


def clean_endpoint_options(rows: list[dict]) -> list[dict]:
    """For the endpoint-options endpoint: drop bad endpoints, fold units.

    Folds duplicate (endpoint, unit) keys that arise from unit aliasing
    by summing their counts.
    """
    folded: dict[tuple[str, str], int] = {}
    for r in rows:
        endpoint = (r.get("endpoint") or "").strip()
        if should_drop(endpoint):
            continue
        unit = normalize_unit(r.get("unit"))
        key = (endpoint, unit)
        folded[key] = folded.get(key, 0) + int(r.get("count") or 0)
    return [
        {"endpoint": e, "unit": u, "count": c}
        for (e, u), c in sorted(folded.items(), key=lambda kv: -kv[1])
    ]
