"""Shared bioactivity filter contract: one dataset, one oracle, two test suites.

``contracts/bioactivity_filters.json`` holds a fixed dataset of measurements
plus the counts a modal must show for chosen (pair, filters) cases. The API
permutation tests (``test_bioact_permutations_pg.py``) and the frontend modal
tests read the same file, so the SQL filters and the browser filters are both
checked against the same expected numbers.

The oracle below is written from the product rules, not from the production
code, so a wrong production helper cannot validate itself:

- an assay is shown unless its endpoint is a hidden hotfix endpoint;
- a unit folds to its canonical alias; a blank unit is "None";
- Evidence, Unit and Source must hold for the SAME assay;
- a row is listed when at least one of its assays passes every filter;
- a facet count is the rows listed after picking that value.

Regenerate after a deliberate data change:
``uv run python -m tests.bioact_contract``.
"""

from __future__ import annotations

import itertools
import json
import random
from pathlib import Path

from src.repositories._bioact_hotfix import (
    _LEAKED_ASSAY_ENDPOINTS,
    _OUTCOME_NOT_ENDPOINT,
    _UNIT_ALIASES,
)

CONTRACT = (
    Path(__file__).resolve().parents[3] / "contracts" / "bioactivity_filters.json"
)

EVIDENCE = ["in vitro", "in vivo", "molecular-level"]
UNITS = ["uM", "nM", "mg/kg", "None"]
SOURCES = ["", "experimental", "predicted"]

_RAW_EVIDENCE = ["in vitro", "in vitro ", "in vivo", "molecular-level", ""]
_RAW_UNITS = ["uM", "um", "µM", "nM", "mg/kg", "", None]
_RAW_SOURCES = ["Experimental", "predicted", "Computational", "curated"]
_RAW_ENDPOINTS = ["IC50", "Kd", "HEPG2TOX ASSAY - CC50", "Cytotoxicity"]


# --- oracle --------------------------------------------------------------


def shown(m: dict) -> bool:
    ep = (m.get("endpoint") or "").strip()
    return ep not in _LEAKED_ASSAY_ENDPOINTS and ep.lower() not in _OUTCOME_NOT_ENDPOINT


def norm_unit(u: str | None) -> str:
    s = (u or "").strip()
    if not s or s.upper() == "NONE":
        return "None"
    return _UNIT_ALIASES.get(s.lower(), s)


def kind(src: str | None) -> str:
    s = (src or "").lower()
    if s.startswith("exp"):
        return "experimental"
    if s.startswith(("pred", "comp")):
        return "predicted"
    return ""


def matches(m: dict, f: dict, skip: str = "") -> bool:
    if not shown(m):
        return False
    ets, units = f.get("evidence", []), f.get("unit", [])
    if ets and skip != "evidence" and (m.get("evidence_type") or "").strip() not in ets:
        return False
    if units and skip != "unit" and norm_unit(m.get("unit")) not in units:
        return False
    sk = f.get("source", "")
    return not (sk and skip != "source" and kind(m.get("evidence_source")) != sk)


def modal_count(measurements: list[dict], f: dict) -> int:
    return sum(matches(m, f) for m in measurements)


# --- dataset -------------------------------------------------------------


def _measurement(rng: random.Random) -> dict:
    return {
        "evidence_type": rng.choice(_RAW_EVIDENCE),
        "unit": rng.choice(_RAW_UNITS),
        "evidence_source": rng.choice(_RAW_SOURCES),
        "endpoint": rng.choice(_RAW_ENDPOINTS),
        "value": round(rng.uniform(0.1, 900), 2),
        "outcome": rng.choice(["Active", "Inactive"]),
    }


def build_dataset(seed: int = 7) -> dict:
    rng = random.Random(seed)
    bioactivities = ["anticancer", "antioxidant"]
    chemicals = [f"chem{i:02d}" for i in range(1, 11)]
    foods = ["apple", "pear"]
    categories = {
        "chem01": ["flavonoid"],
        "chem02": ["flavonoid", "polyphenol"],
        "chem03": ["alkaloid"],
        "chem07": ["alkaloid", "flavonoid"],
    }
    pairs = []
    for kind_, heads in (("chemical", chemicals), ("food", foods)):
        for head, bio in itertools.product(heads, bioactivities):
            if rng.random() < 0.15:
                continue
            n = rng.choice([1, 2, 3, 5, 8, 26, 31])
            pairs.append(
                {
                    "kind": kind_,
                    "head": head,
                    "bioactivity": bio,
                    "measurements": [_measurement(rng) for _ in range(n)],
                }
            )
    composition = {"apple": chemicals[:6], "pear": chemicals[3:9]}
    return {
        "bioactivities": bioactivities,
        "categories": categories,
        "composition": composition,
        "pairs": pairs,
    }


def filter_cases() -> list[dict]:
    """Every Evidence x Unit x Source combination."""
    cases = []
    for n_et in range(len(EVIDENCE) + 1):
        for ets in itertools.combinations(EVIDENCE, n_et):
            for n_u in range(len(UNITS) + 1):
                for units in itertools.combinations(UNITS, n_u):
                    cases.extend(
                        {"evidence": list(ets), "unit": list(units), "source": sk}
                        for sk in SOURCES
                    )
    return cases


def modal_cases(data: dict) -> tuple[list[dict], list[list[int]]]:
    """Expected modal totals per pair, as ``[pair, case, expected]`` triples."""
    picked = filter_cases()[::7]
    triples = [
        [i, j, modal_count(p["measurements"], f)]
        for i, p in enumerate(data["pairs"])
        for j, f in enumerate(picked)
    ]
    return picked, triples


def api_measurements(raw: list[dict]) -> list[dict]:
    """What ``/bioactivity/measurements`` returns: shown assays, units folded."""
    return [{**m, "unit": norm_unit(m.get("unit"))} for m in raw if shown(m)]


def write_contract() -> None:
    data = build_dataset()
    for p in data["pairs"]:
        p["api_measurements"] = api_measurements(p["measurements"])
    data["modal_filters"], triples = modal_cases(data)
    body = json.dumps(data, indent=1, ensure_ascii=False)
    rows = ",\n  ".join(json.dumps(t) for t in triples)
    CONTRACT.parent.mkdir(exist_ok=True)
    CONTRACT.write_text(body[:-2] + f',\n "modal_cases": [\n  {rows}\n ]\n}}\n')


def load_contract() -> dict:
    data: dict = json.loads(CONTRACT.read_text(encoding="utf-8"))
    return data


if __name__ == "__main__":
    write_contract()
