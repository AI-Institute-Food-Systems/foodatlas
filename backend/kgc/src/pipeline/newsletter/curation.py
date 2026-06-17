"""LLM chemical curation for newsletter angle highlights.

Food names are NOT touched here — the newsletter uses literal foodatlas
common_names (see statistics._canonical). This module only cleans the
newly-discovered chemical list per highlighted food: most-recognizable name per
compound, lipid species collapsed to classes, generics/artifacts dropped. The
already-chosen food name is passed only as context. ~one call per highlight.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import TYPE_CHECKING

import anthropic
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    import pandas as pd

    from .statistics import FoodHighlight

logger = logging.getLogger(__name__)

_MAX_SYNONYMS = 6
_MAX_CANDIDATES = 80


class _Chemicals(BaseModel):
    chemicals: list[str] = Field(
        description="Distinct compounds; most-recognizable name each; notable first.",
    )


class _CleanName(BaseModel):
    index: int
    name: str | None = Field(description="Clean common name, or null to drop.")


class _CleanNames(BaseModel):
    items: list[_CleanName]


_CHEM_SYSTEM = """\
You clean a list of chemical names linked to one food, for a public
food-chemistry newsletter.

For each chemical, choose the MOST RECOGNIZABLE name from its common name +
synonyms ("vitamin C", not "l-ascorbate" or "E300"; "folate", not "folate(2-)").
Then:
- One compound per entry (deduplicate synonyms).
- Collapse lipid species into their class: "tg(...)" -> "triacylglycerol",
  "pc(...)" / "lecithin" -> "phosphatidylcholine", and likewise diacylglycerol,
  phosphatidylethanolamine, etc.
- Drop generic umbrella terms and nutrient classes (not specific compounds):
  protein, carbohydrate, fat, fatty acid, essential fatty acid, saturated /
  unsaturated fat, amino acid, peptide, sugar, sugars, starch, fiber, lipid,
  sterol, polyphenols, flavonoids, carotenoids, tannins, minerals, ash, water,
  energy, calories. (Keep specific named compounds and defined lipid classes
  such as triacylglycerol / phosphatidylcholine.)
- Drop artifacts / non-food entries: contaminants and synthetic compounds
  (perfluoro- compounds, heavy metals), lab reagents, bare peptides ("phe-ala",
  "gly-ala"), hormones.

Return the most notable distinct compounds, most notable first.\
"""

_CHEM_USER = """\
Food: {food}

Chemicals linked to this food (name; synonyms):
{chemicals}

Return up to {max_n} clean, distinct food compounds.\
"""


def curate_chemicals(
    highlights: list[FoodHighlight],
    entities: pd.DataFrame,
    *,
    model: str,
    max_chemicals: int,
    max_workers: int,
) -> list[FoodHighlight]:
    """Per highlight, clean ``new_chemicals`` (food_name is used only as context)."""
    if not highlights:
        return []
    client = anthropic.Anthropic()
    name_map = _name_map(entities)
    synonyms = _synonym_map(entities)

    def curate(h: FoodHighlight) -> FoodHighlight:
        chems = _format_entities(h.new_chemical_ids, name_map, synonyms)
        try:
            names = _curate_one(client, model, h.food_name, chems, max_chemicals)
        except Exception:
            logger.exception("Chemical curation failed for %s", h.food_name)
            names = h.new_chemicals[:max_chemicals]
        return replace(h, new_chemicals=names)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(curate, highlights))


def _curate_one(
    client: anthropic.Anthropic,
    model: str,
    food: str,
    chemicals: str,
    max_n: int,
) -> list[str]:
    response = client.messages.parse(
        model=model,
        max_tokens=1500,
        system=_CHEM_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": _CHEM_USER.format(
                    food=food, chemicals=chemicals, max_n=max_n
                ),
            }
        ],
        output_format=_Chemicals,
    )
    parsed = response.parsed_output
    return parsed.chemicals[:max_n] if parsed else []


_CLEAN_SYSTEM = """\
You normalize chemical names for a public food-chemistry newsletter, one entry
per input index.

For each numbered chemical, return the SAME compound under its MOST RECOGNIZABLE
name (common name or a synonym): "vitamin C" not "l-ascorbate"; "folate" not
"folate(2-)"; "phosphatidylethanolamine" not a systematic glycerophospholipid
name. Collapse a specific lipid species to its class ("tg(16:0/...)" ->
"triacylglycerol"; a galactosyldiacylglycerol -> "monogalactosyldiacylglycerol"
/ "digalactosyldiacylglycerol" as appropriate).

Return name=null to DROP an entry that is not a specific food compound: generic
umbrella terms (protein, fat, fatty acid, sugar, fiber, lipid, sterol,
polyphenols, minerals, ash, water, energy) and artifacts / non-food entries
(perfluoro- compounds, heavy metals, lab reagents, bare peptides like "phe-ala",
hormones).

Return exactly one item per input index.\
"""


def clean_chemical_names(
    chem_ids: list[str],
    entities: pd.DataFrame,
    *,
    model: str,
    max_workers: int,
    chunk_size: int = 40,
) -> dict[str, str | None]:
    """Map each chemical entity id to a clean common name (or None to drop).

    Aligned per-entity cleaning — unlike ``curate_chemicals`` (which collapses a
    food's whole list). Gives the "new in the literature" cards the same clean
    names as the highlights. A failed chunk degrades to the raw common_names.
    """
    ids = list(dict.fromkeys(chem_ids))
    if not ids:
        return {}
    client = anthropic.Anthropic()
    name_map = _name_map(entities)
    synonyms = _synonym_map(entities)
    chunks = [ids[i : i + chunk_size] for i in range(0, len(ids), chunk_size)]

    def clean(chunk: list[str]) -> dict[str, str | None]:
        try:
            return _clean_chunk(client, model, chunk, name_map, synonyms)
        except Exception:
            logger.exception("Card chemical curation failed for a chunk")
            return {cid: name_map.get(cid) for cid in chunk}

    out: dict[str, str | None] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for resolved in pool.map(clean, chunks):
            out.update(resolved)
    return out


def _clean_chunk(
    client: anthropic.Anthropic,
    model: str,
    ids: list[str],
    name_map: dict[str, str],
    synonyms: dict[str, list[str]],
) -> dict[str, str | None]:
    lines = []
    for index, cid in enumerate(ids):
        name = name_map.get(cid, cid)
        syns = [
            s
            for s in synonyms.get(cid, [])[:_MAX_SYNONYMS]
            if s.lower() != name.lower()
        ]
        syn_part = f"; {', '.join(syns)}" if syns else ""
        lines.append(f"{index}. {name}{syn_part}")

    response = client.messages.parse(
        model=model,
        max_tokens=2500,
        system=_CLEAN_SYSTEM,
        messages=[{"role": "user", "content": "Chemicals:\n" + "\n".join(lines)}],
        output_format=_CleanNames,
    )
    resolved: dict[str, str | None] = {cid: name_map.get(cid) for cid in ids}
    parsed = response.parsed_output
    if parsed:
        for item in parsed.items:
            if 0 <= item.index < len(ids):
                clean = (item.name or "").strip()
                resolved[ids[item.index]] = clean or None
    return resolved


def _format_entities(
    ids: list[str],
    name_map: dict[str, str],
    synonyms: dict[str, list[str]],
) -> str:
    lines = []
    for eid in ids[:_MAX_CANDIDATES]:
        name = name_map.get(eid, eid)
        syns = [
            s
            for s in synonyms.get(eid, [])[:_MAX_SYNONYMS]
            if s.lower() != name.lower()
        ]
        syn_part = f"; {', '.join(syns)}" if syns else ""
        lines.append(f"- {name}{syn_part}")
    return "\n".join(lines) or "(none)"


def _name_map(entities: pd.DataFrame) -> dict[str, str]:
    e = entities if "foodatlas_id" in entities.columns else entities.reset_index()
    return {
        str(fid): str(name)
        for fid, name in zip(e["foodatlas_id"], e["common_name"], strict=False)
    }


def _synonym_map(entities: pd.DataFrame) -> dict[str, list[str]]:
    if "synonyms" not in entities.columns:
        return {}
    e = entities if "foodatlas_id" in entities.columns else entities.reset_index()
    return {
        str(fid): _as_list(syn)
        for fid, syn in zip(e["foodatlas_id"], e["synonyms"], strict=False)
    }


def _as_list(value: object) -> list[str]:
    """Synonyms are stored as a JSON-encoded list string; parse to a list."""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return [value] if value.strip() else []
        return (
            [str(v) for v in parsed if str(v).strip()]
            if isinstance(parsed, list)
            else []
        )
    try:
        return [str(v) for v in value if str(v).strip()]
    except TypeError:
        return []
