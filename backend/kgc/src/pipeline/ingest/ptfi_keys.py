"""How a PTFI chemical is keyed — shared by the prepare step that writes the
source, the adapter that reads it, and the entity and triplet stages that
resolve it."""

from __future__ import annotations

from typing import NamedTuple


class ChemicalKey(NamedTuple):
    column: str  # ptfi_chemicals.csv column
    source: str  # registry source and xref target; also the key's prefix
    external_ids_key: str  # where the id sits on a resolved entity


CHEMICAL_KEYS = (
    ChemicalKey("cid", "pubchem", "pubchem_compound"),
    ChemicalKey("chebi", "chebi", "chebi"),
    ChemicalKey("fdc_nutrient", "fdc_nutrient", "fdc_nutrient"),
)
"""In priority order. A chemical's ``chemical_key`` is ``<source>:<id>`` for the
first column it has a value in: most chemicals have a CID; nutrient classes and
totals (``protein``, ``saturated fatty acid``) have only a ChEBI or FDC nutrient
id. Every id it has becomes an xref."""
