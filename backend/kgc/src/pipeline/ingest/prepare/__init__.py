"""Convert a bioactivity + PTFI delivery into the source files the adapters read.

Upstream deliveries cite FoodAtlas entity ids, which are build artifacts: the
PTFI delta cites hundreds of pre-existing chemicals by id instead of describing
them, and the whole-food bioactivity file keys its foods on the delta's private
ids. The pipeline never consumes such an id. This step de-references them once
— against the KG snapshot the delivery was built on and the delta's own entity
table — and writes source data keyed only on external identifiers (PubChem
CIDs, ChEBI and FDC nutrient ids, GGB sample ids), failing loudly on anything
it cannot key. ``scripts/prepare_delivery.py`` is the entry point.
"""

from .runner import prepare_delivery

__all__ = ["prepare_delivery"]
