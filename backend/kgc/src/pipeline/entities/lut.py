"""EntityLUT — ambiguity-aware lookup table replacing placeholder entities."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class EntityLUT:
    """Synonym → entity ID lookup that handles ambiguity without placeholders.

    Ambiguous entries (one name mapping to multiple entity IDs) are stored
    as-is. Callers choose how to handle ambiguity (prefer common_name match,
    prefer high-priority synonym type, or explode to all candidates).
    """

    def __init__(self) -> None:
        self._food: dict[str, list[str]] = {}
        self._chemical: dict[str, list[str]] = {}
        self._disease: dict[str, list[str]] = {}

    def _get_lut(self, entity_type: str) -> dict[str, list[str]]:
        luts = {
            "food": self._food,
            "chemical": self._chemical,
            "disease": self._disease,
        }
        return luts[entity_type]

    def add(self, entity_type: str, name: str, entity_id: str) -> None:
        """Register *name* → *entity_id* in the appropriate LUT."""
        lut = self._get_lut(entity_type)
        key = name.lower()
        if key not in lut:
            lut[key] = []
        if entity_id not in lut[key]:
            lut[key].append(entity_id)

    def lookup(self, entity_type: str, name: str) -> list[str]:
        """Return all entity IDs matching *name*. ``len > 1`` = ambiguous."""
        return self._get_lut(entity_type).get(name.lower(), [])

    def lookup_unique(self, entity_type: str, name: str) -> str | None:
        """Return entity ID only if unambiguous, else ``None``."""
        ids = self.lookup(entity_type, name)
        return ids[0] if len(ids) == 1 else None

    def contains(self, entity_type: str, name: str) -> bool:
        """Check if *name* is registered for *entity_type*."""
        return name.lower() in self._get_lut(entity_type)

    def get_food_lut(self) -> dict[str, list[str]]:
        """Return the raw food LUT (for serialization)."""
        return dict(self._food)

    def get_chemical_lut(self) -> dict[str, list[str]]:
        """Return the raw chemical LUT (for serialization)."""
        return dict(self._chemical)

    def ambiguous_entries(self, entity_type: str) -> dict[str, list[str]]:
        """Return all entries where a name maps to >1 entity ID."""
        return {k: v for k, v in self._get_lut(entity_type).items() if len(v) > 1}
