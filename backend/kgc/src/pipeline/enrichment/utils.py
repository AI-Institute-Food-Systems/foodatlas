"""Shared helpers for enrichment modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...models.attributes import EntityAttributes

if TYPE_CHECKING:
    import pandas as pd
    from pydantic import BaseModel


def read_attributes[T: EntityAttributes](
    entities: pd.DataFrame,
    entity_id: str,
    model: type[T],
) -> T:
    """Read the ``attributes`` dict for *entity_id* and parse via *model*.

    Returns a default instance if the cell is missing or empty.
    """
    raw = entities.at[entity_id, "attributes"]
    if isinstance(raw, dict) and raw:
        result: T = model.model_validate(raw)
        return result
    return model()


def write_attributes(
    entities: pd.DataFrame,
    entity_id: str,
    attrs: BaseModel,
) -> None:
    """Serialize *attrs* and write back into the ``attributes`` column."""
    entities.at[entity_id, "attributes"] = attrs.model_dump()
