"""Select the most frequently mentioned synonym as each entity's common name."""

import logging

import pandas as pd

from ...stores.attestation_store import AttestationStore
from ...stores.entity_store import EntityStore
from ...stores.triplet_store import TripletStore
from ...utils.constants import ID_PREFIX_MAPPER

logger = logging.getLogger(__name__)


def _is_internal_mention(mention: str) -> bool:
    """Return True if *mention* is an auto-generated internal ID string."""
    return any(mention.startswith(f"_{prefix}") for prefix in ID_PREFIX_MAPPER.values())


def count_synonym_mentions(
    entities: pd.DataFrame,
    triplets: pd.DataFrame,
    metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Count how often each synonym appears as a mention in metadata.

    Args:
        entities: Entity DataFrame indexed by ``foodatlas_id``.
        triplets: Triplet DataFrame (only ``r1`` rows are used).
        metadata: MetadataContains DataFrame indexed by ``foodatlas_id``.

    Returns:
        *entities* with a new ``synonym_counts`` column (dict[str, int]).
    """
    entities = entities.copy()
    entities["synonym_counts"] = entities["synonyms"].apply(
        lambda x: dict.fromkeys(x, 0),
    )

    contains_triplets = triplets[triplets["relationship_id"] == "r1"]

    for _, row in contains_triplets.iterrows():
        head_id: str = row["head_id"]
        tail_id: str = row["tail_id"]
        meta_ids: list[str] = row["metadata_ids"]

        meta_rows = metadata.loc[metadata.index.intersection(meta_ids)]
        head_mentions: list[str] = meta_rows["_food_name"].tolist()
        tail_mentions: list[str] = meta_rows["_chemical_name"].tolist()

        _increment_counts(entities, head_id, head_mentions)
        _increment_counts(entities, tail_id, tail_mentions)

    return entities


def _increment_counts(
    entities: pd.DataFrame,
    entity_id: str,
    mentions: list[str],
) -> None:
    """Increment synonym_counts for *entity_id* by observed *mentions*."""
    if entity_id not in entities.index:
        return
    counts: dict[str, int] = entities.at[entity_id, "synonym_counts"]
    for mention in mentions:
        if not _is_internal_mention(mention) and mention in counts:
            counts[mention] += 1


def update_common_names(entities: pd.DataFrame) -> pd.DataFrame:
    """Set ``common_name`` to the synonym with the highest mention count.

    Expects *entities* to have a ``synonym_counts`` column (from
    :func:`count_synonym_mentions`).  Entities with zero total mentions
    keep their existing ``common_name``.

    Returns:
        *entities* without the ``synonym_counts`` column.
    """
    entities = entities.copy()

    mask = entities["synonym_counts"].apply(lambda c: sum(c.values()) > 0)
    entities.loc[mask, "common_name"] = entities.loc[mask, "synonym_counts"].apply(
        lambda c: max(c, key=c.__getitem__),
    )

    return entities.drop(columns=["synonym_counts"])


def apply_common_names(
    entity_store: EntityStore,
    triplet_store: TripletStore,
    metadata_store: AttestationStore,
) -> None:
    """End-to-end: count mentions and update common names in *entity_store*."""
    entities = count_synonym_mentions(
        entity_store._entities,
        triplet_store._triplets,
        metadata_store._records,
    )
    updated = update_common_names(entities)
    entity_store._entities = updated
    logger.info("Updated common names for %d entities.", len(updated))
