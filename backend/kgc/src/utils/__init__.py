"""KGC utilities."""

from .constants import ID_PREFIX_MAPPER, get_lookup_key_by_id
from .merge_sets import merge_sets

__all__ = [
    "ID_PREFIX_MAPPER",
    "get_lookup_key_by_id",
    "merge_sets",
]
