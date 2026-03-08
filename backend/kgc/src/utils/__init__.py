"""KGC utilities."""

from .constants import ID_PREFIX_MAPPER, get_lookup_key_by_id
from .json_io import read_json, write_json
from .merge_sets import merge_sets

__all__ = [
    "ID_PREFIX_MAPPER",
    "get_lookup_key_by_id",
    "merge_sets",
    "read_json",
    "write_json",
]
