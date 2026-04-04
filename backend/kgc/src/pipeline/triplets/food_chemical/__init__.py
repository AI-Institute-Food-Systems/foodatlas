"""Food-chemical (CONTAINS) triplet builders."""

from .dmd import merge_dmd_triplets
from .fdc import merge_fdc_triplets

__all__ = ["merge_dmd_triplets", "merge_fdc_triplets"]
