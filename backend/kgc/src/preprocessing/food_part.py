"""Food part standardization."""

from __future__ import annotations


def standardize_food_part(food_part: str) -> str:
    """Standardize a food part string.

    Args:
        food_part: The raw food part string.

    Returns:
        The standardized food part string.

    """
    return food_part.strip().lower()
