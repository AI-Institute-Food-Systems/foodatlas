"""Existence checks shared by the /v1/ sub-resource routes.

A sub-resource of an unknown parent must 404 like the parent itself does;
otherwise ``/v1/foods/nope/chemicals`` answers ``200 []`` and a developer
cannot tell a typo from a food with no data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException

from src.repositories.v1 import entities

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def require_entity(db: AsyncSession, entity_type: str, entity_id: str) -> None:
    """Raise 404 unless ``entity_id`` is a live entity of ``entity_type``."""
    found = await entities.resolve_id(db, entity_id)
    if found is None or found[0] != entity_type:
        raise HTTPException(
            status_code=404, detail=f"{entity_type.capitalize()} not found"
        )
