"""Shared pagination helpers for /v1/ list endpoints."""

from __future__ import annotations

from src.repositories.v1.serializers import Page

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100
# Bounds ``page`` so ``(page - 1) * page_size`` can never overflow a bigint
# OFFSET (which surfaced as a 500) and so nobody asks Postgres to skip
# billions of rows. Far past any real result set at the default page size.
MAX_PAGE = 1_000_000


def clamp_page_size(page_size: int) -> int:
    if page_size < 1:
        return DEFAULT_PAGE_SIZE
    return min(page_size, MAX_PAGE_SIZE)


def offset(page: int, page_size: int) -> int:
    return max(0, (page - 1) * page_size)


def build_page(page: int, page_size: int, total: int) -> Page:
    return Page(
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    )
