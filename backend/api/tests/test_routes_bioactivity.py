"""Tests for /bioactivity internal SSR routes."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

if TYPE_CHECKING:
    from unittest.mock import AsyncMock

    from fastapi.testclient import TestClient


_META_SAMPLE = {
    "data": [
        {
            "common_name": "anti-inflammatory",
            "id": "bio1",
            "entity_type": "bioactivity",
            "scientific_name": "",
            "synonyms": [],
            "external_ids": {},
            "description": "reduces inflammation",
            "ambiguity_siblings": [],
        }
    ],
    "metadata": {"row_count": 1},
}

_PAGED = {
    "data": [{"id": "x1", "name": "x"}],
    "metadata": {
        "row_count": 1,
        "rows_per_page": 10,
        "current_row": 1,
        "current_page": 1,
        "total_rows": 1,
        "total_pages": 1,
    },
}


class TestBioactivityMetadata:
    def test_returns_data(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.bioactivity.get_metadata", return_value=_META_SAMPLE
        ):
            resp = client.get(
                "/bioactivity/metadata", params={"common_name": "anti-inflammatory"}
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["entity_type"] == "bioactivity"

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/bioactivity/metadata")
        assert resp.status_code == 422


class TestBioactivityChemicals:
    def test_returns_paged(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch("src.repositories.bioactivity.get_chemicals", return_value=_PAGED):
            resp = client.get(
                "/bioactivity/chemicals", params={"common_name": "anti-inflammatory"}
            )
        assert resp.status_code == 200
        assert resp.json()["metadata"]["total_rows"] == 1


class TestBioactivityFoods:
    def test_returns_paged(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch("src.repositories.bioactivity.get_foods", return_value=_PAGED):
            resp = client.get(
                "/bioactivity/foods",
                params={"common_name": "antioxidant", "exhibit_type": "direct"},
            )
        assert resp.status_code == 200
        assert resp.json()["data"][0]["id"] == "x1"


class TestBioactivityDiseases:
    def test_returns_paged(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch("src.repositories.bioactivity.get_diseases", return_value=_PAGED):
            resp = client.get(
                "/bioactivity/diseases", params={"common_name": "anti-inflammatory"}
            )
        assert resp.status_code == 200
        assert resp.json()["metadata"]["total_pages"] == 1
