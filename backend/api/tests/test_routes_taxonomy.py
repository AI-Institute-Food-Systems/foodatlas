"""Tests for taxonomy route endpoints across all entity types."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

TAXONOMY_SAMPLE = {
    "data": {
        "entity_id": "e123",
        "nodes": [
            {"id": "e123", "name": "apple"},
            {"id": "e59", "name": "plant fruit food product"},
        ],
        "edges": [
            {"child_id": "e123", "parent_id": "e59"},
        ],
    }
}

TAXONOMY_EMPTY: dict[str, object] = {
    "data": {"entity_id": None, "nodes": [], "edges": []},
}


class TestFoodTaxonomy:
    def test_returns_taxonomy_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.taxonomy.get_taxonomy",
            return_value=TAXONOMY_SAMPLE,
        ):
            resp = client.get("/food/taxonomy", params={"common_name": "apple"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["entity_id"] == "e123"
        assert len(body["data"]["nodes"]) == 2
        assert len(body["data"]["edges"]) == 1

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/food/taxonomy")
        assert resp.status_code == 422


class TestChemicalTaxonomy:
    def test_returns_taxonomy_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.taxonomy.get_taxonomy",
            return_value=TAXONOMY_SAMPLE,
        ):
            resp = client.get("/chemical/taxonomy", params={"common_name": "glucose"})
        assert resp.status_code == 200
        assert "nodes" in resp.json()["data"]

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/chemical/taxonomy")
        assert resp.status_code == 422


class TestDiseaseTaxonomy:
    def test_returns_taxonomy_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.taxonomy.get_taxonomy",
            return_value=TAXONOMY_SAMPLE,
        ):
            resp = client.get("/disease/taxonomy", params={"common_name": "diabetes"})
        assert resp.status_code == 200
        assert "edges" in resp.json()["data"]

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/disease/taxonomy")
        assert resp.status_code == 422
