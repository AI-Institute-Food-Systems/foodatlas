"""Tests for chemical, disease, and metadata route endpoints."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

# -- /chemical/metadata -----------------------------------------------------

CHEM_META_SAMPLE = {
    "data": [
        {
            "common_name": "glucose",
            "id": "FA:C001",
            "entity_type": "chemical",
            "scientific_name": "D-glucose",
            "synonyms": ["dextrose"],
            "external_ids": {},
            "chemical_classification": ["carbohydrate"],
        }
    ],
    "metadata": {"row_count": 1},
}


class TestChemicalMetadata:
    def test_returns_data_and_metadata(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.chemical.get_metadata",
            return_value=CHEM_META_SAMPLE,
        ):
            resp = client.get("/chemical/metadata", params={"common_name": "glucose"})
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "metadata" in body
        assert body["data"][0]["entity_type"] == "chemical"

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/chemical/metadata")
        assert resp.status_code == 422


# -- /chemical/composition --------------------------------------------------

CHEM_COMP_SAMPLE = {
    "data": {
        "with_concentrations": [
            {"id": "FA:F001", "name": "apple", "median_concentration": 5.0}
        ],
        "without_concentrations": [],
    },
    "metadata": {"row_count": 1},
}


class TestChemicalComposition:
    def test_returns_split_composition(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.chemical.get_composition",
            return_value=CHEM_COMP_SAMPLE,
        ):
            resp = client.get(
                "/chemical/composition", params={"common_name": "glucose"}
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "with_concentrations" in body["data"]
        assert "without_concentrations" in body["data"]

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/chemical/composition")
        assert resp.status_code == 422


# -- /chemical/correlation --------------------------------------------------

CHEM_CORR_SAMPLE = {
    "data": {
        "positive_associations": [
            {"id": "FA:D001", "name": "diabetes", "sources": [], "evidences": []}
        ],
        "negative_associations": None,
    },
    "metadata": {
        "row_count": 1,
        "rows_per_page": 10,
        "current_row": 1,
        "current_page": 1,
        "total_rows": 1,
        "total_pages": 1,
    },
}


class TestChemicalCorrelation:
    def test_returns_correlation_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.chemical.get_correlation",
            return_value=CHEM_CORR_SAMPLE,
        ):
            resp = client.get(
                "/chemical/correlation", params={"common_name": "glucose"}
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "positive_associations" in body["data"]
        assert body["metadata"]["rows_per_page"] == 10

    def test_with_relation_param(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.chemical.get_correlation",
            return_value=CHEM_CORR_SAMPLE,
        ):
            resp = client.get(
                "/chemical/correlation",
                params={"common_name": "glucose", "relation": "negative"},
            )
        assert resp.status_code == 200


# -- /disease/metadata ------------------------------------------------------

DISEASE_META_SAMPLE = {
    "data": [
        {
            "common_name": "diabetes",
            "id": "FA:D001",
            "entity_type": "disease",
            "scientific_name": "diabetes mellitus",
            "synonyms": [],
            "external_ids": {},
        }
    ],
    "metadata": {"row_count": 1},
}


class TestDiseaseMetadata:
    def test_returns_data_and_metadata(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.disease.get_metadata",
            return_value=DISEASE_META_SAMPLE,
        ):
            resp = client.get("/disease/metadata", params={"common_name": "diabetes"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["entity_type"] == "disease"

    def test_missing_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/disease/metadata")
        assert resp.status_code == 422


# -- /disease/correlation ---------------------------------------------------

DISEASE_CORR_SAMPLE = {
    "data": {
        "positive_associations": [
            {"id": "FA:C001", "name": "glucose", "sources": [], "evidences": []}
        ],
        "negative_associations": None,
    },
    "metadata": {
        "row_count": 1,
        "rows_per_page": 10,
        "current_row": 1,
        "current_page": 1,
        "total_rows": 1,
        "total_pages": 1,
    },
}


class TestDiseaseCorrelation:
    def test_returns_correlation_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.disease.get_correlation",
            return_value=DISEASE_CORR_SAMPLE,
        ):
            resp = client.get(
                "/disease/correlation", params={"common_name": "diabetes"}
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "positive_associations" in body["data"]
        assert body["metadata"]["total_pages"] == 1


# -- /metadata/search -------------------------------------------------------

SEARCH_SAMPLE = {
    "data": [
        {
            "foodatlas_id": "FA:0001",
            "associations": 42,
            "entity_type": "food",
            "common_name": "apple",
            "scientific_name": "Malus domestica",
            "synonyms": [],
            "external_ids": {},
        }
    ],
    "metadata": {
        "row_count": 1,
        "rows_per_page": 10,
        "current_row": 1,
        "current_page": 1,
        "total_rows": 1,
        "total_pages": 1,
    },
}


class TestMetadataSearch:
    def test_returns_search_results(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.search.search",
            return_value=SEARCH_SAMPLE,
        ):
            resp = client.get("/metadata/search", params={"term": "apple"})
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["data"]) == 1
        assert body["metadata"]["total_rows"] == 1

    def test_empty_term_allowed(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.search.search",
            return_value=SEARCH_SAMPLE,
        ):
            resp = client.get("/metadata/search")
        assert resp.status_code == 200


# -- /metadata/statistics ---------------------------------------------------

STATS_SAMPLE = {
    "data": {
        "statistics": {
            "foods": 1000,
            "chemicals": 500,
            "diseases": 200,
            "publications": 5000,
            "connections": 15000,
        }
    },
    "metadata": {"row_count": 5},
}


class TestMetadataStatistics:
    def test_returns_statistics(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.search.get_statistics",
            return_value=STATS_SAMPLE,
        ):
            resp = client.get("/metadata/statistics")
        assert resp.status_code == 200
        body = resp.json()
        assert "statistics" in body["data"]
        stats = body["data"]["statistics"]
        assert stats["foods"] == 1000
        assert stats["connections"] == 15000
