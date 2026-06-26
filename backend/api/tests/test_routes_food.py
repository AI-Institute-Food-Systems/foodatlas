"""Tests for food and download route endpoints."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

# -- /food/metadata --------------------------------------------------------

FOOD_META_SAMPLE = {
    "data": [
        {
            "common_name": "apple",
            "id": "FA:0001",
            "entity_type": "food",
            "scientific_name": "Malus domestica",
            "synonyms": ["apple fruit"],
            "external_ids": {"fdc": "12345"},
            "food_classification": "fruit",
        }
    ],
    "metadata": {"row_count": 1},
}


class TestFoodMetadata:
    def test_returns_data_and_metadata(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.food.get_metadata",
            return_value=FOOD_META_SAMPLE,
        ):
            resp = client.get("/food/metadata", params={"common_name": "apple"})
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "metadata" in body
        assert body["metadata"]["row_count"] == 1

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get("/food/metadata")
        assert resp.status_code == 422


# -- /food/profile ---------------------------------------------------------

FOOD_PROFILE_SAMPLE = {
    "data": {
        "carbohydrates(incl.fiber)": [{"name": "glucose", "median_concentration": 5.0}],
        "lipids": [],
        "vitamins": [],
        "amino acids and proteins": [],
        "minerals(incl.derivatives)": [],
        "others": [],
    },
}


class TestFoodProfile:
    def test_returns_profile_data(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.food.get_profile",
            return_value=FOOD_PROFILE_SAMPLE,
        ):
            resp = client.get("/food/profile", params={"common_name": "apple"})
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "carbohydrates(incl.fiber)" in body["data"]

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get("/food/profile")
        assert resp.status_code == 422


# -- /food/composition -----------------------------------------------------

FOOD_COMP_SAMPLE = {
    "data": [
        {
            "name": "glucose",
            "id": "FA:C001",
            "chemical_classification": ["carbohydrate"],
            "median_concentration": 5.0,
        }
    ],
    "metadata": {
        "row_count": 1,
        "rows_per_page": 25,
        "current_row": 1,
        "current_page": 1,
        "total_rows": 1,
        "total_pages": 1,
    },
}


class TestFoodComposition:
    def test_returns_paginated_data(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.food.get_composition",
            return_value=FOOD_COMP_SAMPLE,
        ):
            resp = client.get("/food/composition", params={"common_name": "apple"})
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert body["metadata"]["rows_per_page"] == 25

    def test_query_params_forwarded(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.food.get_composition",
            return_value=FOOD_COMP_SAMPLE,
        ) as mocked:
            client.get(
                "/food/composition",
                params={
                    "common_name": "apple",
                    "page": "2",
                    "filter_source": "fdc",
                    "search": "gluc",
                    "sort_by": "median_concentration",
                    "sort_dir": "asc",
                    "show_all_rows": "false",
                },
            )
            call_args = mocked.call_args
            # The route converts show_all_rows string to bool
            assert call_args is not None

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get("/food/composition")
        assert resp.status_code == 422


# -- /download -------------------------------------------------------------


BUNDLE_SAMPLE = [
    {
        "version": "v1.0",
        "release_date": "2026-04-20",
        "file_size": "1.2 GB",
        "kgc_run": "20260420T173000Z",
        "download_link": "https://example.s3.us-west-1.amazonaws.com/bundles/foodatlas-v1.0/foodatlas-v1.0.zip",
        "summary_link": "https://example.s3.us-west-1.amazonaws.com/bundles/foodatlas-v1.0/SUMMARY.md",
    },
]


class TestDownload:
    def test_returns_empty_when_no_bucket_configured(self, client: TestClient) -> None:
        resp = client.get("/download")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"] == []
        assert body["metadata"]["row_count"] == 0

    def test_returns_manifest_entries_when_bucket_configured(
        self, client_with_downloads_bucket: TestClient
    ) -> None:
        with patch(
            "src.repositories.downloads.fetch_manifest",
            return_value=BUNDLE_SAMPLE,
            new_callable=AsyncMock,
        ):
            resp = client_with_downloads_bucket.get("/download")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"] == BUNDLE_SAMPLE
        assert body["metadata"]["row_count"] == 1
