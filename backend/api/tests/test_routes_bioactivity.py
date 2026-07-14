"""Tests for /bioactivity route endpoints (sidebar count facets)."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient


# -- /bioactivity/categories ----------------------------------------------

CATEGORY_SAMPLE = {
    "data": [
        {"category": "flavonoid", "count": 12},
        {"category": "alkaloid", "count": 3},
    ],
    "metadata": {"row_count": 2},
}


class TestBioactivityCategoryOptions:
    def test_returns_categories(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.bioactivity.get_category_options",
            return_value=CATEGORY_SAMPLE,
        ):
            resp = client.get(
                "/bioactivity/categories",
                params={"common_name": "antioxidant"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["category"] == "flavonoid"
        assert body["data"][0]["count"] == 12

    def test_facet_filters_forwarded(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.bioactivity.get_category_options",
            return_value=CATEGORY_SAMPLE,
        ) as mocked:
            client.get(
                "/bioactivity/categories",
                params={
                    "common_name": "antioxidant",
                    "filter_unit": "uM",
                    "filter_source_kind": "experimental",
                    "search": "querc",
                },
            )
            kwargs = mocked.call_args.kwargs
            assert kwargs["filter_unit"] == "uM"
            assert kwargs["filter_source_kind"] == "experimental"
            assert kwargs["search"] == "querc"

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get("/bioactivity/categories")
        assert resp.status_code == 422


# -- /bioactivity/source_kinds --------------------------------------------

SOURCE_KIND_SAMPLE = {
    "data": {"both": 15, "experimental": 10, "predicted": 5},
}


class TestBioactivitySourceKindCounts:
    def test_returns_counts(self, client: TestClient, mock_db: AsyncMock) -> None:
        with patch(
            "src.repositories.bioactivity.get_source_kind_counts",
            return_value=SOURCE_KIND_SAMPLE,
        ):
            resp = client.get(
                "/bioactivity/source_kinds",
                params={
                    "common_name": "antioxidant",
                    "direction": "bioactivity-chemicals",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["experimental"] == 10
        assert body["data"]["predicted"] == 5

    def test_facet_filters_forwarded(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        with patch(
            "src.repositories.bioactivity.get_source_kind_counts",
            return_value=SOURCE_KIND_SAMPLE,
        ) as mocked:
            client.get(
                "/bioactivity/source_kinds",
                params={
                    "common_name": "antioxidant",
                    "direction": "bioactivity-chemicals",
                    "filter_unit": "uM",
                    "filter_category": "flavonoid",
                    "search": "querc",
                },
            )
            kwargs = mocked.call_args.kwargs
            assert kwargs["filter_unit"] == "uM"
            assert kwargs["filter_category"] == "flavonoid"
            assert kwargs["search"] == "querc"

    def test_missing_required_params_returns_422(self, client: TestClient) -> None:
        resp = client.get("/bioactivity/source_kinds")
        assert resp.status_code == 422
