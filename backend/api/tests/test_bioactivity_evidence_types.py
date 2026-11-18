"""Tests for the /bioactivity/evidence_types sidebar-count endpoint
and its repository function `get_evidence_type_counts`.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from src.repositories.bioactivity_facets import PageFilters, get_evidence_type_counts

# -- Route -----------------------------------------------------------------


SAMPLE_PAYLOAD = {
    "data": [
        {"evidence_type": "in vitro", "count": 42},
        {"evidence_type": "in vivo", "count": 7},
        {"evidence_type": "molecular-level", "count": 3},
    ],
    "metadata": {"row_count": 3},
}


class TestEvidenceTypesRoute:
    def test_returns_data_and_metadata(
        self, client: TestClient, mock_db: AsyncMock
    ) -> None:
        del mock_db  # fixture must be requested to override the DB dep
        with patch(
            "src.repositories.bioactivity_facets.get_evidence_type_counts",
            return_value=SAMPLE_PAYLOAD,
        ):
            resp = client.get(
                "/bioactivity/evidence_types",
                params={
                    "common_name": "antioxidant",
                    "direction": "bioactivity-chemicals",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["metadata"]["row_count"] == 3
        assert body["data"][0]["evidence_type"] == "in vitro"

    def test_missing_common_name_returns_422(self, client: TestClient) -> None:
        resp = client.get(
            "/bioactivity/evidence_types",
            params={"direction": "bioactivity-chemicals"},
        )
        assert resp.status_code == 422

    def test_missing_direction_returns_422(self, client: TestClient) -> None:
        resp = client.get(
            "/bioactivity/evidence_types",
            params={"common_name": "antioxidant"},
        )
        assert resp.status_code == 422


# -- Repository ------------------------------------------------------------


class TestUnknownDirection:
    @pytest.mark.asyncio
    async def test_returns_empty_without_querying(self) -> None:
        session = AsyncMock()
        result = await get_evidence_type_counts(
            session, "antioxidant", "not-a-real-direction", PageFilters()
        )
        assert result["data"] == []
        assert result["metadata"]["row_count"] == 0
        session.execute.assert_not_called()


# -- Every facet route forwards every page filter ---------------------------

_ALL_FILTERS = {
    "search": "querc",
    "filter_evidence_type": "in vitro+in vivo",
    "filter_unit": "uM",
    "filter_source_kind": "experimental",
    "filter_category": "flavonoid",
}


@pytest.mark.parametrize(
    ("path", "fn"),
    [
        ("/bioactivity/endpoints", "get_unit_counts"),
        ("/bioactivity/evidence_types", "get_evidence_type_counts"),
        ("/bioactivity/source_kinds", "get_source_kind_counts"),
        ("/bioactivity/categories", "get_category_counts"),
    ],
)
def test_facet_route_forwards_every_filter(
    client: TestClient, mock_db: AsyncMock, path: str, fn: str
) -> None:
    del mock_db
    with patch(
        f"src.repositories.bioactivity_facets.{fn}", return_value={"data": []}
    ) as mocked:
        resp = client.get(
            path,
            params={
                "common_name": "antioxidant",
                "direction": "bioactivity-chemicals",
                **_ALL_FILTERS,
            },
        )
    assert resp.status_code == 200
    assert mocked.call_args.args[-1] == PageFilters(
        search="querc",
        evidence_type="in vitro+in vivo",
        unit="uM",
        source_kind="experimental",
        category="flavonoid",
    )
