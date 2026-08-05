"""Tests for /v1/ routes (HTTP layer)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

# -- /v1/foods --------------------------------------------------------------


class TestListFoods:
    def test_returns_paginated_envelope(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.list_entities",
            return_value=(
                [
                    {
                        "id": "FA:0001",
                        "common_name": "apple",
                        "scientific_name": "",
                        "food_classification": [],
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["id"] == "FA:0001"
        assert body["page"] == {
            "page": 1,
            "page_size": 50,
            "total": 1,
            "has_more": False,
        }

    def test_clamps_page_size(self, client: TestClient) -> None:
        # FastAPI Query validation rejects > 100; this confirms the bound.
        resp = client.get("/v1/foods?page_size=500")
        assert resp.status_code == 422


class TestGetFood:
    def test_returns_food(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value={
                "id": "FA:0001",
                "common_name": "apple",
                "scientific_name": "",
                "synonyms": [],
                "external_ids": {},
                "food_classification": [],
            },
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods/FA:0001")
        assert resp.status_code == 200
        assert resp.json()["data"]["common_name"] == "apple"

    def test_404_when_missing(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods/FA:NOPE")
        assert resp.status_code == 404


class TestFoodChemicals:
    def test_returns_composition_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_composition",
            return_value=(
                [
                    {
                        "food_id": "FA:0001",
                        "food_name": "apple",
                        "chemical_id": "FA:C001",
                        "chemical_name": "glucose",
                        "chemical_classification": [],
                        "median_concentration": None,
                        "attestation_count": 0,
                        "sources": [],
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods/FA:0001/chemicals")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["chemical_id"] == "FA:C001"


# -- /v1/bioactivities ------------------------------------------------------


class TestListBioactivities:
    def test_returns_paginated_envelope(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.list_entities",
            return_value=(
                [
                    {
                        "id": "FA:B001",
                        "common_name": "antioxidant",
                        "description": "",
                        "n_foods": 3,
                        "n_chemicals": 7,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities?q=anti")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["id"] == "FA:B001"
        assert body["data"][0]["n_chemicals"] == 7


class TestGetBioactivity:
    def test_returns_bioactivity(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value={
                "id": "FA:B001",
                "common_name": "antioxidant",
                "description": "Reduces oxidative stress.",
                "n_foods": 3,
                "n_chemicals": 7,
                "synonyms": ["antioxidative"],
                "external_ids": {},
                "parents": [{"foodatlas_id": "FA:B000", "common_name": "bioactivity"}],
                "children": [],
            },
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/FA:B001")
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["parents"][0]["common_name"] == "bioactivity"

    def test_404_when_missing(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/FA:NOPE")
        assert resp.status_code == 404


class TestBioactivityChemicals:
    def test_returns_chemical_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_bioactivity_chemicals",
            return_value=(
                [
                    {
                        "bioactivity_id": "FA:B001",
                        "bioactivity_name": "antioxidant",
                        "chemical_id": "FA:C001",
                        "chemical_name": "quercetin",
                        "measurement_count": 755,
                        "active_count": 83,
                        "inactive_count": 261,
                        "top_measurement": {
                            "endpoint": "IC50",
                            "value": 17.175,
                            "unit": "MICROMOLAR",
                        },
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/FA:B001/chemicals")
        assert resp.status_code == 200
        row = resp.json()["data"][0]
        assert row["chemical_name"] == "quercetin"
        assert row["top_measurement"]["endpoint"] == "IC50"


class TestBioactivityFoods:
    def test_returns_food_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_bioactivity_foods",
            return_value=(
                [
                    {
                        "bioactivity_id": "FA:B001",
                        "bioactivity_name": "antioxidant",
                        "food_id": "FA:0001",
                        "food_name": "snail",
                        "measurement_count": 1,
                        "top_measurement": {
                            "endpoint": "Activity",
                            "value": 0.519,
                            "unit": "mmol/100g",
                        },
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/FA:B001/foods")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["food_name"] == "snail"


class TestFoodBioactivities:
    def test_returns_food_bioactivity_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_bioactivity_foods",
            return_value=(
                [
                    {
                        "bioactivity_id": "FA:B001",
                        "bioactivity_name": "antioxidant",
                        "food_id": "FA:0001",
                        "food_name": "snail",
                        "measurement_count": 1,
                        "top_measurement": None,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods/FA:0001/bioactivities")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["bioactivity_id"] == "FA:B001"


class TestChemicalBioactivities:
    def test_returns_chemical_bioactivity_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_bioactivity_chemicals",
            return_value=(
                [
                    {
                        "bioactivity_id": "FA:B001",
                        "bioactivity_name": "antioxidant",
                        "chemical_id": "FA:C001",
                        "chemical_name": "quercetin",
                        "measurement_count": 12,
                        "active_count": 2,
                        "inactive_count": 3,
                        "top_measurement": None,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/chemicals/FA:C001/bioactivities")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["bioactivity_id"] == "FA:B001"


# -- /v1/chemicals ----------------------------------------------------------


class TestChemicalDiseases:
    def test_relation_required_to_be_valid(self, client: TestClient) -> None:
        resp = client.get("/v1/chemicals/FA:C001/diseases?relation=bogus")
        assert resp.status_code == 422

    def test_returns_correlation_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.relationships.list_correlation",
            return_value=(
                [
                    {
                        "chemical_id": "FA:C001",
                        "chemical_name": "glucose",
                        "disease_id": "FA:D001",
                        "disease_name": "diabetes",
                        "relation": "worsens",
                        "source_chemical_id": "",
                        "source_chemical_name": "",
                        "sources": [],
                        "evidence_count": 1,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/chemicals/FA:C001/diseases?relation=worsens")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["relation"] == "worsens"


# -- /v1/triplets -----------------------------------------------------------


class TestListTriplets:
    def test_returns_triplet_list(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.triplets.list_triplets",
            return_value=(
                [
                    {
                        "triplet_id": 1,
                        "head_id": "FA:0001",
                        "head_name": "apple",
                        "relationship_id": "r1",
                        "relationship_name": "contains",
                        "tail_id": "FA:C001",
                        "tail_name": "glucose",
                        "source": "",
                        "attestations": [],
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/triplets?relationship=contains")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["triplet_id"] == 1


class TestGetTriplet:
    def test_404_when_missing(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.triplets.get_triplet",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/triplets/9999")
        assert resp.status_code == 404


# -- /v1/attestations -------------------------------------------------------


class TestGetAttestation:
    def test_returns_attestation(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.triplets.get_attestation",
            return_value={
                "attestation_id": "att1",
                "source": "fdc",
                "evidence_id": "ev1",
                "head_id": "FA:0001",
                "head_name_raw": "apple",
                "tail_id": "FA:C001",
                "tail_name_raw": "glucose",
                "relationship_id": "r1",
                "conc_value": 5.0,
                "conc_unit": "mg/100g",
                "food_part": "fruit",
                "food_processing": "",
                "validated": False,
                "validated_correct": True,
                "trust_score": None,
                "trust_reason": "",
            },
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/attestations/att1")
        assert resp.status_code == 200
        assert resp.json()["data"]["attestation_id"] == "att1"

    def test_404_when_missing(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.triplets.get_attestation",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/attestations/missing")
        assert resp.status_code == 404


# -- /v1/search -------------------------------------------------------------


class TestSearch:
    def test_q_required(self, client: TestClient) -> None:
        resp = client.get("/v1/search")
        assert resp.status_code == 422

    def test_returns_hits(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.search.search",
            return_value=(
                [
                    {
                        "id": "FA:0001",
                        "common_name": "apple",
                        "entity_type": "food",
                        "scientific_name": "Malus",
                        "associations": 42,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/search?q=apple")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["common_name"] == "apple"

    def test_invalid_entity_type_rejected(self, client: TestClient) -> None:
        resp = client.get("/v1/search?q=apple&entity_type=bogus")
        assert resp.status_code == 422


# -- /v1/stats --------------------------------------------------------------


class TestStats:
    def test_returns_counts(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.search.get_stats",
            return_value={
                "foods": 100,
                "chemicals": 50,
                "diseases": 25,
                "publications": 200,
                "connections": 1000,
            },
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/stats")
        assert resp.status_code == 200
        assert resp.json()["data"]["foods"] == 100


# -- /v1/bundles ------------------------------------------------------------


class TestBundles:
    def test_empty_when_no_bucket(self, client: TestClient) -> None:
        resp = client.get("/v1/bundles")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"] == []
        assert body["page"]["total"] == 0

    def test_lists_bundles(self, client_with_downloads_bucket: TestClient) -> None:
        with patch(
            "src.repositories.downloads.fetch_manifest",
            return_value=[
                {
                    "version": "v1.0",
                    "release_date": "2026-04-20",
                    "file_size": "1 GB",
                    "kgc_run": "x",
                    "download_link": "https://example/foodatlas-v1.0.zip",
                    "summary_link": "",
                }
            ],
            new_callable=AsyncMock,
        ):
            resp = client_with_downloads_bucket.get("/v1/bundles")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["version"] == "v1.0"


# -- /v1/diseases -----------------------------------------------------------


class TestDiseaseEndpoints:
    def test_get_disease_404(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/diseases/FA:NOPE")
        assert resp.status_code == 404

    def test_disease_chemicals_relation_validated(self, client: TestClient) -> None:
        resp = client.get("/v1/diseases/FA:D001/chemicals?relation=bogus")
        assert resp.status_code == 422


# -- OpenAPI security scheme ------------------------------------------------


class TestOpenAPISchema:
    def test_security_scheme_registered(self, client: TestClient) -> None:
        resp = client.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["components"]["securitySchemes"]["bearerAuth"] == {
            "type": "http",
            "scheme": "bearer",
        }

    def test_v1_routes_carry_security(self, client: TestClient) -> None:
        schema = client.get("/openapi.json").json()
        v1_paths = [p for p in schema["paths"] if p.startswith("/v1/")]
        assert v1_paths
        for path in v1_paths:
            for op in schema["paths"][path].values():
                if isinstance(op, dict):
                    assert op.get("security") == [{"bearerAuth": []}]
