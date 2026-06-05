"""Tests for /v1/bioactivities routes and the cross-entity sub-routes."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


# -- /v1/bioactivities ------------------------------------------------------


class TestListBioactivities:
    def test_returns_paginated_envelope(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.list_entities",
            return_value=(
                [
                    {
                        "id": "bio1",
                        "common_name": "anti-inflammatory",
                        "scientific_name": "",
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities")
        assert resp.status_code == 200
        body = resp.json()
        assert body["data"][0]["id"] == "bio1"
        assert body["page"]["total"] == 1


class TestGetBioactivity:
    def test_returns_detail(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value={
                "id": "bio1",
                "common_name": "anti-inflammatory",
                "scientific_name": "",
                "synonyms": [],
                "external_ids": {},
                "description": "reduces inflammation",
            },
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/bio1")
        assert resp.status_code == 200
        assert resp.json()["data"]["description"] == "reduces inflammation"

    def test_404_when_missing(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.entities.get_entity",
            return_value=None,
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/missing")
        assert resp.status_code == 404


class TestBioactivityChemicals:
    def test_returns_measurement_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_measurements",
            return_value=(
                [
                    {
                        "chemical_id": "c1",
                        "chemical_name": "quercetin",
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "anti-inflammatory",
                        "measurement_count": 1,
                        "potency": {"value": 5.0, "unit": "uM"},
                        "hill_curve": {
                            "zero_activity": 0.5,
                            "infinite_activity": -50.0,
                            "log_ac50": -5.0,
                            "hill_slope": 1.0,
                        },
                        "target_ids": ["UniProt:P1"],
                        "evidence_source": "PubChem AID:1",
                        "evidence_type": "In vitro",
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/bio1/chemicals")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["chemical_name"] == "quercetin"


class TestBioactivityFoods:
    def test_returns_exhibit_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_exhibits",
            return_value=(
                [
                    {
                        "food_id": "f1",
                        "food_name": "strawberry",
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "anti-inflammatory",
                        "exhibit_type": "direct",
                        "via_chemical_id": None,
                        "via_chemical_name": None,
                        "efficacy_pred": None,
                        "evidence_count": 1,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/bio1/foods?exhibit_type=direct")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["exhibit_type"] == "direct"

    def test_rejects_invalid_exhibit_type(self, client: TestClient) -> None:
        resp = client.get("/v1/bioactivities/bio1/foods?exhibit_type=bogus")
        assert resp.status_code == 422


class TestBioactivityDiseases:
    def test_returns_association_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_associations",
            return_value=(
                [
                    {
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "anti-inflammatory",
                        "disease_id": "d1",
                        "disease_name": "asthma",
                        "polarity": None,
                        "target_ids": ["UniProt:P1"],
                        "evidence_count": 1,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/bioactivities/bio1/diseases")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["disease_name"] == "asthma"


# -- Cross-entity sub-routes ------------------------------------------------


class TestChemicalBioactivities:
    def test_returns_measurement_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_measurements",
            return_value=(
                [
                    {
                        "chemical_id": "c1",
                        "chemical_name": "quercetin",
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "anti-inflammatory",
                        "measurement_count": 1,
                        "potency": {"value": 5.0, "unit": "uM"},
                        "hill_curve": {},
                        "target_ids": [],
                        "evidence_source": None,
                        "evidence_type": None,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/chemicals/c1/bioactivities")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["bioactivity_name"] == "anti-inflammatory"


class TestFoodBioactivities:
    def test_returns_exhibit_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_exhibits",
            return_value=(
                [
                    {
                        "food_id": "f1",
                        "food_name": "strawberry",
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "antioxidant",
                        "exhibit_type": "inherited",
                        "via_chemical_id": "c1",
                        "via_chemical_name": "quercetin",
                        "efficacy_pred": None,
                        "evidence_count": 2,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/foods/f1/bioactivities?exhibit_type=inherited")
        assert resp.status_code == 200
        row = resp.json()["data"][0]
        assert row["exhibit_type"] == "inherited"
        assert row["via_chemical_name"] == "quercetin"

    def test_rejects_invalid_exhibit_type(self, client: TestClient) -> None:
        resp = client.get("/v1/foods/f1/bioactivities?exhibit_type=bogus")
        assert resp.status_code == 422


class TestDiseaseBioactivities:
    def test_returns_association_rows(self, client: TestClient) -> None:
        with patch(
            "src.repositories.v1.bioactivity.list_associations",
            return_value=(
                [
                    {
                        "bioactivity_id": "bio1",
                        "bioactivity_name": "anti-inflammatory",
                        "disease_id": "d1",
                        "disease_name": "asthma",
                        "polarity": None,
                        "target_ids": [],
                        "evidence_count": 0,
                    }
                ],
                1,
            ),
            new_callable=AsyncMock,
        ):
            resp = client.get("/v1/diseases/d1/bioactivities")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["disease_name"] == "asthma"
