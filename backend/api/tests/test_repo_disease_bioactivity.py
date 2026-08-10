"""Tests for src.repositories.disease_bioactivity."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from src.repositories.disease_bioactivity import (
    _shape_chemical_row,
    get_disease_bioactivities,
    get_disease_bioactivity_chemicals,
)


def _chemical_row(**overrides) -> dict:
    """A fully-populated chemical row as the SQL layer returns it (flat)."""
    row = {
        "bioactivity_name": "anticancer",
        "bioactivity_foodatlas_id": "b1",
        "chemical_name": "quercetin",
        "chemical_foodatlas_id": "c1",
        "n_assays": 5,
        "n_active_measurements": 12,
        "relationships": ["therapeutic"],
        "food_name": "olive",
        "food_foodatlas_id": "f1",
        "food_conc_mg_per_100g": 6124.0,
        "conc_quality_flag": "ok",
        "efficacy_fraction": 1.0,
        "dose_over_ac50_log": 3.97,
        "conc_vs_ac50": "above",
        "logac50": -5.1,
        "n_curves": 2,
        "endpoint_type": "IC50",
        "saturated": True,
    }
    row.update(overrides)
    return row


def _mock_session(rows: list[dict]) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    result.__iter__.return_value = [MagicMock(_mapping=row) for row in rows]
    session.execute.return_value = result
    return session


class TestShapeChemicalRow:
    def test_nests_food_fields_under_dietary(self):
        out = _shape_chemical_row(_chemical_row())
        assert out["dietary"]["food_name"] == "olive"
        assert out["dietary"]["dose_over_ac50_log"] == 3.97
        # Flat food keys must not survive alongside the nested object.
        assert "food_name" not in out
        assert "conc_quality_flag" not in out

    def test_assay_only_row_gets_null_dietary(self):
        """A chemical with no food dose carries no empty food keys at all."""
        out = _shape_chemical_row(
            _chemical_row(
                food_name=None,
                food_foodatlas_id=None,
                food_conc_mg_per_100g=None,
                conc_quality_flag=None,
                efficacy_fraction=None,
                dose_over_ac50_log=None,
                conc_vs_ac50=None,
                logac50=None,
                n_curves=None,
                endpoint_type=None,
                saturated=None,
            )
        )
        assert out["dietary"] is None
        assert out["chemical_name"] == "quercetin"

    def test_keeps_association_fields(self):
        out = _shape_chemical_row(_chemical_row())
        assert out["n_assays"] == 5
        assert out["relationships"] == ["therapeutic"]


class TestGetDiseaseBioactivityChemicals:
    @pytest.mark.asyncio
    async def test_counts_dietary_rows_in_metadata(self):
        session = _mock_session(
            [
                _chemical_row(),
                _chemical_row(chemical_name="vorinostat", dose_over_ac50_log=None),
            ]
        )
        out = await get_disease_bioactivity_chemicals(session, "melanoma")
        assert out["metadata"] == {"row_count": 2, "n_dietary": 1}

    @pytest.mark.asyncio
    async def test_bioactivity_filter_is_parameterised(self):
        """The optional filter must bind, never interpolate the user's string."""
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma", "anticancer")
        params = session.execute.call_args[0][1]
        assert params == {"name": "melanoma", "bioactivity": "anticancer"}
        assert "d.bioactivity_name = :bioactivity" in str(
            session.execute.call_args[0][0]
        )

    @pytest.mark.asyncio
    async def test_omits_filter_clause_when_unset(self):
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma")
        assert session.execute.call_args[0][1] == {"name": "melanoma"}
        assert ":bioactivity" not in str(session.execute.call_args[0][0])

    @pytest.mark.asyncio
    async def test_empty_result(self):
        out = await get_disease_bioactivity_chemicals(_mock_session([]), "nope")
        assert out == {"data": [], "metadata": {"row_count": 0, "n_dietary": 0}}


class TestGetDiseaseBioactivities:
    @pytest.mark.asyncio
    async def test_returns_rows_and_count(self):
        session = _mock_session(
            [
                {
                    "bioactivity_name": "anticancer",
                    "bioactivity_foodatlas_id": "b1",
                    "n_chemicals": 1112,
                    "n_dietary_chemicals": 37,
                    "n_assays": 4675,
                    "n_active_measurements": 4675,
                    "best_dose_over_ac50_log": 4.94,
                }
            ]
        )
        out = await get_disease_bioactivities(session, "melanoma")
        assert out["metadata"] == {"row_count": 1}
        assert out["data"][0]["n_dietary_chemicals"] == 37


# -- routes -----------------------------------------------------------------

_SUMMARY = {"data": [{"bioactivity_name": "anticancer"}], "metadata": {"row_count": 1}}
_CHEMICALS = {"data": [], "metadata": {"row_count": 0, "n_dietary": 0}}


class TestDiseaseBioactivityRoutes:
    def test_bioactivities_returns_data(self, client: TestClient) -> None:
        with patch(
            "src.repositories.disease_bioactivity.get_disease_bioactivities",
            return_value=_SUMMARY,
        ):
            resp = client.get("/disease/bioactivities", params={"common_name": "mel"})
        assert resp.status_code == 200
        assert resp.json()["data"][0]["bioactivity_name"] == "anticancer"

    def test_bioactivities_requires_common_name(self, client: TestClient) -> None:
        assert client.get("/disease/bioactivities").status_code == 422

    def test_chemicals_passes_bioactivity_through(self, client: TestClient) -> None:
        with patch(
            "src.repositories.disease_bioactivity.get_disease_bioactivity_chemicals",
            return_value=_CHEMICALS,
        ) as mock_repo:
            resp = client.get(
                "/disease/bioactivity-chemicals",
                params={"common_name": "melanoma", "bioactivity": "anticancer"},
            )
        assert resp.status_code == 200
        assert mock_repo.call_args[0][1:] == ("melanoma", "anticancer")

    def test_chemicals_bioactivity_is_optional(self, client: TestClient) -> None:
        with patch(
            "src.repositories.disease_bioactivity.get_disease_bioactivity_chemicals",
            return_value=_CHEMICALS,
        ) as mock_repo:
            resp = client.get(
                "/disease/bioactivity-chemicals", params={"common_name": "melanoma"}
            )
        assert resp.status_code == 200
        assert mock_repo.call_args[0][2] is None
