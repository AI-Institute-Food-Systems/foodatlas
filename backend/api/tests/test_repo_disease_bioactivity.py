"""Tests for src.repositories.disease_bioactivity."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from src.repositories.disease_bioactivity import (
    get_bioactivity_diseases,
    get_disease_bioactivities,
    get_disease_bioactivity_chemicals,
)


def _chemical_row(**overrides) -> dict:
    row = {
        "bioactivity_name": "anticancer",
        "bioactivity_foodatlas_id": "b1",
        "chemical_name": "quercetin",
        "chemical_foodatlas_id": "c1",
        "n_assays": 5,
        "n_active_measurements": 12,
        "relationships": ["therapeutic"],
        "target_genes": ["NCBIGene: 7157"],
        "assays": ["AID: 1"],
        "literature_directions": [],
    }
    row.update(overrides)
    return row


def _mock_session(rows: list[dict], labels: list[dict] | None = None) -> AsyncMock:
    """A session whose first execute returns rows and whose second returns labels.

    The repositories label target genes with a follow-up query, so anything
    calling attach_targets consumes two results.
    """
    session = AsyncMock()
    main = MagicMock()
    main.__iter__.return_value = [MagicMock(_mapping=row) for row in rows]
    label_result = MagicMock()
    label_result.__iter__.return_value = [
        MagicMock(_mapping=label) for label in labels or []
    ]
    session.execute.side_effect = [main, label_result]
    return session


class TestGetDiseaseBioactivityChemicals:
    @pytest.mark.asyncio
    async def test_returns_rows_and_count(self):
        session = _mock_session([_chemical_row(), _chemical_row(n_assays=1)])
        out = await get_disease_bioactivity_chemicals(session, "melanoma")
        assert out["metadata"] == {"row_count": 2}
        assert out["data"][0]["chemical_name"] == "quercetin"

    @pytest.mark.asyncio
    async def test_carries_no_efficacy_fields(self):
        """The food-dose columns were deliberately dropped; keep them gone.

        They implied a precision the density-1 proxy doesn't support, so a
        regression that quietly re-adds them should fail here.
        """
        out = await get_disease_bioactivity_chemicals(
            _mock_session([_chemical_row()]), "melanoma"
        )
        row = out["data"][0]
        banned_fields = (
            "dietary",
            "food_name",
            "efficacy_fraction",
            "dose_over_ac50_log",
        )
        for banned in banned_fields:
            assert banned not in row

    @pytest.mark.asyncio
    async def test_does_not_join_efficacy_view(self):
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma")
        sql = str(session.execute.call_args[0][0])
        assert "mv_food_chemical_efficacy" not in sql

    @pytest.mark.asyncio
    async def test_orders_by_assay_count(self):
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma")
        assert "ORDER BY n_assays DESC" in str(session.execute.call_args[0][0])

    @pytest.mark.asyncio
    async def test_bioactivity_filter_is_parameterised(self):
        """The optional filter must bind, never interpolate the user's string."""
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma", "anticancer")
        params = session.execute.call_args[0][1]
        assert params == {"name": "melanoma", "bioactivity": "anticancer"}
        assert "bioactivity_name = :bioactivity" in str(session.execute.call_args[0][0])

    @pytest.mark.asyncio
    async def test_omits_filter_clause_when_unset(self):
        session = _mock_session([])
        await get_disease_bioactivity_chemicals(session, "melanoma")
        assert session.execute.call_args[0][1] == {"name": "melanoma"}
        assert ":bioactivity" not in str(session.execute.call_args[0][0])

    @pytest.mark.asyncio
    async def test_empty_result(self):
        out = await get_disease_bioactivity_chemicals(_mock_session([]), "nope")
        assert out == {"data": [], "metadata": {"row_count": 0}}


class TestGetDiseaseBioactivities:
    @pytest.mark.asyncio
    async def test_returns_rows_and_count(self):
        session = _mock_session(
            [
                {
                    "bioactivity_name": "anticancer",
                    "bioactivity_foodatlas_id": "b1",
                    "n_chemicals": 1112,
                    "n_assays": 4675,
                    "n_active_measurements": 4675,
                }
            ]
        )
        out = await get_disease_bioactivities(session, "melanoma")
        assert out["metadata"] == {"row_count": 1}
        assert out["data"][0]["n_chemicals"] == 1112

    @pytest.mark.asyncio
    async def test_summary_does_not_join_efficacy_view(self):
        session = _mock_session([])
        await get_disease_bioactivities(session, "melanoma")
        assert "mv_food_chemical_efficacy" not in str(session.execute.call_args[0][0])


class TestGetBioactivityDiseases:
    """The mirror direction, powering the Diseases tab on bioactivity pages."""

    @pytest.mark.asyncio
    async def test_returns_rows_and_count(self):
        session = _mock_session(
            [
                {
                    "disease_name": "carcinoma, hepatocellular",
                    "disease_foodatlas_id": "d1",
                    "n_chemicals": 2038,
                    "n_assays": 8275,
                    "n_active_measurements": 8275,
                }
            ]
        )
        out = await get_bioactivity_diseases(session, "anticancer")
        assert out["metadata"] == {"row_count": 1}
        assert out["data"][0]["disease_name"] == "carcinoma, hepatocellular"

    @pytest.mark.asyncio
    async def test_filters_on_bioactivity_and_groups_by_disease(self):
        session = _mock_session([])
        await get_bioactivity_diseases(session, "anticancer")
        sql = str(session.execute.call_args[0][0])
        assert "WHERE bioactivity_name = :name" in sql
        assert "GROUP BY s.disease_name" in sql
        assert session.execute.call_args[0][1] == {"name": "anticancer"}

    @pytest.mark.asyncio
    async def test_reads_only_the_assay_attributed_view(self):
        """Must not fall back to the loose chemical→disease view."""
        session = _mock_session([])
        await get_bioactivity_diseases(session, "anticancer")
        sql = str(session.execute.call_args[0][0])
        assert "mv_disease_bioactivity" in sql
        assert "mv_chemical_disease_bioactivity" not in sql

    @pytest.mark.asyncio
    async def test_empty_result(self):
        out = await get_bioactivity_diseases(_mock_session([]), "nope")
        assert out == {"data": [], "metadata": {"row_count": 0}}

    @pytest.mark.asyncio
    async def test_reports_the_direction_split(self):
        """The tab's whole point: how much of the evidence is therapeutic.

        A disease can be reached by a thousand chemicals and still have almost
        no therapeutic evidence behind it, which a bare total hides.
        """
        session = _mock_session([])
        await get_bioactivity_diseases(session, "anticancer")
        sql = str(session.execute.call_args[0][0])
        assert "FILTER (WHERE 'therapeutic' = ANY(relationships))" in sql
        assert "FILTER (WHERE 'marker/mechanism' = ANY(relationships))" in sql
        assert "cardinality(literature_directions) > 0" in sql

    @pytest.mark.asyncio
    async def test_ranks_shared_targets_by_chemical_count(self):
        """Top targets, not the union — a union over 2,000 chemicals is noise."""
        session = _mock_session([])
        sql_before = await get_bioactivity_diseases(session, "anticancer")
        assert sql_before["metadata"]["row_count"] == 0
        sql = str(session.execute.call_args[0][0])
        assert "ORDER BY n_chemicals DESC" in sql
        assert "[1:12]" in sql


class TestTargetLabelling:
    """Gene ids are paired with readable names, best-effort."""

    @pytest.mark.asyncio
    async def test_labels_are_zipped_onto_ids(self):
        session = _mock_session(
            [_chemical_row(target_genes=["NCBIGene: 7157", "NCBIGene: 999"])],
            labels=[{"gene_id": "NCBIGene: 7157", "label": "Cellular tumor antigen p53"}],
        )
        out = await get_disease_bioactivity_chemicals(session, "melanoma")
        assert out["data"][0]["targets"] == [
            {"id": "NCBIGene: 7157", "label": "Cellular tumor antigen p53"},
            # Unlabelled genes still appear; the UI falls back to the id.
            {"id": "NCBIGene: 999", "label": None},
        ]

    @pytest.mark.asyncio
    async def test_entrez_and_uniprot_forms_collapse_to_one_target(self):
        """p53 arrives under both ids; showing it twice wastes a slot."""
        session = _mock_session(
            [_chemical_row(target_genes=["NCBIGene: 7157", "UniProt: P04637"])],
            labels=[
                {"gene_id": "NCBIGene: 7157", "label": "Cellular tumor antigen p53"},
                {"gene_id": "UniProt: P04637", "label": "Cellular tumor antigen p53"},
            ],
        )
        out = await get_disease_bioactivity_chemicals(session, "melanoma")
        assert out["data"][0]["targets"] == [
            {"id": "NCBIGene: 7157", "label": "Cellular tumor antigen p53"}
        ]

    @pytest.mark.asyncio
    async def test_missing_label_table_does_not_lose_rows(self):
        """A later-added lookup must not take the associations down with it."""
        session = AsyncMock()
        main = MagicMock()
        main.__iter__.return_value = [MagicMock(_mapping=_chemical_row())]
        session.execute.side_effect = [main, SQLAlchemyError("no such table")]
        out = await get_disease_bioactivity_chemicals(session, "melanoma")
        assert out["metadata"] == {"row_count": 1}
        assert out["data"][0]["targets"] == [{"id": "NCBIGene: 7157", "label": None}]


# -- routes -----------------------------------------------------------------

_SUMMARY = {"data": [{"bioactivity_name": "anticancer"}], "metadata": {"row_count": 1}}
_CHEMICALS: dict[str, object] = {"data": [], "metadata": {"row_count": 0}}


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

    def test_bioactivity_diseases_route(self, client: TestClient) -> None:
        """The mirror route lives under /bioactivity, not /disease."""
        payload = {
            "data": [{"disease_name": "carcinoma, hepatocellular"}],
            "metadata": {"row_count": 1},
        }
        with patch(
            "src.repositories.disease_bioactivity.get_bioactivity_diseases",
            return_value=payload,
        ) as mock_repo:
            resp = client.get(
                "/bioactivity/diseases", params={"common_name": "anticancer"}
            )
        assert resp.status_code == 200
        assert resp.json()["data"][0]["disease_name"] == "carcinoma, hepatocellular"
        assert mock_repo.call_args[0][1] == "anticancer"

    def test_bioactivity_diseases_requires_common_name(
        self, client: TestClient
    ) -> None:
        assert client.get("/bioactivity/diseases").status_code == 422
