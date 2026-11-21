"""The association endpoints return each pair's assay groups, targets labelled."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from src.repositories.bioactivity_disease import get_chemical_disease_associations

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

GROUPS = [
    {
        "activities": ["anticancer"],
        "relationships": ["therapeutic"],
        "n_assays": 2,
        "assays": ["AID: 1", "AID: 2"],
        "target_genes": ["NCBIGene: 1"],
    },
    {
        "activities": [],
        "relationships": ["marker/mechanism"],
        "n_assays": 1,
        "assays": ["AID: 3"],
        "target_genes": ["NCBIGene: 2"],
    },
]


async def test_groups_come_back_with_labelled_targets(pg_session: AsyncSession) -> None:
    await pg_session.execute(
        text(
            "INSERT INTO mv_chemical_disease_bioactivity (chemical_name,"
            " chemical_foodatlas_id, disease_name, disease_foodatlas_id, n_assays,"
            " relationships, target_genes, assays, assay_groups) VALUES"
            " ('quercetin', 'c1', 'asthma', 'd1', 3,"
            " ARRAY['marker/mechanism', 'therapeutic'],"
            " ARRAY['NCBIGene: 1', 'NCBIGene: 2'], ARRAY['AID: 1'],"
            " CAST(:g AS JSONB))"
        ),
        {"g": json.dumps(GROUPS)},
    )
    await pg_session.execute(
        text(
            "INSERT INTO mv_assay_target_labels (gene_id, label) VALUES"
            " ('NCBIGene: 1', 'EGFR')"
        )
    )
    await pg_session.commit()
    payload = await get_chemical_disease_associations(pg_session, "quercetin")
    data = payload["data"]
    assert isinstance(data, list)
    groups = data[0]["assay_groups"]
    assert [g["n_assays"] for g in groups] == [2, 1]
    assert groups[0]["targets"] == [{"id": "NCBIGene: 1", "label": "EGFR"}]
    assert groups[1]["targets"] == [{"id": "NCBIGene: 2", "label": None}]
