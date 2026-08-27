"""Tests for src.etl.materializer_assay_target_labels."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_assay_target_labels import (
    _modal_labels,
    materialize_assay_target_labels,
)

_MODULE = "src.etl.materializer_assay_target_labels"


def _assay(entrez: str = "", uniprot: str = "", name: str = "") -> dict:
    return {
        "target_entrez_gene": entrez,
        "target_uniprot": uniprot,
        "target_name": name,
    }


class TestModalLabels:
    def test_picks_the_most_common_spelling(self):
        rows = [_assay(entrez="NCBIGene: 4780", name="Nrf2") for _ in range(3)]
        rows.append(_assay(entrez="NCBIGene: 4780", name="Keap1/Nrf2"))
        out = _modal_labels(pd.DataFrame(rows))
        assert out.to_dict("records") == [
            {"gene_id": "NCBIGene: 4780", "label": "Nrf2"}
        ]

    def test_entrez_and_uniprot_ids_both_get_a_label(self):
        out = _modal_labels(
            pd.DataFrame(
                [
                    _assay(
                        entrez="NCBIGene: 4780",
                        uniprot="UniProt: Q16236",
                        name="Nrf2",
                    )
                ]
            )
        )
        assert dict(zip(out["gene_id"], out["label"], strict=True)) == {
            "NCBIGene: 4780": "Nrf2",
            "UniProt: Q16236": "Nrf2",
        }

    def test_blank_ids_are_dropped(self):
        """Most assays carry only one of the two id kinds; the other is ''."""
        out = _modal_labels(pd.DataFrame([_assay(uniprot="UniProt: P1", name="RNase")]))
        assert out["gene_id"].tolist() == ["UniProt: P1"]

    def test_ties_break_to_the_shorter_name(self):
        out = _modal_labels(
            pd.DataFrame(
                [
                    _assay(entrez="NCBIGene: 1", name="Histone deacetylase 6"),
                    _assay(
                        entrez="NCBIGene: 1",
                        name="histone deacetylase 6 (3.5.1.-)",
                    ),
                ]
            )
        )
        assert out["label"].tolist() == ["Histone deacetylase 6"]

    def test_no_usable_ids_returns_empty_frame(self):
        out = _modal_labels(pd.DataFrame([_assay(name="orphan target")]))
        assert out.empty
        assert list(out.columns) == ["gene_id", "label"]


class TestMaterializeAssayTargetLabels:
    def test_skips_when_no_assays_have_names(self):
        empty = pd.DataFrame(
            columns=["target_entrez_gene", "target_uniprot", "target_name"]
        )
        with (
            patch(f"{_MODULE}.pd.read_sql", return_value=empty),
            patch(f"{_MODULE}.bulk_copy") as copy,
        ):
            materialize_assay_target_labels(MagicMock())
        copy.assert_not_called()

    def test_skips_when_names_exist_but_no_ids_do(self):
        with (
            patch(
                f"{_MODULE}.pd.read_sql",
                return_value=pd.DataFrame([_assay(name="orphan target")]),
            ),
            patch(f"{_MODULE}.bulk_copy") as copy,
        ):
            materialize_assay_target_labels(MagicMock())
        copy.assert_not_called()

    def test_writes_labels(self):
        with (
            patch(
                f"{_MODULE}.pd.read_sql",
                return_value=pd.DataFrame(
                    [_assay(entrez="NCBIGene: 1", name="Kinase")]
                ),
            ),
            patch(f"{_MODULE}.bulk_copy") as copy,
        ):
            materialize_assay_target_labels(MagicMock())
        assert copy.call_args[0][1] == "mv_assay_target_labels"
        assert copy.call_args[0][3] == ["gene_id", "label"]
