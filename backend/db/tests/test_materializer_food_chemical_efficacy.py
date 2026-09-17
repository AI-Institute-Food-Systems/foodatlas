"""Tests for src.etl.materializer_food_chemical_efficacy."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.materializer_food_chemical_efficacy import (
    _external_id_map,
    _norm_cid,
    _resolve,
    materialize_food_chemical_efficacy,
)


class TestNormCid:
    def test_int_str_passthrough(self):
        assert _norm_cid("5281224") == "5281224"

    def test_float_str_truncates(self):
        assert _norm_cid("5281224.0") == "5281224"

    def test_number_input(self):
        assert _norm_cid(123) == "123"


class TestExternalIdMap:
    def test_maps_pubchem_cid_to_chemical_id(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "c1",
                    "entity_type": "chemical",
                    "external_ids": {"pubchem_compound": ["5281224.0"]},
                },
                {
                    "foodatlas_id": "d1",
                    "entity_type": "disease",
                    "external_ids": {"pubchem_compound": ["999"]},
                },
            ]
        )
        m = _external_id_map(entities, "chemical", "pubchem_compound")
        assert m == {"5281224": "c1"}

    def test_ignores_non_dict_external_ids(self):
        entities = pd.DataFrame(
            [
                {"foodatlas_id": "c1", "entity_type": "chemical", "external_ids": None},
            ]
        )
        assert _external_id_map(entities, "chemical", "pubchem_compound") == {}


class TestResolve:
    def test_missing_external_ids_key_returns_empty_map(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "c1",
                    "entity_type": "chemical",
                    "external_ids": {"other_key": ["10"]},
                }
            ]
        )
        assert _external_id_map(entities, "chemical", "pubchem_compound") == {}

    def test_first_occurrence_wins(self):
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "c1",
                    "entity_type": "chemical",
                    "external_ids": {"pubchem_compound": ["10"]},
                },
                {
                    "foodatlas_id": "c2",
                    "entity_type": "chemical",
                    "external_ids": {"pubchem_compound": ["10"]},
                },
            ]
        )
        m = _external_id_map(entities, "chemical", "pubchem_compound")
        assert m == {"10": "c1"}


class TestResolveExtra:
    def test_attaches_names_and_drops_unresolved(self):
        efficacy = pd.DataFrame(
            [
                {"foodatlas_id": "f1", "cid": "10", "bioactivity_id": "E300001"},
                {"foodatlas_id": "f_missing", "cid": "10", "bioactivity_id": "E300001"},
                {"foodatlas_id": "f1", "cid": "999", "bioactivity_id": "UNCLASSIFIED"},
            ]
        )
        name_map = {"f1": "onion", "c1": "luteolin", "b1": "antiviral"}
        cid_to_chem = {"10": "c1"}
        native_to_bio = {"E300001": "b1"}
        out = _resolve(efficacy, name_map, cid_to_chem, native_to_bio)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["food_name"] == "onion"
        assert row["chemical_name"] == "luteolin"
        assert row["bioactivity_name"] == "antiviral"
        assert row["bioactivity_id_raw"] == "E300001"


class TestMaterialize:
    @patch("src.etl.materializer_food_chemical_efficacy.bulk_copy")
    @patch("src.etl.materializer_food_chemical_efficacy.pd.read_sql")
    def test_skips_when_empty(self, mock_read, mock_bulk):
        mock_read.return_value = pd.DataFrame()
        materialize_food_chemical_efficacy(MagicMock())
        mock_bulk.assert_not_called()

    @patch("src.etl.materializer_food_chemical_efficacy.bulk_copy")
    @patch("src.etl.materializer_food_chemical_efficacy.pd.read_sql")
    def test_writes_resolved_rows(self, mock_read, mock_bulk):
        efficacy_row = {
            "foodatlas_id": "f1",
            "cid": "10",
            "bioactivity_id": "E300001",
            "food_conc_mg_per_100g": 1.0,
            "food_conc_mass_fraction_pct": 0.001,
            "conc_quality_flag": "ok",
            "molecular_weight": 100.0,
            "food_conc_m": 1e-5,
            "food_conc_logm": -5.0,
            "rep_source_assay_id": "A1",
            "endpoint_type": "IC50",
            "endpoint_class": "potency",
            "curve_method": "4-point",
            "logac50": -5.0,
            "hillslope": 1.0,
            "zeroactivity": 0.0,
            "infiniteactivity": 100.0,
            "n_curves": 1,
            "n_curves_4param": 1,
            "curve_agreement": "single",
            "ac50_spread_log": 0.0,
            "logac50_median": -5.0,
            "logac50_min": -5.0,
            "logac50_max": -5.0,
            "dose_over_ac50_log": 0.0,
            "conc_vs_ac50": "above",
            "efficacy_fraction": 0.5,
            "efficacy_response": 50.0,
            "saturated": False,
        }
        entities = pd.DataFrame(
            [
                {
                    "foodatlas_id": "f1",
                    "entity_type": "food",
                    "common_name": "onion",
                    "external_ids": {},
                },
                {
                    "foodatlas_id": "c1",
                    "entity_type": "chemical",
                    "common_name": "luteolin",
                    "external_ids": {"pubchem_compound": ["10"]},
                },
                {
                    "foodatlas_id": "b1",
                    "entity_type": "bioactivity",
                    "common_name": "antiviral",
                    "external_ids": {"bioactivity_concept": ["E300001"]},
                },
            ]
        )
        mock_read.side_effect = [pd.DataFrame([efficacy_row]), entities]
        materialize_food_chemical_efficacy(MagicMock())
        mock_bulk.assert_called_once()
        args = mock_bulk.call_args.args
        assert args[1] == "mv_food_chemical_efficacy"
        df = args[2]
        assert len(df) == 1
        assert df.iloc[0]["chemical_name"] == "luteolin"
