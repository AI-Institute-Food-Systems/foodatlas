"""Tests for the chemical concentration orchestrator."""

import numpy as np
import pandas as pd
from src.preprocessing.chemical_conc import standardize_chemical_conc


class TestStandardizeChemicalConc:
    """Test the end-to-end orchestrator."""

    def test_basic_flow(self) -> None:
        df = pd.DataFrame({"_conc": ["5mg/100g", "10ug/g"]})
        result = standardize_chemical_conc(df)
        assert "conc_value" in result.columns
        assert "conc_unit" in result.columns
        assert result["conc_value"].iloc[0] == 5.0
        assert result["conc_unit"].iloc[0] == "mg/100g"

    def test_empty_conc_becomes_nan(self) -> None:
        df = pd.DataFrame({"_conc": [""]})
        result = standardize_chemical_conc(df)
        assert pd.isna(result["_conc_value"].iloc[0])

    def test_mg_100ml_converted(self) -> None:
        df = pd.DataFrame({"_conc": ["5mg/100ml"]})
        result = standardize_chemical_conc(df)
        assert result["conc_unit"].iloc[0] == "mg/100g (converted)"

    def test_excessive_value_removed(self) -> None:
        df = pd.DataFrame({"_conc": ["200000mg/100g"]})
        result = standardize_chemical_conc(df)
        assert pd.isna(result["conc_value"].iloc[0])

    def test_weight_type_parsed(self) -> None:
        df = pd.DataFrame({"_conc": ["5mg/100gfw"]})
        result = standardize_chemical_conc(df)
        assert result["_conc_weight_type"].iloc[0] == "fresh"

    def test_nan_conc(self) -> None:
        df = pd.DataFrame({"_conc": [np.nan]})
        result = standardize_chemical_conc(df)
        assert pd.isna(result["conc_value"].iloc[0])
