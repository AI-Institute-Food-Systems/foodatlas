"""Chemical concentration standardization — orchestrator."""

from __future__ import annotations

import numpy as np
import pandas as pd
from tqdm import tqdm

from .conc_converter import check_conc_value, convert_conc_unit
from .conc_parser import parse_conc_string

tqdm.pandas()


def standardize_chemical_conc(metadata: pd.DataFrame) -> pd.DataFrame:
    """Standardize chemical concentration strings in a metadata DataFrame.

    Parses raw concentration strings, converts units to standard forms,
    replaces mg/100ml with mg/100g (converted), and removes obviously
    wrong values (>1e5 mg/100g).

    Args:
        metadata: DataFrame with a ``_conc`` column containing raw
            concentration strings.

    Returns:
        The DataFrame with added columns: ``_conc_value``, ``_conc_unit``,
        ``_conc_weight_type``, ``conc_value``, ``conc_unit``.

    """
    metadata["_conc"] = metadata["_conc"].replace("", np.nan)

    metadata[["_conc_value", "_conc_unit", "_conc_weight_type"]] = (
        metadata["_conc"].progress_apply(parse_conc_string).apply(pd.Series)
    )

    def _apply_convert(row: pd.Series) -> pd.Series:
        return pd.Series(convert_conc_unit(row["_conc_value"], row["_conc_unit"]))

    metadata[["conc_value", "conc_unit"]] = metadata[
        ["_conc_value", "_conc_unit"]
    ].progress_apply(_apply_convert, axis=1)

    metadata["conc_unit"] = metadata["conc_unit"].str.replace(
        "mg/100ml",
        "mg/100g (converted)",
    )

    def _apply_check(row: pd.Series) -> pd.Series:
        return pd.Series(check_conc_value(row["conc_value"], row["conc_unit"]))

    metadata[["conc_value", "conc_unit"]] = metadata[["conc_value", "conc_unit"]].apply(
        _apply_check, axis=1
    )

    return metadata
