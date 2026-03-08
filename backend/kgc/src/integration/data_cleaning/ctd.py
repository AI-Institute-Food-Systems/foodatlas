"""Clean raw CTD CSV data into parquet files."""

import logging
from pathlib import Path

import pandas as pd

from ...models.settings import KGCSettings

logger = logging.getLogger(__name__)

_CHEMDIS_FILENAME = "CTD_chemicals_diseases.csv"
_DISEASE_FILENAME = "CTD_diseases.csv"
_COLUMNS_WITH_LISTS = [
    "OmimIDs",
    "PubMedIDs",
    "ParentIDs",
    "TreeNumbers",
    "ParentTreeNumbers",
    "Synonyms",
    "AltDiseaseIDs",
    "SlimMappings",
]


def process_ctd(settings: KGCSettings) -> None:
    """Parse raw CTD CSVs and save cleaned parquet files."""
    ctd_dir = Path(settings.data_dir) / "CTD"
    dp_dir = Path(settings.data_cleaning_dir)
    dp_dir.mkdir(parents=True, exist_ok=True)

    chemdis = _load_ctd_csv(ctd_dir / _CHEMDIS_FILENAME)
    chemdis = chemdis[chemdis["DirectEvidence"].notnull()].reset_index(drop=True)
    chemdis.to_parquet(dp_dir / "ctd_chemdis_cleaned.parquet")

    diseases = _load_ctd_csv(ctd_dir / _DISEASE_FILENAME)
    diseases.to_parquet(dp_dir / "ctd_diseases_cleaned.parquet")

    logger.info(
        "Processed CTD: %d chemdis rows, %d diseases.",
        len(chemdis),
        len(diseases),
    )


def _load_ctd_csv(file_path: Path) -> pd.DataFrame:
    """Load a CTD CSV file, parsing the ``# Fields:`` header."""
    with file_path.open() as f:
        lines = f.readlines()
        fields_idx = next(
            i for i, line in enumerate(lines) if line.strip() == "# Fields:"
        )
        header_idx = fields_idx + 1
        header = lines[header_idx].strip().replace("# ", "").split(",")

    df = pd.read_csv(
        file_path,
        comment="#",
        skiprows=range(1, header_idx),
        names=header,
    )
    df = df.dropna(how="all").reset_index(drop=True)
    return _change_content_to_list(df)


def _change_content_to_list(
    df: pd.DataFrame,
    splitby: str = "|",
) -> pd.DataFrame:
    """Split pipe-delimited columns into Python lists."""
    for column in _COLUMNS_WITH_LISTS:
        if column not in df.columns:
            continue
        df[column] = df[column].apply(
            lambda x: x.split(splitby) if pd.notnull(x) else []
        )
        df[column] = df[column].apply(
            lambda x: [int(i) if isinstance(i, str) and i.isdigit() else i for i in x]
        )
    return df
