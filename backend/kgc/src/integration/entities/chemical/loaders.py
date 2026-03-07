"""Loaders for chemical data: ChEBI, CDNO, FDC nutrients, MeSH, PubChem."""

from collections import OrderedDict
from pathlib import Path

import pandas as pd

from ....models.settings import KGCSettings

# -- ChEBI ------------------------------------------------------------


def load_mapper_name_to_chebi_id(settings: KGCSettings) -> pd.DataFrame:
    """Load name -> ChEBI ID mapper from preprocessed data."""
    dp_dir = Path(settings.integration_dir)
    name2chebi: pd.DataFrame = pd.read_parquet(
        dp_dir / "chebi_name_to_id.parquet",
    )
    names_remove = ["ash"]
    return name2chebi[~name2chebi["NAME"].isin(names_remove)].copy()


def load_mapper_chebi_id_to_names(settings: KGCSettings) -> pd.DataFrame:
    """Load ChEBI ID -> list of names mapper (inverse of name->id)."""
    name2chebi = load_mapper_name_to_chebi_id(settings).set_index("NAME")

    mapper: dict[int, list[str]] = {}
    for name, row in name2chebi.iterrows():
        for chebi_id in row["CHEBI_ID"]:
            if chebi_id not in mapper:
                mapper[chebi_id] = []
            mapper[chebi_id].append(str(name))

    result: pd.DataFrame = (
        pd.Series(mapper).reset_index().rename(columns={0: "NAME", "index": "CHEBI_ID"})
    )
    return result


# -- CDNO --------------------------------------------------------------


def load_cdno(settings: KGCSettings) -> pd.DataFrame:
    """Load preprocessed CDNO data with ChEBI and FDC nutrient mappings."""
    dp_dir = Path(settings.integration_dir)
    cdno: pd.DataFrame = pd.read_parquet(
        dp_dir / "cdno_cleaned.parquet",
    )
    cdno = cdno.rename(columns={"index": "cdno_id"})
    cdno["cdno_id"] = cdno["cdno_id"].apply(lambda x: x.split("/")[-1])
    cdno["chebi_id"] = (
        cdno["chebi_id"]
        .apply(
            lambda x: (
                int(x.split("/")[-1].split("_")[-1]) if isinstance(x, str) else None
            )
        )
        .astype("Int64")
    )
    cdno["fdc_nutrient_ids"] = cdno["fdc_nutrient_ids"].apply(
        lambda x: [int(xx) for xx in x]
    )
    cdno["label"] = cdno["label"].apply(
        lambda x: x.split("concentration of ")[-1].split(" in material entity")[0]
    )
    cdno.loc[cdno["label"] == "nitrogen atom", "chebi_id"] = 29351
    return cdno[["cdno_id", "label", "chebi_id", "fdc_nutrient_ids"]].copy()


# -- FDC Nutrients -----------------------------------------------------


def load_fdc_nutrient(settings: KGCSettings) -> pd.DataFrame:
    """Load FDC nutrient data with manual corrections."""
    data_dir = Path(settings.data_dir)
    fdc_path = (
        data_dir
        / "FDC"
        / "FoodData_Central_foundation_food_csv_2024-04-18"
        / "nutrient.csv"
    )
    fdc: pd.DataFrame = pd.read_csv(fdc_path)
    fdc["name"] = fdc["name"].str.lower().str.strip()

    ids_remove = [2048, 1008, 1062]
    fdc = fdc[~fdc["id"].isin(ids_remove)].copy()
    fdc.at[fdc[fdc["id"] == 2047].index[0], "name"] = "energy"

    return fdc.set_index("id")


# -- MeSH --------------------------------------------------------------


def load_mapper_name_to_mesh_id(settings: KGCSettings) -> pd.Series:
    """Load name -> MeSH ID mapper from preprocessed MeSH data."""
    dp_dir = Path(settings.integration_dir)
    mesh_desc = pd.read_parquet(
        dp_dir / "mesh_desc_cleaned.parquet",
    ).set_index("name")
    mesh_supp = pd.read_parquet(
        dp_dir / "mesh_supp_cleaned.parquet",
    ).set_index("name")

    mapper: pd.Series = pd.concat([mesh_desc, mesh_supp])["id"]
    return mapper


def load_mesh(settings: KGCSettings) -> pd.Series:
    """Load MeSH terms with lowercased, deduplicated synonyms."""
    dp_dir = Path(settings.integration_dir)
    meshd = pd.read_parquet(
        dp_dir / "mesh_desc_cleaned.parquet",
    ).set_index("id")
    meshs = pd.read_parquet(
        dp_dir / "mesh_supp_cleaned.parquet",
    ).set_index("id")

    mesh: pd.Series = pd.concat([meshd["synonyms"], meshs["synonyms"]])
    mesh = mesh.apply(
        lambda names: list(
            OrderedDict.fromkeys([name.lower() for name in names]).keys()
        )
    )
    return mesh


# -- PubChem -----------------------------------------------------------


def load_mapper_pubchem_cid_to_mesh_id(settings: KGCSettings) -> pd.Series:
    """Load PubChem CID -> MeSH ID mapper."""
    name_to_mesh = load_mapper_name_to_mesh_id(settings)
    data_dir = Path(settings.data_dir)

    cid_to_name: pd.DataFrame = pd.read_csv(
        data_dir / "PubChem" / "CID-MeSH.txt",
        sep="\t",
        names=["cid", "mesh_term", "mesh_term_alt"],
    ).set_index("cid")

    cid_to_name["mesh_id"] = cid_to_name["mesh_term"].map(name_to_mesh)
    result: pd.Series = cid_to_name["mesh_id"].dropna()
    return result


def load_mapper_chebi_id_to_pubchem_cid(settings: KGCSettings) -> pd.Series:
    """Load ChEBI ID -> PubChem CID mapper from preprocessed data."""
    dp_dir = Path(settings.integration_dir)
    chebi2cid: pd.DataFrame = pd.read_parquet(
        dp_dir / "pubchem-sid-map-small.parquet",
        columns=["registry_id", "cid"],
    )
    chebi2cid["chebi_id"] = chebi2cid["registry_id"].apply(
        lambda x: int(x.split(":")[-1])
    )
    chebi2cid["cid"] = chebi2cid["cid"].astype("Int64")
    chebi2cid = chebi2cid.dropna(subset=["cid"])
    result: pd.Series = chebi2cid.set_index("chebi_id")["cid"]
    return result
