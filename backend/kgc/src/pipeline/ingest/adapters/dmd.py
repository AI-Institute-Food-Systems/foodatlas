"""DMD adapter — faithful ingest of Dairy Molecules Database."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ....models.ingest import SourceManifest
from ..protocol import (
    ProgressCallback,
    _noop_progress,
    serialize_raw_attrs,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_ID = "dmd"

_MILK_DENSITY = 1.03  # g/mL


def _parse_set_field(value: str) -> list[str]:
    """Parse PostgreSQL-style set notation ``{val1,"val2"}`` → list."""
    if pd.isna(value) or not value:
        return []
    s = value.strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    if not s:
        return []
    items: list[str] = []
    buf: list[str] = []
    in_quotes = False
    for ch in s:
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == "," and not in_quotes:
            items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        items.append("".join(buf).strip())
    return [i for i in items if i]


def _ug240ml_to_mg100g(value: float) -> float:
    """Convert µg/240 mL to mg/100 g using milk density."""
    return value / _MILK_DENSITY * 100 / 240 / 1e3


class DMDAdapter:
    """Parse DMD CSV files into standardized ingest parquet."""

    @property
    def source_id(self) -> str:
        return SOURCE_ID

    def ingest(
        self,
        raw_dir: Path,
        output_dir: Path,
        progress: ProgressCallback = _noop_progress,
    ) -> SourceManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        dmd_dir = raw_dir / "DMD"

        molecules = pd.read_csv(dmd_dir / "molecule.csv")
        concentrations = pd.read_csv(dmd_dir / "concentration.csv")
        estimated = pd.read_csv(dmd_dir / "estimated_concentration.csv")

        total = len(molecules) + len(concentrations)
        progress(0, total)

        nodes, xrefs = _build_nodes_and_xrefs(molecules, progress, total)
        glycan_ids = set(
            molecules.loc[molecules["Omic Lab"] == "{Glycomics}", "DMD ID"]
        )
        edges = _build_edges(
            concentrations, estimated, glycan_ids, progress, total, len(molecules)
        )

        nodes = serialize_raw_attrs(nodes)
        edges = serialize_raw_attrs(edges)

        nodes_path = output_dir / f"{SOURCE_ID}_nodes.parquet"
        edges_path = output_dir / f"{SOURCE_ID}_edges.parquet"
        xrefs_path = output_dir / f"{SOURCE_ID}_xrefs.parquet"
        nodes.to_parquet(nodes_path)
        edges.to_parquet(edges_path)
        xrefs.to_parquet(xrefs_path)
        files = [str(nodes_path), str(edges_path), str(xrefs_path)]

        manifest = SourceManifest(
            source_id=SOURCE_ID,
            node_count=len(nodes),
            edge_count=len(edges),
            xref_count=len(xrefs),
            raw_dir=str(raw_dir),
            output_files=files,
        )
        write_manifest(manifest, output_dir)
        progress(total, total)
        logger.info(
            "DMD ingest: %d nodes, %d edges, %d xrefs.",
            len(nodes),
            len(edges),
            len(xrefs),
        )
        return manifest


def _build_nodes_and_xrefs(
    molecules: pd.DataFrame,
    progress: ProgressCallback,
    total: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    node_rows: list[dict] = []
    xref_rows: list[dict] = []

    for i, (_, row) in enumerate(molecules.iterrows()):
        dmd_id = str(row["DMD ID"])
        mol_name = str(row["Molecule Name"]) if pd.notna(row["Molecule Name"]) else ""
        composition = (
            str(row["Chemical Composition"])
            if pd.notna(row["Chemical Composition"])
            else ""
        )

        synonyms = [mol_name] if mol_name else []
        synonym_types = ["name"] if mol_name else []
        if composition:
            synonyms.append(composition)
            synonym_types.append("synonym")

        classification = _parse_set_field(row.get("Molecule Classification", ""))
        omic_lab = _parse_set_field(row.get("Omic Lab", ""))

        node_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": dmd_id,
                "name": mol_name,
                "synonyms": synonyms,
                "synonym_types": synonym_types,
                "node_type": "molecule",
                "raw_attrs": {
                    "molecule_classification": classification,
                    "omic_lab": omic_lab,
                    "molecular_weight": (
                        float(row["Molecular Weight"])
                        if pd.notna(row.get("Molecular Weight"))
                        else None
                    ),
                    "molecular_weight_unit": (
                        str(row["Molecular Weight Unit"])
                        if pd.notna(row.get("Molecular Weight Unit"))
                        else None
                    ),
                },
            }
        )

        _emit_xrefs(row, dmd_id, xref_rows)
        progress(i + 1, total)

    return pd.DataFrame(node_rows), pd.DataFrame(xref_rows)


def _emit_xrefs(row: pd.Series, dmd_id: str, xref_rows: list[dict]) -> None:
    """Parse External Database IDs and KEGG IDs into xref rows."""
    ext_raw = row.get("External Database IDs")
    if pd.notna(ext_raw):
        try:
            ext_ids: dict = json.loads(str(ext_raw))
        except (json.JSONDecodeError, ValueError):
            ext_ids = {}
        _KEY_MAP = {
            "ChEBI": "chebi",
            "PubChem": "pubchem_cid",
            "UniProt": "uniprot",
            "miRBase": "mirbase",
            "HMDB": "hmdb",
            "LIPID MAPS": "lipid_maps",
        }
        for ext_key, target_source in _KEY_MAP.items():
            for val in ext_ids.get(ext_key, []):
                xref_rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "native_id": dmd_id,
                        "target_source": target_source,
                        "target_id": str(val),
                    }
                )

    kegg_raw = row.get("KEGG IDs")
    for kid in _parse_set_field(str(kegg_raw) if pd.notna(kegg_raw) else ""):
        xref_rows.append(
            {
                "source_id": SOURCE_ID,
                "native_id": dmd_id,
                "target_source": "kegg",
                "target_id": kid,
            }
        )


def _build_edges(
    concentrations: pd.DataFrame,
    estimated: pd.DataFrame,
    glycan_ids: set[str],
    progress: ProgressCallback,
    total: int,
    offset: int,
) -> pd.DataFrame:
    """Build contains edges from concentration data with unit conversion."""
    est_lookup = estimated.set_index("DMD Concentration ID")[
        "Estimated Concentration Value"
    ]

    has_value = concentrations[concentrations["Concentration Value Alt"].notna()]
    edge_rows: list[dict] = []

    for i, (_, row) in enumerate(has_value.iterrows()):
        conc_val_alt = row["Concentration Value Alt"]
        unit_alt = str(row["Concentration Unit Alt"])
        dmd_conc_id = str(row["DMD ID"])
        mol_id = str(row["DMD Molecule ID"])

        conc_value, conc_unit = _convert_concentration(
            conc_val_alt, unit_alt, dmd_conc_id, mol_id, glycan_ids, est_lookup
        )

        edge_rows.append(
            {
                "source_id": SOURCE_ID,
                "head_native_id": "milk",
                "tail_native_id": mol_id,
                "edge_type": "contains",
                "raw_attrs": {
                    "conc_value": conc_value,
                    "conc_unit": conc_unit,
                    "conc_value_raw": float(conc_val_alt),
                    "conc_unit_raw": unit_alt,
                    "dmd_concentration_id": dmd_conc_id,
                },
            }
        )
        progress(offset + i + 1, total)

    return pd.DataFrame(edge_rows)


def _convert_concentration(
    value: float,
    unit: str,
    dmd_conc_id: str,
    mol_id: str,
    glycan_ids: set[str],
    est_lookup: pd.Series,
) -> tuple[float | None, str | None]:
    """Convert concentration to mg/100g where possible.

    Returns (conc_value, conc_unit).
    """
    if value == 0:
        return None, None

    if unit == "µg/240mL":
        return _ug240ml_to_mg100g(value), "mg/100g"

    if unit == "%" and mol_id not in glycan_ids:
        est_val = est_lookup.get(dmd_conc_id)
        if pd.notna(est_val):
            return _ug240ml_to_mg100g(float(est_val)), "mg/100g"

    return float(value), unit
