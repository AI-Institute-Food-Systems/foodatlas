"""Tests for CDNO ingest adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from src.pipeline.ingest.adapters.cdno import CDNOAdapter

if TYPE_CHECKING:
    from pathlib import Path


def _write_cdno_owl(path: Path) -> None:
    """Write minimal CDNO OWL for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("""\
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:oboInOwl="http://www.geneontology.org/formats/oboInOwl#">
  <owl:Class rdf:about="http://cdno/C001">
    <rdfs:label>Vitamin C concentration</rdfs:label>
    <rdfs:subClassOf rdf:resource="http://cdno/ROOT"/>
    <oboInOwl:hasDbXref>USDA_fdc_id:1001</oboInOwl:hasDbXref>
    <owl:equivalentClass>
      <rdf:Description rdf:about="http://purl.obolibrary.org/obo/CHEBI_29073"/>
    </owl:equivalentClass>
  </owl:Class>
  <owl:Class rdf:about="http://cdno/C002">
    <rdfs:label>Protein concentration</rdfs:label>
    <rdfs:subClassOf rdf:resource="http://cdno/ROOT"/>
  </owl:Class>
  <owl:Class rdf:about="http://cdno/DEP">
    <owl:deprecated>true</owl:deprecated>
    <rdfs:label>Deprecated</rdfs:label>
  </owl:Class>
</rdf:RDF>
""")


def test_cdno_adapter(tmp_path: Path) -> None:
    _write_cdno_owl(tmp_path / "CDNO" / "cdno.owl")

    adapter = CDNOAdapter()
    out_dir = tmp_path / "output"
    manifest = adapter.ingest(tmp_path, out_dir)

    assert manifest.source_id == "cdno"
    assert manifest.node_count == 2  # C001, C002 (DEP is deprecated)
    assert manifest.edge_count == 2  # 2 subClassOf
    assert manifest.xref_count >= 2  # chebi + fdc xrefs from C001

    nodes = pd.read_parquet(out_dir / "cdno_nodes.parquet")
    assert len(nodes) == 2
    assert "vitamin c concentration" in nodes["name"].values

    xrefs = pd.read_parquet(out_dir / "cdno_xrefs.parquet")
    chebi_xrefs = xrefs[xrefs["target_source"] == "chebi"]
    fdc_xrefs = xrefs[xrefs["target_source"] == "fdc_nutrient"]
    assert len(chebi_xrefs) == 1
    assert "CHEBI_29073" in chebi_xrefs.iloc[0]["target_id"]
    assert len(fdc_xrefs) == 1
    assert fdc_xrefs.iloc[0]["target_id"] == "1001"


def test_cdno_adapter_source_id() -> None:
    assert CDNOAdapter().source_id == "cdno"
