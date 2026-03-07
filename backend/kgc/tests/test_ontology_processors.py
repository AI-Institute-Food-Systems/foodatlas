"""Tests for ontology preprocessing modules."""

from pathlib import Path

import pandas as pd
import pytest
from bs4 import BeautifulSoup
from src.integration.ontologies.cdno import (
    _disambiguate_fdc_ids,
    _extract_chebi_ids,
    _validate_fdc_chebi_mapping,
)
from src.integration.ontologies.chebi import _build_name_lut
from src.integration.ontologies.foodon import (
    _clean,
    _label_is_food,
    _label_is_organism,
    _rename_foodon_id,
)
from src.integration.ontologies.mesh import (
    _ensure_list,
    _extract_synonyms,
    _parse_mesh_desc,
    _parse_mesh_supp,
)
from src.integration.ontologies.pubchem import process_pubchem
from src.models.settings import KGCSettings


class TestProcessPubchem:
    def test_filters_chebi_rows(self, tmp_path: Path) -> None:
        tsv = tmp_path / "PubChem" / "SID-Map"
        tsv.parent.mkdir(parents=True)
        tsv.write_text(
            "1\tChEBI\tCHEBI:100\t200\n2\tDrugBank\tDB001\t300\n"
            "3\tChEBI\tCHEBI:101\t400\n"
        )
        settings = KGCSettings(
            kg_dir=str(tmp_path),
            data_dir=str(tmp_path),
            integration_dir=str(tmp_path),
        )
        process_pubchem(settings)
        result = pd.read_parquet(tmp_path / "pubchem-sid-map-small.parquet")
        assert len(result) == 2
        assert (result["source"] == "ChEBI").all()


class TestEnsureList:
    def test_list_passthrough(self) -> None:
        assert _ensure_list([1, 2]) == [1, 2]

    def test_dict_wrapped(self) -> None:
        assert _ensure_list({"a": 1}) == [{"a": 1}]

    def test_raises_on_string(self) -> None:
        with pytest.raises(ValueError, match="Unknown type"):
            _ensure_list("bad")

    def test_raises_on_int(self) -> None:
        with pytest.raises(ValueError, match="Unknown type"):
            _ensure_list(42)


class TestExtractSynonyms:
    def test_single_concept_single_term(self) -> None:
        record = {
            "ConceptList": {"Concept": {"TermList": {"Term": {"String": "aspirin"}}}}
        }
        assert _extract_synonyms(record) == ["aspirin"]

    def test_multiple_concepts_and_terms(self) -> None:
        record = {
            "ConceptList": {
                "Concept": [
                    {"TermList": {"Term": [{"String": "a"}, {"String": "b"}]}},
                    {"TermList": {"Term": {"String": "c"}}},
                ]
            }
        }
        assert _extract_synonyms(record) == ["a", "b", "c"]


class TestParseMeshDesc:
    def test_parse_desc_xml(self, tmp_path: Path) -> None:
        xml = (
            '<?xml version="1.0"?><DescriptorRecordSet>'
            "<DescriptorRecord><DescriptorUI>D001</DescriptorUI>"
            "<DescriptorName><String>Aspirin</String></DescriptorName>"
            "<TreeNumberList><TreeNumber>D03.1</TreeNumber></TreeNumberList>"
            "<ConceptList><Concept><TermList><Term><String>ASA</String>"
            "</Term></TermList></Concept></ConceptList></DescriptorRecord>"
            "<DescriptorRecord><DescriptorUI>D002</DescriptorUI>"
            "<DescriptorName><String>Beta</String></DescriptorName>"
            "<ConceptList><Concept><TermList><Term><String>B</String>"
            "</Term></TermList></Concept></ConceptList></DescriptorRecord>"
            "</DescriptorRecordSet>"
        )
        p = tmp_path / "desc.xml"
        p.write_text(xml)
        df = _parse_mesh_desc(p)
        assert len(df) == 2
        assert df.iloc[0]["id"] == "D001"
        assert df.iloc[0]["name"] == "Aspirin"
        assert df.iloc[1]["tree_numbers"] == []


class TestParseMeshSupp:
    def test_parse_supp_xml(self, tmp_path: Path) -> None:
        xml = (
            '<?xml version="1.0"?><SupplementalRecordSet>'
            '<SupplementalRecord SCRClass="1">'
            "<SupplementalRecordUI>C001</SupplementalRecordUI>"
            "<SupplementalRecordName><String>CompA</String>"
            "</SupplementalRecordName><HeadingMappedToList><HeadingMappedTo>"
            "<DescriptorReferredTo><DescriptorUI>D001</DescriptorUI>"
            "</DescriptorReferredTo></HeadingMappedTo></HeadingMappedToList>"
            "<ConceptList><Concept><TermList><Term><String>syn1</String>"
            "</Term></TermList></Concept></ConceptList></SupplementalRecord>"
            '<SupplementalRecord SCRClass="1">'
            "<SupplementalRecordUI>C002</SupplementalRecordUI>"
            "<SupplementalRecordName><String>CompB</String>"
            "</SupplementalRecordName><HeadingMappedToList><HeadingMappedTo>"
            "<DescriptorReferredTo><DescriptorUI>D002</DescriptorUI>"
            "</DescriptorReferredTo></HeadingMappedTo></HeadingMappedToList>"
            "<ConceptList><Concept><TermList><Term><String>syn2</String>"
            "</Term></TermList></Concept></ConceptList></SupplementalRecord>"
            "</SupplementalRecordSet>"
        )
        p = tmp_path / "supp.xml"
        p.write_text(xml)
        df = _parse_mesh_supp(p)
        assert len(df) == 2
        assert df.iloc[0]["mapped_to"] == ["D001"]


class TestBuildNameLut:
    def test_star_priority(self) -> None:
        chebi = pd.DataFrame(
            {"NAME": ["alpha", "beta"], "STAR": [3, 2]},
            index=[100, 200],
        )
        chebi.index.name = "ID"
        synonyms = pd.DataFrame(
            {"COMPOUND_ID": [100, 200], "NAME": ["gamma", "delta"]},
        )
        lut = _build_name_lut(chebi, synonyms)
        assert {"alpha", "beta", "gamma", "delta"} == set(lut["NAME"])

    def test_no_duplicate_primary(self) -> None:
        chebi = pd.DataFrame({"NAME": ["x"], "STAR": [3]}, index=[1])
        chebi.index.name = "ID"
        synonyms = pd.DataFrame({"COMPOUND_ID": [1], "NAME": ["x"]})
        assert len(_build_name_lut(chebi, synonyms)) == 1


class TestExtractChebiIds:
    def test_extracts_chebi_from_owl(self) -> None:
        xml = (
            '<root xmlns:owl="http://www.w3.org/2002/07/owl#"'
            ' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            '<owl:Class rdf:about="http://example.org/C1">'
            "<owl:equivalentClass>"
            "<rdf:Description rdf:about="
            '"http://purl.obolibrary.org/obo/CHEBI_123"/>'
            "</owl:equivalentClass></owl:Class></root>"
        )
        soup = BeautifulSoup(xml, "xml")
        el = soup.find("owl:Class")
        assert _extract_chebi_ids(el) == [
            "http://purl.obolibrary.org/obo/CHEBI_123",
        ]

    def test_no_equivalent_class(self) -> None:
        xml = (
            '<root xmlns:owl="http://www.w3.org/2002/07/owl#"'
            ' xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            '<owl:Class rdf:about="http://example.org/C2">'
            "</owl:Class></root>"
        )
        soup = BeautifulSoup(xml, "xml")
        assert _extract_chebi_ids(soup.find("owl:Class")) == []


class TestDisambiguateFdcIds:
    def test_single_match(self) -> None:
        cdno = pd.DataFrame(
            {
                "fdc_nutrient_ids": [["10"]],
                "chebi_id": ["CHEBI_1"],
                "label": ["Nutrient A"],
            },
            index=["id1"],
        )
        assert len(_disambiguate_fdc_ids(cdno)) == 1

    def test_disambiguate_via_chebi(self) -> None:
        cdno = pd.DataFrame(
            {
                "fdc_nutrient_ids": [["20"], ["20"]],
                "chebi_id": [None, "CHEBI_2"],
                "label": ["A", "B"],
            },
            index=["id1", "id2"],
        )
        result = _disambiguate_fdc_ids(cdno)
        assert len(result) == 1
        assert result.iloc[0]["chebi_id"] == "CHEBI_2"


class TestValidateFdcChebiMapping:
    def test_valid_mapping(self) -> None:
        df = pd.DataFrame(
            {"fdc_nutrient_ids": [["10"], ["20"]], "chebi_id": ["C1", "C2"]},
            index=["a", "b"],
        )
        _validate_fdc_chebi_mapping(df)

    def test_invalid_mapping_raises(self) -> None:
        df = pd.DataFrame(
            {"fdc_nutrient_ids": [["10"], ["10"]], "chebi_id": ["C1", "C2"]},
            index=["a", "b"],
        )
        with pytest.raises(ValueError, match="not one-to-one"):
            _validate_fdc_chebi_mapping(df)


class TestRenameFoodonId:
    def test_obo_prefix(self) -> None:
        expected = "http://purl.obolibrary.org/obo/FOODON_001"
        assert _rename_foodon_id("obo.FOODON_001") == expected

    def test_unknown_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown foodon_id"):
            _rename_foodon_id("bad.id")


class TestClean:
    def test_basic_parsing(self) -> None:
        rows = [
            {
                "?class": "<http://ex/A>",
                "?parent": "<http://ex/B>",
                "?type": "label",
                "?label": "Apple@en",
            },
            {
                "?class": "<http://ex/A>",
                "?parent": None,
                "?type": "synonym (exact)",
                "?label": "Malus",
            },
        ]
        result = _clean(pd.DataFrame(rows))
        assert "http://ex/A" in result.index
        row = result.loc["http://ex/A"]
        assert "Apple" in row["synonyms"]["label"]
        assert "Malus" in row["synonyms"]["synonym (exact)"]
        assert "http://ex/B" in row["parents"]


class TestLabelIsFood:
    def test_food_descendant(self) -> None:
        food_root = "http://purl.obolibrary.org/obo/FOODON_00002381"
        df = pd.DataFrame(
            {"parents": [[food_root], [], []], "synonyms": [{}, {}, {}]},
            index=[food_root, "child", "other"],
        )
        df.loc["child", "parents"] = [food_root]
        df.index.name = "foodon_id"
        result = _label_is_food(df)
        assert bool(result.loc["child", "is_food"]) is True
        assert not result.loc["other", "is_food"]


class TestLabelIsOrganism:
    def test_organism_descendant(self) -> None:
        org_root = "http://purl.obolibrary.org/obo/OBI_0100026"
        df = pd.DataFrame(
            {"parents": [[], [], []], "synonyms": [{}, {}, {}]},
            index=[org_root, "child", "other"],
        )
        df.at["child", "parents"] = [org_root]
        df.index.name = "foodon_id"
        result = _label_is_organism(df)
        assert bool(result.loc["child", "is_organism"]) is True
        assert not result.loc["other", "is_organism"]
