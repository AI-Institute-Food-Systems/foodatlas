"""Chemical ontology (IS_A) triplet builders."""

from .cdno import merge_chemical_ontology_cdno
from .chebi import merge_chemical_ontology
from .dmd import merge_chemical_ontology_dmd
from .foodatlas import merge_chemical_ontology_foodatlas

__all__ = [
    "merge_chemical_ontology",
    "merge_chemical_ontology_cdno",
    "merge_chemical_ontology_dmd",
    "merge_chemical_ontology_foodatlas",
]
