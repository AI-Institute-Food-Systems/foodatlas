"""Extraction model — an interpretation of evidence by a specific extractor."""

from pydantic import BaseModel


class Extraction(BaseModel):
    """What a model (or human) extracted from a piece of evidence.

    Links back to evidence via ``evidence_id``. Supports validation:
    a human can mark an extraction as correct or incorrect.
    """

    extraction_id: str
    evidence_id: str  # FK → Evidence
    extractor: str  # "lit2kg:gpt-3.5-ft", "fdc", "human"
    head_name_raw: str = ""  # raw food/chemical name from extraction
    tail_name_raw: str = ""  # raw chemical/disease name from extraction
    conc_value: float | None = None
    conc_unit: str = ""
    food_part: str = ""
    food_processing: str = ""
    quality_score: float | None = None
    validated: bool = False
    validated_correct: bool = True
