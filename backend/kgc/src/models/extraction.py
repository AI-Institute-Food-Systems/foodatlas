"""Extraction model — an interpretation of evidence by a specific extractor."""

from pydantic import BaseModel, Field


class Extraction(BaseModel):
    """What a model (or human) extracted from a piece of evidence.

    Links back to evidence via ``evidence_id``. Supports validation:
    a human can mark an extraction as correct or incorrect.

    Ambiguity: ``head_candidates`` / ``tail_candidates`` list all entity
    IDs the raw name or source ID could resolve to.  ``len == 1`` means
    pristine (unambiguous); ``len > 1`` means the same evidence applies
    to multiple possible entities.
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
    head_candidates: list[str] = Field(default_factory=list)
    tail_candidates: list[str] = Field(default_factory=list)
