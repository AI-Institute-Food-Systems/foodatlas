"""Attestation model — an interpretation of evidence by a specific source."""

from pydantic import BaseModel, Field


class Attestation(BaseModel):
    """What a model (or human) attested from a piece of evidence.

    Links back to evidence via ``evidence_id``. Supports validation:
    a human can mark an attestation as correct or incorrect.

    Ambiguity: ``head_candidates`` / ``tail_candidates`` list all entity
    IDs the raw name or source ID could resolve to.  ``len == 1`` means
    pristine (unambiguous); ``len > 1`` means the same evidence applies
    to multiple possible entities.
    """

    attestation_id: str
    evidence_id: str  # FK → Evidence
    source: str  # "lit2kg:gpt-3.5-ft", "fdc", "human"
    head_name_raw: str = ""  # raw food/chemical name from attestation
    tail_name_raw: str = ""  # raw chemical/disease name from attestation
    conc_value: float | None = None
    conc_unit: str = ""
    conc_value_raw: str = ""
    conc_unit_raw: str = ""
    food_part: str = ""
    food_processing: str = ""
    filter_score: float | None = None
    validated: bool = False
    validated_correct: bool = True
    head_candidates: list[str] = Field(default_factory=list)
    tail_candidates: list[str] = Field(default_factory=list)
