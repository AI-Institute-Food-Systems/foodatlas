"""Shared pydantic shapes for the evaluation judge (structured LLM I/O)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Pair(BaseModel):
    """A single (food, chemical, concentration) extraction."""

    food: str
    chemical: str
    concentration: str = Field(
        default="",
        description="Amount with unit if the sentence states one, else empty.",
    )


class MatchedPair(BaseModel):
    """A model extraction the adjudicator confirmed is stated by the sentence."""

    food: str
    chemical: str
    conc_agree: bool = Field(
        description=(
            "True if the model's and reference's concentrations agree within an "
            "order of magnitude, or both are absent; else False."
        ),
    )


class Adjudication(BaseModel):
    """Per-sentence verdict aligning model extractions against the sentence."""

    matches: list[MatchedPair] = Field(
        description="Model pairs that ARE stated by the sentence (true positives).",
    )
    model_only: list[Pair] = Field(
        description="Model pairs NOT stated by the sentence (false positives).",
    )
    gold_only: list[Pair] = Field(
        description="Facts stated by the sentence the model missed (false negatives).",
    )
