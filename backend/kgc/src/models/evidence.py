"""Evidence model — immutable source reference for a knowledge claim."""

from pydantic import BaseModel


class Evidence(BaseModel):
    """A piece of evidence supporting a triplet.

    Immutable — the source reference never changes. Multiple extractions
    can be derived from the same evidence (different models, human review).
    """

    evidence_id: str
    source_type: str  # "pubmed", "fdc"
    reference: str  # JSON string: {"pmcid": 123, "text": "..."} or {"url": "..."}
