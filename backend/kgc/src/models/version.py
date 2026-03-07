"""KG version model."""

from __future__ import annotations

from pydantic import BaseModel


class KGVersion(BaseModel):
    major: int = 0
    minor: int = 1
    patch: int = 0
    label: str = ""

    @property
    def version_string(self) -> str:
        base = f"{self.major}.{self.minor}.{self.patch}"
        if self.label:
            return f"{base}-{self.label}"
        return base
