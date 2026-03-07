"""KGC settings with Pydantic Settings, env prefix KGC_."""

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

_DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.json"


def _load_defaults() -> dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        with _DEFAULTS_PATH.open() as f:
            data: dict[str, Any] = json.load(f)
            return data
    return {}


class KGCSettings(BaseSettings):
    model_config = {"env_prefix": "KGC_"}

    kg_dir: str = ""
    output_dir: str = ""
    cache_dir: str = ""
    output_format: Literal["json", "jsonl", "parquet"] = "jsonl"
    openai_api_key: str = ""
    ncbi_api_key: str = ""

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        return values
