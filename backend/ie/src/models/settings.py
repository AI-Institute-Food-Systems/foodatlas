"""IE settings with Pydantic Settings, env prefix IE_."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

_PROJECT_ENV = Path(__file__).resolve().parents[2] / ".env"
_ROOT_ENV = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(_PROJECT_ENV)
load_dotenv(_ROOT_ENV)

_DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.json"


def _load_defaults() -> dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        with _DEFAULTS_PATH.open() as f:
            data: dict[str, Any] = json.load(f)
            return data
    return {}


class SearchConfig(BaseModel):
    save_every: int = 10
    query_uid_results: str = "outputs/search/{date}/query_uid_results.tsv"
    filtered_sentences: str = "outputs/search/{date}/retrieved_sentences/result_{i}.tsv"


class BiobertFilterConfig(BaseModel):
    sentence_col: str = "sentence"
    chunk_size: int = 10000
    batch_size: int = 64


class AggregateConfig(BaseModel):
    threshold: float = 0.99
    reference_dir: str = "outputs/extraction"


class ExtractionConfig(BaseModel):
    system_prompt: str = "src/pipeline/extraction/prompts/system/v1.txt"
    user_prompt: str = "src/pipeline/extraction/prompts/user/v1.txt"
    max_new_tokens: int = 512
    temperature: float = 0.0


class ParseConfig(BaseModel):
    batch_input: str = "batch_input.tsv"
    output_tsv: str = "extraction_predicted.tsv"


class PipelineConfig(BaseModel):
    search: SearchConfig = SearchConfig()
    biobert_filter: BiobertFilterConfig = BiobertFilterConfig()
    aggregate: AggregateConfig = AggregateConfig()
    extraction: ExtractionConfig = ExtractionConfig()
    parse: ParseConfig = ParseConfig()


class IESettings(BaseSettings):
    model_config = {"env_prefix": "IE_"}

    date: str = ""
    model: str = "gpt-5.2"
    bioc_pmc_dir: str = ""
    bioc_pmc_dl_dir: str = ""
    biobert_model_dir: str = ""
    food_terms: str = "data/food_terms.txt"
    translated_food_terms: str = "data/translated_food_terms.txt"
    pipeline: PipelineConfig = PipelineConfig()

    @property
    def resolved_date(self) -> str:
        return self.date or datetime.now(tz=UTC).strftime("%Y_%m_%d")

    @property
    def threshold(self) -> float:
        return self.pipeline.aggregate.threshold

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        return values
