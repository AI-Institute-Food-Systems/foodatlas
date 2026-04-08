"""IE settings with Pydantic Settings, env prefix IE_."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

_DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.json"


def _load_defaults() -> dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        with _DEFAULTS_PATH.open() as f:
            data: dict[str, Any] = json.load(f)
            return data
    return {}


class SearchConfig(BaseModel):
    save_every: int = 50
    query_uid_results: str = "outputs/text_parser/{date}/query_uid_results.tsv"
    filtered_sentences: str = (
        "outputs/text_parser/{date}/retrieved_sentences/result_{i}.tsv"
    )
    last_search_date: str = "outputs/text_parser/last_search_date.txt"


class BiobertFilterConfig(BaseModel):
    sentence_col: str = "sentence"
    chunk_size: int = 10000
    batch_size: int = 64


class AggregateConfig(BaseModel):
    threshold: float = 0.99
    reference_dir: str = "outputs/past_sentence_filtering_preds"


class ExtractionConfig(BaseModel):
    system_prompt: str = "You are an expert in food science and chemistry. "
    prompt_version: str = "v1"
    max_new_tokens: int = 512
    temperature: float = 0.0


class ParseConfig(BaseModel):
    batch_input_pattern: str = "batch_input_{date}.tsv"
    output_tsv_pattern: str = "text_parser_predicted_{date}_{model}.tsv"


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
    ncbi_email: str = "user@example.com"
    pipeline: PipelineConfig = PipelineConfig()

    @property
    def resolved_date(self) -> str:
        return self.date or datetime.now(tz=UTC).strftime("%Y_%m_%d")

    @property
    def threshold(self) -> float:
        return self.pipeline.aggregate.threshold

    @property
    def prompt_version(self) -> str:
        return self.pipeline.extraction.prompt_version

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        return values
