"""KGC settings with Pydantic Settings, env prefix KGC_."""

import json
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


class DataCleaningStageConfig(BaseModel):
    output_dir: str = ""


class IntegrationStagesConfig(BaseModel):
    data_cleaning: DataCleaningStageConfig = DataCleaningStageConfig()


class StagesConfig(BaseModel):
    integration: IntegrationStagesConfig = IntegrationStagesConfig()


class PipelineConfig(BaseModel):
    stages: StagesConfig = StagesConfig()


class KGCSettings(BaseSettings):
    model_config = {"env_prefix": "KGC_"}

    kg_dir: str = ""
    data_dir: str = ""
    output_dir: str = ""
    cache_dir: str = ""
    openai_api_key: str = ""
    ncbi_email: str = ""
    ncbi_api_key: str = ""
    pubchem_mapping_file: str = ""
    pipeline: PipelineConfig = PipelineConfig()

    @property
    def data_cleaning_dir(self) -> str:
        return self.pipeline.stages.integration.data_cleaning.output_dir

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        return values
