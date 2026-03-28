"""KGC settings with Pydantic Settings, env prefix KGC_."""

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

from ..utils.json_io import read_json

_ROOT_ENV = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(_ROOT_ENV)

_DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.json"


def _load_defaults() -> dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        data: dict[str, Any] = read_json(_DEFAULTS_PATH)
        return data
    return {}


class DataCleaningStageConfig(BaseModel):
    output_dir: str = ""


class KgInitStageConfig(BaseModel):
    pass


class MetadataProcessingStageConfig(BaseModel):
    pass


class TripletExpansionStageConfig(BaseModel):
    pass


class PostprocessingStageConfig(BaseModel):
    pass


class StagesConfig(BaseModel):
    data_cleaning: DataCleaningStageConfig = DataCleaningStageConfig()
    kg_init: KgInitStageConfig = KgInitStageConfig()
    metadata_processing: MetadataProcessingStageConfig = MetadataProcessingStageConfig()
    triplet_expansion: TripletExpansionStageConfig = TripletExpansionStageConfig()
    postprocessing: PostprocessingStageConfig = PostprocessingStageConfig()


class PipelineConfig(BaseModel):
    stages: StagesConfig = StagesConfig()


class KGCSettings(BaseSettings):
    model_config = {"env_prefix": "KGC_"}

    kg_dir: str = ""
    data_dir: str = ""
    output_dir: str = ""
    cache_dir: str = ""
    openai_api_key: str = ""
    pubchem_mapping_file: str = ""
    pipeline: PipelineConfig = PipelineConfig()

    ncbi_email: str = ""
    ncbi_api_key: str = ""

    @property
    def data_cleaning_dir(self) -> str:
        return self.pipeline.stages.data_cleaning.output_dir

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        _env_fields = [("NCBI_EMAIL", "ncbi_email"), ("NCBI_API_KEY", "ncbi_api_key")]
        for env_key, field in _env_fields:
            if field not in values or values[field] in ("", None):
                values[field] = os.environ.get(env_key, "")
        return values
