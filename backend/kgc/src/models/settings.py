"""KGC settings with Pydantic Settings, env prefix KGC_."""

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
    previous_kg_entities: str = ""


class StagesConfig(BaseModel):
    data_cleaning: DataCleaningStageConfig = DataCleaningStageConfig()
    kg_init: KgInitStageConfig = KgInitStageConfig()


class PipelineConfig(BaseModel):
    stages: StagesConfig = StagesConfig()


class KGCSettings(BaseSettings):
    model_config = {"env_prefix": "KGC_"}

    kg_dir: str = ""
    data_dir: str = ""
    output_dir: str = ""
    cache_dir: str = ""
    ie_raw_dir: str = ""
    pipeline: PipelineConfig = PipelineConfig()

    @property
    def data_cleaning_dir(self) -> str:
        return self.pipeline.stages.data_cleaning.output_dir

    @property
    def previous_kg_entities(self) -> str:
        """Path to previous KG entities TSV for registry seeding."""
        return self.pipeline.stages.kg_init.previous_kg_entities

    @property
    def ingest_dir(self) -> str:
        """Output directory for Phase 1 ingest artifacts."""
        return str(Path(self.output_dir) / "ingest")

    @model_validator(mode="before")
    @classmethod
    def _fill_defaults(cls, values: dict) -> dict:
        defaults = _load_defaults()
        for key, default_value in defaults.items():
            if key not in values or values[key] in ("", None):
                values[key] = default_value
        return values
