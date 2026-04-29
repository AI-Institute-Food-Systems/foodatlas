"""Version-bundle loader for trust signals.

Each trust-signal version is a self-contained YAML at
``versions/<signal_kind>/<version>.yml`` bundling provider, model, prompts,
generation params, and response schema. The :func:`compute_config_hash` over
the parsed (canonicalized) yml is the dedup key that lets re-running an
unchanged version be a no-op while a single-character prompt edit forces a
re-judge under the same ``version`` label.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field


class GenerationParams(BaseModel):
    """LLM generation knobs propagated to the provider call."""

    temperature: float = 0.0
    max_output_tokens: int = 256
    response_mime_type: str = "application/json"


class PromptTemplates(BaseModel):
    """Prompt strings; ``user`` is rendered per-attestation via ``str.format``."""

    system: str
    user: str


class VersionBundle(BaseModel):
    """Parsed and validated version yml.

    Holds only "what we ask the model": provider, model id, prompts,
    generation params, response schema. Operational knobs (batch vs sync,
    limits, source filters) live in :class:`TrustStageConfig`. This means
    flipping sync → batch does NOT change ``config_hash`` and therefore
    does NOT force a re-judge — same intent, different deployment.

    v1 is LLM-shaped (provider/model/prompts required). Future non-LLM signal
    kinds (e.g. ``range_check``) will introduce a sibling shape and a
    discriminated union; not pre-engineered here.
    """

    signal_kind: str
    description: str = ""
    provider: Literal["gemini", "bedrock", "openai"]
    model: str
    generation: GenerationParams = Field(default_factory=GenerationParams)
    prompts: PromptTemplates
    response_schema: dict[str, Any]


_DEFAULT_VERSIONS_DIR = Path(__file__).resolve().parent / "versions"


def compute_config_hash(parsed: dict[str, Any]) -> str:
    """Canonical sha256 of a parsed yml dict.

    Invariant under cosmetic yaml differences (key order, comments, trailing
    whitespace) because we serialize the parsed structure with sorted keys and
    no whitespace before hashing. Sensitive to any semantic change — prompt
    edits, model swaps, temperature tweaks — because those mutate the parsed
    values that get serialized.
    """
    canonical = json.dumps(
        parsed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_version(
    signal_kind: str,
    version: str,
    *,
    base_dir: Path | None = None,
) -> tuple[VersionBundle, str]:
    """Load and validate ``versions/<signal_kind>/<version>.yml``.

    Returns the parsed :class:`VersionBundle` and the canonical config hash.
    The hash is the dedup key written to ``trust_signals.parquet`` rows.
    """
    base = base_dir if base_dir is not None else _DEFAULT_VERSIONS_DIR
    path = base / signal_kind / f"{version}.yml"
    parsed: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        msg = f"Version yml at {path} did not parse to a mapping"
        raise ValueError(msg)
    bundle = VersionBundle.model_validate(parsed)
    if bundle.signal_kind != signal_kind:
        msg = (
            f"Version yml at {path} declares signal_kind={bundle.signal_kind!r} "
            f"but lives under {signal_kind!r} directory"
        )
        raise ValueError(msg)
    return bundle, compute_config_hash(parsed)
