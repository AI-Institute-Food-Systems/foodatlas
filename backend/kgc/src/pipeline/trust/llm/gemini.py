"""Gemini Batch API client.

Uses the ``google-genai`` SDK. Auth via ``GOOGLE_API_KEY`` env var. Batch flow:

1. Write per-request lines to a JSONL file (one record = one prompt).
2. Upload via the Files API.
3. Submit a Batch job referencing the uploaded file id.
4. Poll until terminal state.
5. Download the result file (also JSONL) and join responses back by ``key``.

Same pattern the IE pipeline uses for OpenAI batch in
``backend/ie/src/pipeline/extraction/runner.py`` — adapted for Gemini's
``contents`` / ``system_instruction`` shape.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from google import genai
from google.genai import types
from google.genai.errors import APIError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from .base import TrustLLMClient, TrustLLMRequest, TrustLLMResponse

if TYPE_CHECKING:
    from ..versions import VersionBundle

logger = logging.getLogger(__name__)

# google-genai uses httpx which logs every request at INFO; the SDK itself
# also logs an "AFC is enabled with max remote calls: 10" line per call
# (Automatic Function Calling — irrelevant for us because we pass no tools).
# Both fire once per request and drown the tqdm progress bar; bump to WARNING.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("google_genai.models").setLevel(logging.WARNING)

_COMPLETED_STATES = frozenset(
    {
        "JOB_STATE_SUCCEEDED",
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "JOB_STATE_EXPIRED",
    }
)
_POLL_INTERVAL_S = 30
_SYNC_MAX_WORKERS = 16

# HTTP codes that indicate a transient server / quota condition. Anything
# else (400 INVALID_ARGUMENT, 401/403 auth) is a real error and not retried.
_TRANSIENT_CODES = frozenset({429, 500, 502, 503, 504})


def _is_transient_api_error(exc: BaseException) -> bool:
    if not isinstance(exc, APIError):
        return False
    return getattr(exc, "code", None) in _TRANSIENT_CODES


def _job_state_name(job: Any) -> str:
    """Extract `state.name` from a batch job, raising if either is missing.

    The SDK types both ``state`` and ``state.name`` as Optional, but in
    practice they're always populated for a job that just round-tripped
    through ``batches.get``. Raise if not, so we fail loudly rather than
    silently treating a None as a non-terminal state.
    """
    state = job.state
    if state is None or state.name is None:
        msg = f"Gemini batch {getattr(job, 'name', '?')!r} returned without state"
        raise RuntimeError(msg)
    return cast("str", state.name)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception(_is_transient_api_error),
    # tenacity types `before_sleep_log` against its own LoggerProtocol; the
    # stdlib Logger is structurally compatible but the type stubs disagree.
    before_sleep=before_sleep_log(cast("Any", logger), logging.WARNING),
    reraise=True,
)
def _generate_with_retry(client: Any, model: str, contents: str, config: Any) -> Any:
    """Wrap `models.generate_content` in exponential-backoff retry.

    Retries up to 5 attempts on transient API errors (429, 5xx) with
    exponentially increasing waits up to 30s plus jitter. After the final
    attempt the exception propagates and the caller records an error row.
    Non-transient errors (400, 401, 403, schema validation) are not retried.
    """
    return client.models.generate_content(model=model, contents=contents, config=config)


class GeminiClient(TrustLLMClient):
    """Trust-signal client using the Gemini Batch API (50% off, 24h SLA)."""

    def __init__(self, api_key: str | None = None) -> None:
        key = api_key or os.environ.get("GOOGLE_API_KEY")
        if not key:
            msg = "GOOGLE_API_KEY env var is required for GeminiClient"
            raise RuntimeError(msg)
        self._client = genai.Client(api_key=key)

    def submit_batch(
        self,
        bundle: VersionBundle,
        requests: list[TrustLLMRequest],
        *,
        batch_mode: bool = True,
    ) -> list[TrustLLMResponse]:
        if not requests:
            return []
        if batch_mode:
            return self._submit_via_batch_api(bundle, requests)
        return self._submit_sync(bundle, requests)

    def _submit_sync(
        self,
        bundle: VersionBundle,
        requests: list[TrustLLMRequest],
    ) -> list[TrustLLMResponse]:
        """Concurrent sync calls — fast iteration for small dev runs.

        Each request is one ``models.generate_content`` call; ``ThreadPoolExecutor``
        gives concurrency up to :data:`_SYNC_MAX_WORKERS`. Use this when batch
        latency (5-15 min queue + 24h SLA) outweighs the 50% batch discount.
        """
        config = types.GenerateContentConfig(
            system_instruction=bundle.prompts.system,
            temperature=bundle.generation.temperature,
            max_output_tokens=bundle.generation.max_output_tokens,
            response_mime_type=bundle.generation.response_mime_type,
        )

        def _one(req: TrustLLMRequest) -> TrustLLMResponse:
            try:
                resp = _generate_with_retry(
                    self._client,
                    bundle.model,
                    req.user_prompt,
                    config,
                )
                return TrustLLMResponse(key=req.key, raw_text=resp.text, error=None)
            except Exception as exc:
                # Per-row failure (after retry exhaustion or non-transient
                # error) is recorded as a -1 score so a re-run can retry it.
                return TrustLLMResponse(key=req.key, raw_text=None, error=str(exc))

        results: dict[str, TrustLLMResponse] = {}
        # logging_redirect_tqdm routes log records through tqdm.write so
        # tenacity retry-warnings (and any other INFO/WARNING) don't shred
        # the progress bar — the bar lifts, the line prints, the bar redraws.
        with (
            logging_redirect_tqdm(),
            ThreadPoolExecutor(max_workers=_SYNC_MAX_WORKERS) as pool,
            tqdm(total=len(requests), desc="Gemini sync") as pbar,
        ):
            futures = {pool.submit(_one, req): req for req in requests}
            for fut in as_completed(futures):
                resp = fut.result()
                results[resp.key] = resp
                pbar.update(1)
        return [
            results.get(
                req.key,
                TrustLLMResponse(
                    key=req.key, raw_text=None, error="missing future result"
                ),
            )
            for req in requests
        ]

    def _submit_via_batch_api(
        self,
        bundle: VersionBundle,
        requests: list[TrustLLMRequest],
    ) -> list[TrustLLMResponse]:
        with tempfile.TemporaryDirectory() as tmp:
            jsonl_path = Path(tmp) / "trust_batch_input.jsonl"
            self._write_jsonl(jsonl_path, bundle, requests)

            uploaded = self._client.files.upload(
                file=str(jsonl_path),
                config=types.UploadFileConfig(
                    display_name=f"trust-{bundle.signal_kind}-{len(requests)}",
                    mime_type="jsonl",
                ),
            )
            uploaded_name = uploaded.name
            if uploaded_name is None:
                msg = "Gemini files.upload returned without a name"
                raise RuntimeError(msg)
            logger.info(
                "Uploaded %d-request batch input as %s",
                len(requests),
                uploaded_name,
            )

            job = self._client.batches.create(
                model=bundle.model,
                src=uploaded_name,
                config={"display_name": f"trust-{bundle.signal_kind}"},
            )
            job_name = job.name
            if job_name is None:
                msg = "Gemini batches.create returned without a name"
                raise RuntimeError(msg)
            logger.info("Created Gemini batch job %s", job_name)

            job = self._poll_until_done(job_name)
            return self._collect_results(job, requests)

    def _write_jsonl(
        self,
        path: Path,
        bundle: VersionBundle,
        requests: list[TrustLLMRequest],
    ) -> None:
        # Note: the yml's `response_schema` is intentionally NOT forwarded to
        # Gemini. Gemini's REST API requires a Gemini-flavoured Schema (uppercase
        # type enums, snake_case constraint names) which is not 1:1 with
        # standard JSON Schema. For v1 we rely on `response_mime_type` +
        # explicit prompt instructions + pydantic validation on our side. The
        # schema in the yml still documents intent and contributes to
        # `config_hash`, so prompt/schema edits still force a re-judge.
        gen_config: dict[str, Any] = {
            "temperature": bundle.generation.temperature,
            "max_output_tokens": bundle.generation.max_output_tokens,
            "response_mime_type": bundle.generation.response_mime_type,
        }
        with path.open("w", encoding="utf-8") as f:
            for req in requests:
                record = {
                    "key": req.key,
                    "request": {
                        "contents": [
                            {"parts": [{"text": req.user_prompt}], "role": "user"}
                        ],
                        "system_instruction": {
                            "parts": [{"text": bundle.prompts.system}]
                        },
                        "generation_config": gen_config,
                    },
                }
                f.write(json.dumps(record, ensure_ascii=False))
                f.write("\n")

    def _poll_until_done(self, name: str) -> Any:
        # tqdm shows a spinner with the current state + elapsed time. Gemini
        # doesn't expose per-row progress for a running batch, so this is a
        # status indicator rather than a percentage bar.
        bar_format = "{desc} [{elapsed}] {postfix}"
        with (
            logging_redirect_tqdm(),
            tqdm(
                desc=f"Gemini batch {name}",
                bar_format=bar_format,
                leave=True,
            ) as pbar,
        ):
            job = self._client.batches.get(name=name)
            state_name = _job_state_name(job)
            pbar.set_postfix_str(state_name)
            while state_name not in _COMPLETED_STATES:
                time.sleep(_POLL_INTERVAL_S)
                pbar.update(0)  # refresh the elapsed clock without advancing
                job = self._client.batches.get(name=name)
                state_name = _job_state_name(job)
                pbar.set_postfix_str(state_name)
        if state_name != "JOB_STATE_SUCCEEDED":
            msg = f"Gemini batch {name} ended in {state_name}"
            raise RuntimeError(msg)
        return job

    def _collect_results(
        self,
        job: Any,
        requests: list[TrustLLMRequest],
    ) -> list[TrustLLMResponse]:
        if not (job.dest and job.dest.file_name):
            msg = f"Gemini batch {job.name} succeeded but produced no output file"
            raise RuntimeError(msg)

        raw = self._client.files.download(file=job.dest.file_name)
        by_key: dict[str, TrustLLMResponse] = {}
        for line in raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            key = rec.get("key", "")
            err = rec.get("error")
            if err:
                by_key[key] = TrustLLMResponse(key=key, raw_text=None, error=str(err))
                continue
            by_key[key] = TrustLLMResponse(
                key=key,
                raw_text=_extract_text(rec.get("response") or {}),
                error=None,
            )

        return [
            by_key.get(
                req.key,
                TrustLLMResponse(
                    key=req.key,
                    raw_text=None,
                    error="missing in batch output",
                ),
            )
            for req in requests
        ]


def _extract_text(response: dict[str, Any]) -> str | None:
    """Pull the first candidate's first text part from a Gemini response."""
    candidates = response.get("candidates") or []
    if not candidates:
        return None
    parts = (candidates[0].get("content") or {}).get("parts") or []
    if not parts:
        return None
    text = parts[0].get("text")
    return text if isinstance(text, str) else None
