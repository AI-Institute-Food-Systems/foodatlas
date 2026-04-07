"""OpenAI runner for LLM filter inference (batch or synchronous)."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import ModuleType

from openai import OpenAI

log = logging.getLogger(__name__)


class OpenAIRunner:
    """Run inference via OpenAI, using Batch API or parallel sync calls."""

    DEFAULT_MODEL = "gpt-4.1"

    def __init__(
        self,
        config: ModuleType,
        model_name: str | None = None,
        api_key: str | None = None,
        use_batch: bool = True,
        max_workers: int = 32,
    ) -> None:
        self._system_prompt: str = config.SYSTEM_PROMPT
        self._model_name = model_name or self.DEFAULT_MODEL
        self._max_tokens: int = getattr(config, "MAX_NEW_TOKENS", 1)
        self._temperature: float = getattr(config, "TEMPERATURE", 0.0)
        resolved_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._client = OpenAI(api_key=resolved_key)
        self._use_batch = use_batch
        self._max_workers = max_workers

    def _build_batch_line(
        self,
        idx: int,
        prompt: str,
    ) -> dict[str, Any]:
        return {
            "custom_id": f"row_{idx}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": self._model_name,
                "messages": [
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": self._max_tokens,
                "temperature": self._temperature,
            },
        }

    def _create_batch_file(
        self,
        prompts: list[str],
        path: str,
    ) -> None:
        with Path(path).open("w", encoding="utf-8") as f:
            for i, prompt in enumerate(prompts):
                line = json.dumps(
                    self._build_batch_line(i, prompt),
                    ensure_ascii=False,
                )
                f.write(line + "\n")

    def _upload_and_run(
        self,
        input_path: str,
        output_path: str,
    ) -> None:
        log.info("Model: %s", self._model_name)
        log.info("Uploading batch input file...")
        with Path(input_path).open("rb") as fh:
            batch_input_file = self._client.files.create(
                file=fh,
                purpose="batch",
            )
        log.info("Creating batch job...")
        batch_job = self._client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        log.info("Waiting for batch job to complete...")
        batch_result = self._client.batches.retrieve(batch_job.id)
        while batch_result.status not in ("completed", "failed"):
            log.info("  Status: %s", batch_result.status)
            time.sleep(10)
            batch_result = self._client.batches.retrieve(batch_job.id)

        if batch_result.status == "failed":
            msg = f"Batch job failed: {batch_result}"
            raise RuntimeError(msg)

        result_file_id = batch_result.output_file_id
        if result_file_id is None:
            msg = "Batch completed but output_file_id is None"
            raise RuntimeError(msg)
        content = self._client.files.content(result_file_id).content
        Path(output_path).write_bytes(content)

    def _infer_sync(self, prompts: list[str]) -> list[str]:
        """Send all prompts in parallel using synchronous completions."""
        log.info("Model: %s", self._model_name)
        log.info(
            "Running %d requests with max_workers=%d...",
            len(prompts),
            self._max_workers,
        )

        def call(idx: int, prompt: str) -> tuple[int, str]:
            response = self._client.chat.completions.create(
                model=self._model_name,
                messages=[
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=self._max_tokens,
                temperature=self._temperature,
            )
            text = response.choices[0].message.content or ""
            return idx, text.strip()

        results: dict[int, str] = {}
        with ThreadPoolExecutor(
            max_workers=self._max_workers,
        ) as executor:
            futures = {executor.submit(call, i, p): i for i, p in enumerate(prompts)}
            for done, future in enumerate(
                as_completed(futures),
                start=1,
            ):
                idx, content = future.result()
                results[idx] = content
                if done % 100 == 0 or done == len(prompts):
                    log.info("  %d/%d done", done, len(prompts))
        return [results[i] for i in range(len(prompts))]

    def infer(self, prompts: list[str]) -> list[str]:
        """Run inference on a list of prompts.

        Uses the Batch API by default (cheaper, slower). Pass use_batch=False
        for parallel synchronous calls (faster, full price).

        Args:
            prompts: User-turn text for each sample.

        Returns:
            Generated text for each prompt, in original order.
        """
        if not self._use_batch:
            return self._infer_sync(prompts)

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = str(Path(tmpdir) / "batch_input.jsonl")
            output_path = str(Path(tmpdir) / "batch_output.jsonl")

            self._create_batch_file(prompts, input_path)
            self._upload_and_run(input_path, output_path)

            results: dict[int, str] = {}
            with Path(output_path).open(encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    idx = int(obj["custom_id"].split("_")[1])
                    content: str = obj["response"]["body"]["choices"][0]["message"][
                        "content"
                    ]
                    results[idx] = content.strip()

        return [results[i] for i in range(len(prompts))]
