"""OpenAI runner for LLM filter inference (batch or synchronous)."""

import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from openai import OpenAI


class OpenAIRunner:
    """Run inference via OpenAI, using Batch API or parallel sync calls."""

    DEFAULT_MODEL = "gpt-4.1"

    def __init__(self, config, model_name=None, api_key=None, use_batch=True,
                 max_workers=32):
        self._system_prompt = config.SYSTEM_PROMPT
        self._model_name = model_name or self.DEFAULT_MODEL
        self._max_tokens = getattr(config, "MAX_NEW_TOKENS", 1)
        self._temperature = getattr(config, "TEMPERATURE", 0.0)
        api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._client = OpenAI(api_key=api_key)
        self._use_batch = use_batch
        self._max_workers = max_workers

    def _build_batch_line(self, idx, prompt):
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

    def _create_batch_file(self, prompts, path):
        with open(path, "w", encoding="utf-8") as f:
            for i, prompt in enumerate(prompts):
                f.write(json.dumps(self._build_batch_line(i, prompt), ensure_ascii=False) + "\n")

    def _upload_and_run(self, input_path, output_path):
        print(f"Model: {self._model_name}")
        print("Uploading batch input file...")
        batch_input_file = self._client.files.create(
            file=open(input_path, "rb"),
            purpose="batch",
        )
        print("Creating batch job...")
        batch_job = self._client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        print("Waiting for batch job to complete...")
        batch_result = self._client.batches.retrieve(batch_job.id)
        while batch_result.status not in ("completed", "failed"):
            print(f"  Status: {batch_result.status}")
            time.sleep(10)
            batch_result = self._client.batches.retrieve(batch_job.id)

        if batch_result.status == "failed":
            raise RuntimeError(f"Batch job failed: {batch_result}")

        result_file_id = batch_result.output_file_id
        with open(output_path, "wb") as f:
            f.write(self._client.files.content(result_file_id).content)

    def _infer_sync(self, prompts):
        """Send all prompts in parallel using synchronous chat completions."""
        print(f"Model: {self._model_name}")
        print(f"Running {len(prompts)} requests with max_workers={self._max_workers}...")

        def call(idx, prompt):
            response = self._client.chat.completions.create(
                model=self._model_name,
                messages=[
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=self._max_tokens,
                temperature=self._temperature,
            )
            return idx, response.choices[0].message.content.strip()

        results = {}
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {executor.submit(call, i, p): i for i, p in enumerate(prompts)}
            done = 0
            for future in as_completed(futures):
                idx, content = future.result()
                results[idx] = content
                done += 1
                if done % 100 == 0 or done == len(prompts):
                    print(f"  {done}/{len(prompts)} done")
        return [results[i] for i in range(len(prompts))]

    def infer(self, prompts):
        """Run inference on a list of prompts.

        Uses the Batch API by default (cheaper, slower). Pass use_batch=False
        for parallel synchronous calls (faster, full price).

        Args:
            prompts (list[str]): User-turn text for each sample.

        Returns:
            list[str]: Generated text for each prompt, in original order.
        """
        if not self._use_batch:
            return self._infer_sync(prompts)

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "batch_input.jsonl")
            output_path = os.path.join(tmpdir, "batch_output.jsonl")

            self._create_batch_file(prompts, input_path)
            self._upload_and_run(input_path, output_path)

            results = {}
            with open(output_path, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    idx = int(obj["custom_id"].split("_")[1])
                    content = obj["response"]["body"]["choices"][0]["message"]["content"]
                    results[idx] = content.strip()

        return [results[i] for i in range(len(prompts))]
