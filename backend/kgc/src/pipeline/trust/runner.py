"""TrustRunner — score attestations using a trust-signal LLM judge.

Loads :class:`KnowledgeGraph` for attestations + evidence, reads any existing
``trust_signals.parquet``, selects the to-judge set (no row yet for this
``(signal_kind, version, config_hash)`` OR existing row has ``score < 0``),
calls the configured :class:`TrustLLMClient`, validates JSON output, and
rewrites ``trust_signals.parquet`` so it always represents the latest "best
knowledge" — no duplicate signal_ids per ``(signal_kind, version,
config_hash)``.

Errors (LLM/transport) are recorded as rows with ``score = -1`` and a
non-empty ``error_text`` so they can be retried by the next run; on success
the upsert in the DB loader overwrites the prior error row.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import ValidationError

from ...models.trust_signal import LLMPlausibilityResponse, TrustSignal
from ..checkpoint import load_checkpoint
from ..knowledge_graph import KnowledgeGraph
from .llm import create_client
from .llm.base import TrustLLMRequest, TrustLLMResponse
from .versions import VersionBundle, load_version

if TYPE_CHECKING:
    from ...models.settings import KGCSettings, TrustStageConfig

logger = logging.getLogger(__name__)

FILE_TRUST_SIGNALS = "trust_signals.parquet"

_TRUST_COLUMNS = list(TrustSignal.model_fields.keys())


def signal_id(
    attestation_id: str,
    signal_kind: str,
    version: str,
    config_hash: str,
) -> str:
    """Content-addressed signal id: dedup key for a (att, kind, ver, cfg) tuple."""
    key = f"{attestation_id}|{signal_kind}|{version}|{config_hash}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


class TrustRunner:
    """Orchestrate the TRUST stage: load, judge, persist."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings
        self._cfg: TrustStageConfig = settings.pipeline.stages.trust

    def run(self) -> None:
        kg_dir = Path(self._settings.kg_dir)
        load_checkpoint(kg_dir, "enrichment")
        bundle, config_hash = load_version(self._cfg.signal, self._cfg.version)
        logger.info(
            "Trust stage: signal=%s version=%s model=%s config_hash=%s",
            bundle.signal_kind,
            self._cfg.version,
            bundle.model,
            config_hash[:12],
        )

        kg = KnowledgeGraph(self._settings)

        existing = _read_existing(kg_dir / FILE_TRUST_SIGNALS)
        retry_ids = self._select_attestations(kg, existing, bundle, config_hash)
        if not retry_ids:
            logger.info("Trust stage: no attestations to judge; exiting.")
            return

        requests = self._build_requests(kg, retry_ids, bundle)
        if not requests:
            logger.info("Trust stage: 0 valid requests after rendering; exiting.")
            return

        if self._cfg.limit is not None:
            requests = requests[: self._cfg.limit]
            logger.info(
                "Trust stage: capped to %d requests by config.limit",
                len(requests),
            )

        client = create_client(bundle.provider)
        responses = client.submit_batch(
            bundle, requests, batch_mode=self._cfg.batch_mode
        )

        new_rows = _responses_to_rows(
            responses,
            requests_by_key={r.key: r for r in requests},
            bundle=bundle,
            version=self._cfg.version,
            config_hash=config_hash,
        )

        merged = _merge_for_run(
            existing,
            new_rows,
            bundle.signal_kind,
            self._cfg.version,
            config_hash,
        )
        out_path = kg_dir / FILE_TRUST_SIGNALS
        merged.to_parquet(out_path, index=False)
        logger.info(
            "Trust stage: wrote %d rows to %s (this run: %d new/retried)",
            len(merged),
            out_path,
            len(new_rows),
        )

    def _select_attestations(
        self,
        kg: KnowledgeGraph,
        existing: pd.DataFrame,
        bundle: VersionBundle,
        config_hash: str,
    ) -> list[str]:
        """Return attestation IDs to judge in this run.

        Selection rule:
        - If no existing row for (signal_kind, version, config_hash, att_id) → judge it.
        - If the existing row has score < 0 (a prior error) → judge it (will overwrite).
        - Otherwise (existing row with score >= 0) → skip.
        Optionally filtered by ``source_filter`` from the stage config.
        """
        att_df: pd.DataFrame = kg.attestations._records
        if att_df.empty:
            return []

        candidate = att_df
        if self._cfg.source_filter:
            prefixes = tuple(self._cfg.source_filter)
            candidate = candidate[candidate["source"].str.startswith(prefixes)]
            logger.info(
                "Trust stage: source_filter=%r → %d candidate attestations",
                self._cfg.source_filter,
                len(candidate),
            )

        # Attestations whose conc_value is unparseable contribute no signal.
        candidate = candidate[candidate["conc_value"].notna()]

        if existing.empty:
            return list(candidate.index)

        run_existing = existing[
            (existing["signal_kind"] == bundle.signal_kind)
            & (existing["version"] == self._cfg.version)
            & (existing["config_hash"] == config_hash)
        ]
        done_ids = set(run_existing[run_existing["score"] >= 0]["attestation_id"])
        return [aid for aid in candidate.index if aid not in done_ids]

    def _build_requests(
        self,
        kg: KnowledgeGraph,
        attestation_ids: list[str],
        bundle: VersionBundle,
    ) -> list[TrustLLMRequest]:
        att_df: pd.DataFrame = kg.attestations._records
        ev_df: pd.DataFrame = kg.evidence._records
        rows = att_df.loc[attestation_ids]

        requests: list[TrustLLMRequest] = []
        for att_id, row in rows.iterrows():
            sentence = _resolve_sentence(ev_df, str(row["evidence_id"]))
            if sentence is None:
                logger.debug("Skipping %s: no source sentence in evidence", att_id)
                continue
            try:
                user_prompt = bundle.prompts.user.format(
                    food=row.get("head_name_raw", ""),
                    chemical=row.get("tail_name_raw", ""),
                    conc_value=row["conc_value"],
                    conc_value_raw=row.get("conc_value_raw", ""),
                    conc_unit_raw=row.get("conc_unit_raw", ""),
                    sentence=sentence,
                )
            except KeyError:
                logger.exception(
                    "Prompt template references unknown field for %s",
                    att_id,
                )
                continue
            requests.append(TrustLLMRequest(key=str(att_id), user_prompt=user_prompt))
        return requests


def _read_existing(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=_TRUST_COLUMNS)
    return pd.read_parquet(path)


def _resolve_sentence(ev_df: pd.DataFrame, evidence_id: str) -> str | None:
    if ev_df.empty or evidence_id not in ev_df.index:
        return None
    raw = ev_df.loc[evidence_id, "reference"]
    try:
        ref: dict[str, Any] = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    text = ref.get("text")
    return text if isinstance(text, str) and text else None


def _responses_to_rows(
    responses: list[TrustLLMResponse],
    *,
    requests_by_key: dict[str, TrustLLMRequest],
    bundle: VersionBundle,
    version: str,
    config_hash: str,
) -> pd.DataFrame:
    now = datetime.now(UTC)
    model_id = f"{bundle.provider}:{bundle.model}"
    records: list[dict[str, Any]] = []
    for resp in responses:
        if resp.key not in requests_by_key:
            continue  # provider returned an unknown key — ignore
        if resp.error or resp.raw_text is None:
            score = -1.0
            reason = ""
            error_text = resp.error or "no response text"
        else:
            try:
                parsed = LLMPlausibilityResponse.model_validate_json(resp.raw_text)
                score = parsed.score
                reason = parsed.reason
                error_text = ""
            except ValidationError as exc:
                score = -1.0
                reason = ""
                error_text = f"response failed schema: {exc!s}"

        sid = signal_id(resp.key, bundle.signal_kind, version, config_hash)
        records.append(
            {
                "signal_id": sid,
                "attestation_id": resp.key,
                "signal_kind": bundle.signal_kind,
                "version": version,
                "config_hash": config_hash,
                "model": model_id,
                "score": score,
                "reason": reason,
                "error_text": error_text,
                "created_at": now,
            }
        )
    return pd.DataFrame.from_records(records, columns=_TRUST_COLUMNS)


def _merge_for_run(
    existing: pd.DataFrame,
    new_rows: pd.DataFrame,
    signal_kind: str,
    version: str,
    config_hash: str,
) -> pd.DataFrame:
    """Drop any existing rows for this run that are being replaced; append new."""
    if existing.empty:
        return new_rows
    if new_rows.empty:
        return existing
    new_keys = set(new_rows["signal_id"])
    keep_mask = ~(
        (existing["signal_kind"] == signal_kind)
        & (existing["version"] == version)
        & (existing["config_hash"] == config_hash)
        & (existing["signal_id"].isin(new_keys))
    )
    return pd.concat([existing[keep_mask], new_rows], ignore_index=True)
