"""Ad-hoc trust-judge spot check for a specific food.

Runs the configured `llm_plausibility v1` prompt against every lit2kg
attestation whose ``head_name_raw`` exactly matches the given food, prints a
sorted summary, and DOES NOT write to ``trust_signals.parquet`` — pure
diagnostic.

Usage::

    cd backend/kgc
    uv run python scripts/spot_check_food.py tomato
    uv run python scripts/spot_check_food.py "cherry tomato"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

# Importing settings triggers the module-level load_dotenv on the root .env
# (where GOOGLE_API_KEY lives, plus any proxy overrides). Without this, a
# SOCKS proxy from the shell environment leaks into httpx and breaks Gemini.
from src.models import settings as _settings  # noqa: F401
from src.models.trust_signal import LLMPlausibilityResponse
from src.pipeline.trust.llm import create_client
from src.pipeline.trust.llm.base import TrustLLMRequest
from src.pipeline.trust.versions import load_version

KG_DIR = Path("outputs/kg")


def main(food: str) -> None:
    att = pd.read_parquet(KG_DIR / "attestations.parquet")
    ev = pd.read_parquet(KG_DIR / "evidence.parquet").set_index("evidence_id")

    mask = (
        att["source"].str.startswith("lit2kg:")
        & att["conc_value"].notna()
        & (att["head_name_raw"] == food)
    )
    matched = att[mask].set_index("attestation_id")
    print(f"Found {len(matched)} lit2kg attestations for food={food!r}")
    if matched.empty:
        return

    bundle, config_hash = load_version("llm_plausibility", "v1")
    print(f"Using {bundle.signal_kind} v1 (config_hash={config_hash[:12]})")

    requests: list[TrustLLMRequest] = []
    for att_id, row in matched.iterrows():
        raw = ev.loc[row["evidence_id"], "reference"]
        try:
            ref = json.loads(raw)
        except (TypeError, ValueError):
            continue
        sentence = ref.get("text") or ""
        if not sentence:
            continue
        user_prompt = bundle.prompts.user.format(
            food=row["head_name_raw"],
            chemical=row["tail_name_raw"],
            conc_value=row["conc_value"],
            conc_value_raw=row.get("conc_value_raw", ""),
            conc_unit_raw=row.get("conc_unit_raw", ""),
            sentence=sentence,
        )
        requests.append(TrustLLMRequest(key=str(att_id), user_prompt=user_prompt))

    print(f"Built {len(requests)} requests; running sync (16 workers)…")
    client = create_client(bundle.provider)
    responses = client.submit_batch(bundle, requests, batch_mode=False)

    rows: list[dict] = []
    for resp in responses:
        if resp.error or resp.raw_text is None:
            rows.append(
                {
                    "attestation_id": resp.key,
                    "score": -1.0,
                    "reason": "",
                    "error": resp.error or "no response",
                }
            )
            continue
        try:
            parsed = LLMPlausibilityResponse.model_validate_json(resp.raw_text)
            rows.append(
                {
                    "attestation_id": resp.key,
                    "score": parsed.score,
                    "reason": parsed.reason,
                    "error": "",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "attestation_id": resp.key,
                    "score": -1.0,
                    "reason": "",
                    "error": str(exc)[:120],
                }
            )

    df = pd.DataFrame(rows)
    extras = matched[
        ["tail_name_raw", "conc_value", "conc_value_raw", "conc_unit_raw"]
    ].reset_index()
    df = df.merge(extras, on="attestation_id", how="left")
    # Display: show the original (raw) value+unit AND the standardised mg/100g
    # value side by side, so the unit label is always unambiguous.
    df["original"] = (
        df["conc_value_raw"].astype(str).str.strip()
        + " "
        + df["conc_unit_raw"].astype(str).str.strip()
    ).str.strip()
    df = df.rename(columns={"conc_value": "mg_per_100g"})
    df = df.sort_values("score")

    pd.set_option("display.max_colwidth", 100)
    pd.set_option("display.width", 220)
    print()
    print(f"=== {food} judgments (sorted by score) ===")
    print(
        df[["tail_name_raw", "original", "mg_per_100g", "score", "reason"]].to_string(
            index=False
        )
    )
    print()
    valid = df[df["score"] >= 0]
    print(f"Score distribution ({len(valid)}/{len(df)} valid):")
    print(valid["score"].describe().round(3))
    n_err = int((df["score"] < 0).sum())
    if n_err:
        print(f"\n{n_err} error rows:")
        print(df[df["score"] < 0][["tail_name_raw", "error"]].to_string(index=False))


if __name__ == "__main__":
    food_arg = sys.argv[1] if len(sys.argv) > 1 else "tomato"
    main(food_arg)
