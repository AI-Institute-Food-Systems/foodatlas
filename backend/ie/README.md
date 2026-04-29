# FoodAtlas Information Extraction

End-to-end pipeline for extracting food–chemical relationships from the biomedical literature. Starting from a list of food terms, the pipeline searches PubMed/PMC, filters candidate sentences with a fine-tuned BioBERT classifier, and extracts structured triplets (`food, food_part, chemical, concentration`) using an LLM via the OpenAI Batch API.

The output TSV is consumed by KGC's `ie` stage (see [`backend/kgc/src/pipeline/ie/`](../kgc/src/pipeline/ie/)).

---

## Setup

```bash
cd backend/ie
uv sync
```

### Required assets

The fine-tuned BioBERT binary classifier and the historical prediction archive aren't checked in. Fetch them with the bundled `download.sh` scripts:

```bash
bash scripts/biobert_binary_prod/download.sh
bash scripts/past_sentence_filtering_preds/download.sh
```

### API keys

```bash
export NCBI_API_KEY=<your_key>     # Optional — raises PubMed rate limits
export NCBI_EMAIL=<your_email>     # Required by NCBI E-utilities
export OPENAI_API_KEY=<your_key>   # Required for the extraction stage
```

A `.env` at the project root or `backend/ie/.env` is also picked up automatically.

---

## Running the Pipeline

```bash
cd backend/ie

# List available stages
uv run python main.py stages

# Run all stages
uv run python main.py run

# Run a single stage (by name or number)
uv run python main.py run --stages corpus
uv run python main.py run --stages 0

# Run a range
uv run python main.py run --stages 1:3      # search → filtering → extraction
```

The `--config` flag on the top-level group lets you point at a JSON file that overrides `src/config/defaults.json`. Field names match the `IESettings` model (env prefix `IE_`).

For a full monthly run, the canonical orchestrator is [`infra/local/scripts/run_monthly.sh`](../../infra/local/scripts/run_monthly.sh), which chains IE → KGC → DB → S3. On SLURM clusters, wrap individual stages in `sbatch --wrap="cd backend/ie && uv run python main.py run --stages <stage>"` with stage-appropriate resources (GPU for `filtering`, CPU elsewhere).

---

## Pipeline Stages

| # | Stage | Module | Description |
|---|-------|--------|-------------|
| 0 | `CORPUS` | `src/pipeline/corpus/` | Refresh the local BioC-PMC corpus and the `PMC-ids.csv` mapping |
| 1 | `SEARCH` | `src/pipeline/search/` | Query PubMed for each food term, retrieve and fuzzy-match sentences from BioC-PMC |
| 2 | `FILTERING` | `src/pipeline/filtering/` | BioBERT binary classifier (GPU) + threshold + dedup against historical predictions |
| 3 | `EXTRACTION` | `src/pipeline/extraction/` | Submit filtered sentences to the OpenAI Batch API; parse responses into `extraction_predicted.tsv` |

Per-stage runners live at `src/pipeline/<stage>/runner.py`. The top-level `IERunner` (`src/pipeline/runner.py`) dispatches them in order.

The extraction stage reads its prompts from `src/pipeline/extraction/prompts/{system,user}/v1.txt` so prompt changes are versioned alongside the code.

---

## Configuration

Defaults live in `src/config/defaults.json` and load into the `IESettings` Pydantic model. CLI flags / env vars (`IE_*`) override the file. Key fields:

| Setting | Default | Description |
|---|---|---|
| `date` | today (UTC) | Run date tag (`YYYY_MM_DD`) used in output paths |
| `model` | `gpt-5.2` | LLM model for extraction |
| `bioc_pmc_dir` | (set in defaults) | Path to the local BioC-PMC corpus |
| `biobert_model_dir` | (set in defaults) | Path to the fine-tuned BioBERT classifier |
| `food_terms` | `data/food_terms.txt` | Food query terms file |
| `pipeline.biobert_filter.threshold` | `0.99` (under `aggregate`) | BioBERT confidence cutoff |
| `pipeline.aggregate.reference_dir` | `outputs/extraction` | Historical predictions for sentence dedup |
| `pipeline.extraction.temperature` | `0.0` | LLM sampling temperature |

---

## Project Structure

```
ie/
├── main.py                          # Click CLI entry point
├── pyproject.toml
├── data/
│   ├── food_terms.txt               # Query terms used to search PubMed/PMC
│   └── translated_food_terms.txt    # Translated/aliased food terms
├── outputs/                         # Per-stage outputs (created at runtime)
│   ├── corpus/                      #   Refreshed PMC IDs + BioC archives
│   ├── search/{date}/               #   Search results + retrieved sentences
│   ├── filtering/{date}/            #   BioBERT predictions + aggregated TSVs
│   └── extraction/{date}/           #   Batch input + parsed predictions
├── scripts/
│   ├── train_biobert_binary.sh      # BioBERT training job (SLURM)
│   ├── biobert_binary_prod/
│   │   └── download.sh              # Fetch the fine-tuned classifier
│   └── past_sentence_filtering_preds/
│       └── download.sh              # Fetch historical predictions for dedup
├── src/
│   ├── config/defaults.json         # Default configuration
│   ├── models/settings.py           # IESettings (Pydantic)
│   └── pipeline/
│       ├── stages.py                # IEStage enum
│       ├── runner.py                # Top-level IERunner
│       ├── corpus/                  # Stage 0
│       ├── search/                  # Stage 1 — pubmed_search.py + sentence_retrieval.py
│       ├── filtering/               # Stage 2 — biobert/ + aggregate.py
│       └── extraction/              # Stage 3 — openai/, prompts/, parse_predictions.py
└── tests/
```

---

## Training BioBERT (optional)

```bash
cd backend/ie
uv run python -m src.pipeline.filtering.biobert.train --output_dir outputs/biobert_binary_prod --production
```

Or via SLURM:

```bash
sbatch scripts/train_biobert_binary.sh
```
