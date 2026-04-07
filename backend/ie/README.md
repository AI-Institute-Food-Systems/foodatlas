# FoodAtlas Information Extraction

End-to-end pipeline for discovering food-chemical relationships from the biomedical literature. Starting from a list of food terms, the pipeline searches PubMed/PMC, filters candidate sentences with a fine-tuned BioBERT model, and extracts structured triplets (`food, food_part, chemical, concentration`) using an LLM (GPT-4 / GPT-3.5-ft / GPT-5.2).

---

## Project Structure

```
ie/
├── main.py                          # Click CLI entry point
├── pyproject.toml
├── data/
│   ├── food_terms.txt               # Query terms used to search PubMed/PMC
│   ├── translated_food_terms.txt    # Translated/aliased food terms
│   └── NCBI/                        # PMC-ids.csv downloaded at runtime
├── outputs/
│   ├── biobert_binary_prod/         # Fine-tuned BioBERT sentence classifier
│   ├── past_sentence_filtering_preds/  # Historical LLM prediction files
│   └── text_parser/
│       ├── last_search_date.txt     # Date of most recent completed run
│       └── {DATE}/                  # Per-run outputs (created at runtime)
│           ├── retrieved_sentences/ # Raw PMC sentences + merged input
│           ├── sentence_filtering/  # BioBERT chunk predictions
│           └── filtered_sentences/  # Aggregated & deduplicated IE input
├── scripts/
│   ├── run_pipeline.sh              # SLURM orchestrator (wraps main.py)
│   └── train_biobert_binary.sh      # BioBERT training job
├── src/
│   ├── config/
│   │   └── defaults.json            # Default pipeline configuration
│   ├── runner.py                    # Pipeline runner (dispatches stages)
│   ├── stages.py                    # IEStage enum
│   └── lit2kg/
│       ├── 0_update_PMC_BioC.py                       # Step 1: update local BioC-PMC corpus
│       ├── 1_search_pubmed_pmc.py                     # Step 2: search + retrieve sentences
│       ├── pubmed_search.py                           #   PubMed query utilities
│       ├── sentence_retrieval.py                      #   BioC sentence extraction
│       ├── 2_run_sentence_filtering.py                # Step 3: BioBERT inference
│       ├── 3_aggregate_sentence_filtering_results.py  # Step 4: aggregate + dedup
│       ├── 4_run_information_extraction.py            # Step 5: LLM extraction
│       ├── 5_parse_text_parser_predictions.py         # Step 6: parse LLM output
│       ├── information_extraction_model_config.py     # LLM prompt config
│       ├── biobert/                                   # BioBERT model + training code
│       └── openai/                                    # OpenAI batch API wrapper
└── tests/
```

---

## Setup

### 1. Install dependencies

```bash
cd backend/ie
uv sync
```

### 2. Download the BioBERT model

The fine-tuned BioBERT binary classifier must be placed at `outputs/biobert_binary_prod/`:

```bash
bash outputs/biobert_binary_prod/download.sh
```

### 3. Download historical LLM predictions

Past prediction files are required by Step 4 (aggregate) to deduplicate sentences already processed in prior runs:

```bash
bash outputs/past_sentence_filtering_preds/download.sh
```

### 4. API keys

```bash
export NCBI_API_KEY=<your_key>     # Optional — avoids PubMed rate limits
export OPENAI_API_KEY=<your_key>   # Required for Step 5 (LLM extraction)
```

---

## Running the Pipeline

### CLI (recommended)

```bash
cd backend/ie

# List available stages
uv run python main.py stages

# Run all stages
uv run python main.py run

# Run specific stages (by number or range)
uv run python main.py run --stages 3        # BioBERT filter only
uv run python main.py run --stages 2:4      # Search + BioBERT + Aggregate
uv run python main.py run --stages 5:6      # Extract + Parse

# Override defaults
uv run python main.py run --stages 5:6 --model gpt-4 --date 2026_04_06
```

| Option | Default | Description |
|---|---|---|
| `--stages` | all | Stage number or range (e.g. `3`, `2:5`) |
| `--date` | today | Run date tag (`YYYY_MM_DD`) |
| `--model` | `gpt-5.2` | LLM model for extraction |
| `--bioc-pmc-dir` | `data/BioC-PMC` | Local BioC-PMC corpus path |
| `--biobert-model-dir` | `outputs/biobert_binary_prod` | BioBERT model path |
| `--food-terms` | `data/food_terms.txt` | Food query terms file |
| `--threshold` | `0.99` | BioBERT confidence threshold |

### SLURM

For GPU-heavy steps or long-running jobs, wrap the CLI in sbatch:

```bash
# GPU step (BioBERT)
sbatch --gres=gpu:1 --mem=32G --time=48:00:00 \
  --wrap="cd backend/ie && uv run python main.py run --stages 3"

# CPU step (aggregation)
sbatch --mem=8G --time=02:00:00 \
  --wrap="cd backend/ie && uv run python main.py run --stages 4"
```

A full SLURM orchestrator with job dependencies is also available:

```bash
bash scripts/run_pipeline.sh [DATE] [MODEL_NAME]
```

---

## Pipeline Stages

| Stage | Name | Script | Description |
|-------|------|--------|-------------|
| 0 | `DOWNLOAD_PMC_IDS` | — | Download `PMC-ids.csv.gz` from NCBI FTP |
| 1 | `UPDATE_BIOC` | `0_update_PMC_BioC.py` | Incrementally download new BioC-PMC articles |
| 2 | `SEARCH_PUBMED` | `1_search_pubmed_pmc.py` | Search PubMed, retrieve and filter sentences |
| 3 | `BIOBERT_FILTER` | `2_run_sentence_filtering.py` | BioBERT binary classification (GPU) |
| 4 | `AGGREGATE` | `3_aggregate_sentence_filtering_results.py` | Merge chunks, threshold, deduplicate |
| 5 | `EXTRACT` | `4_run_information_extraction.py` | LLM extraction via OpenAI Batch API |
| 6 | `PARSE` | `5_parse_text_parser_predictions.py` | Parse LLM outputs into TSV/JSON |

### Step 0 — Download PMC ID map

Downloads `PMC-ids.csv.gz` from NCBI FTP into `data/NCBI/`. This maps PMC IDs to PubMed IDs and is required by Step 2.

### Step 1 — Update BioC-PMC corpus

Incrementally downloads new BioC-PMC article archives from NCBI FTP. Only archives containing articles newer than the local corpus are downloaded.

### Step 2 — Search PubMed / PMC

For each food term, queries PubMed for matching articles, retrieves full-text sentences from the local BioC-PMC corpus, and applies fuzzy matching. Outputs chunked TSV files merged into `sentence_filtering_input.tsv`.

### Step 3 — BioBERT sentence filtering (GPU)

Runs the fine-tuned BioBERT binary classifier to score sentences as food-chemical relevant. Processes in chunks of 10,000 sentences.

### Step 4 — Aggregate & deduplicate

Merges BioBERT outputs, applies confidence threshold (default 0.99), and deduplicates against historical predictions in `outputs/past_sentence_filtering_preds/`.

### Step 5 — LLM information extraction

Sends filtered sentences to the OpenAI Batch API. Extracts structured triplets: `food, food_part, chemical, concentration`.

### Step 6 — Parse predictions

Parses raw LLM batch outputs into clean TSV and JSON files.

---

## Configuration

Default pipeline parameters are in `src/config/defaults.json`. CLI flags override these defaults. Key settings:

- **`model`** — LLM model name (e.g. `gpt-4`, `gpt-3.5-ft`, `gpt-5.2`)
- **`pipeline.biobert_filter.threshold`** — BioBERT confidence cutoff
- **`pipeline.extraction.temperature`** — LLM sampling temperature
- **`pipeline.aggregate.reference_dir`** — Historical predictions for dedup

---

## Training BioBERT (optional)

```bash
cd backend/ie
uv run python -m src.lit2kg.biobert.train --output_dir outputs/biobert_binary_prod --production
```

Or via SLURM:

```bash
sbatch scripts/train_biobert_binary.sh
```

---

## Tests

```bash
cd backend/ie
uv run pytest              # 116 tests, 86% coverage
uv run ruff check src/     # Lint
uv run mypy src/           # Type check
```
