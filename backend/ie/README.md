# FoodAtlas Information Extraction

End-to-end pipeline for discovering food-chemical relationships from the biomedical literature. Starting from a list of food terms, the pipeline searches PubMed/PMC, filters candidate sentences with a fine-tuned BioBERT model, and extracts structured triplets (`food, food_part, chemical, concentration`) using an LLM (GPT-4 / GPT-3.5-ft / GPT-5.2).

---

## Project Structure

```
ie/
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
│   ├── run_pipeline.sh              # SLURM orchestrator (main entry point)
│   └── train_biobert_binary.sh      # BioBERT training job
├── src/
│   └── Lit2_KG/
│       ├── 0_update_PMC_BioC.py                       # Step 0: update local BioC-PMC corpus
│       ├── 1_search_pubmed_pmc.py                     # Step 1: search + retrieve sentences
│       ├── 2_run_sentence_filtering.py                # Step 2: BioBERT inference
│       ├── 3_aggregate_sentence_filtering_results.py  # Step 3: aggregate + dedup
│       ├── 4_run_infomation_extraction.py             # Step 4: LLM extraction
│       ├── 5_parse_text_parser_predictions.py         # Step 5: parse LLM output
│       ├── information_extraction_model_config.py     # LLM prompt config
│       ├── biobert/                                   # BioBERT model + training code
│       └── openai/                                    # OpenAI batch API wrapper
└── pyproject.toml
```

---

## Setup

### 1. Conda environment

The pipeline runs inside the `foodatlas_pipeline` conda environment:

```bash
conda create -n foodatlas_pipeline python=3.12
conda activate foodatlas_pipeline
pip install datasets nltk numpy openai pandas requests scikit-learn thefuzz torch tqdm transformers
```

### 2. Download the BioBERT model

The fine-tuned BioBERT binary classifier must be placed at `outputs/biobert_binary_prod/`. Run the download script from the `ie/` directory:

```bash
bash outputs/biobert_binary_prod/download.sh
```

Downloads `biobert_binary_prod.zip` (~383 MB) from Box and extracts it in place.

### 3. Download historical LLM predictions

Past prediction files are required by Step 3 to deduplicate sentences already processed in prior runs:

```bash
bash outputs/past_sentence_filtering_preds/download.sh
```

Downloads `text_parser_predictions.zip` (~89 MB) from Box, which contains:

| File | Model | Run date |
|---|---|---|
| `text_parser_predicted_2024_02_25_gpt-4.tsv/.json` | GPT-4 | 2024-02-25 |
| `text_parser_predicted_2024_07_11_gpt-3.5-ft.tsv/.json` | GPT-3.5-ft | 2024-07-11 |
| `text_parser_predicted_2026_02_17_gpt-5.2.tsv/.json` | GPT-5.2 | 2026-02-17 |

### 4. API keys

```bash
export NCBI_API_KEY=<your_key>     # Optional but recommended — avoids PubMed rate limits
export OPENAI_API_KEY=<your_key>   # Required for Step 4 (LLM extraction)
```

---

## Running the Pipeline

The pipeline is orchestrated via SLURM. Each step is submitted as a dependent job so they run in strict sequence:

```bash
bash scripts/run_pipeline.sh [DATE] [MODEL_NAME]
```

| Argument | Default | Description |
|---|---|---|
| `DATE` | today (`YYYY_MM_DD`) | Run tag; creates `outputs/text_parser/{DATE}/` |
| `MODEL_NAME` | `gpt-5.2` | LLM model for Step 4 (`gpt-4`, `gpt-3.5-ft`, `gpt-5.2`) |

**Example:**
```bash
bash scripts/run_pipeline.sh 2026_03_23 gpt-5.2
```

SLURM logs are written to `scripts/logs/`.

> **Note:** Steps 2–6 in `run_pipeline.sh` are commented out by default. Uncomment the steps you want to run before executing the script.

---

## Pipeline Steps

### Step 0 — Download PMC ID map

Downloads the latest `PMC-ids.csv.gz` from NCBI FTP, decompresses it into `data/NCBI/`. This file maps PMC IDs to PubMed IDs and is required by Step 1. The directory is cleared and recreated on every run to ensure a fresh mapping.


### Step 1 — Update BioC-PMC corpus
`src/Lit2_KG/0_update_PMC_BioC.py`

Incrementally downloads new BioC-PMC article archives from NCBI FTP into `/mnt/data/shared/BioC-PMC`. Only archives containing articles newer than what is already stored locally are downloaded, so subsequent runs are fast.


### Step 2 — Search PubMed / PMC
`src/Lit2_KG/1_search_pubmed_pmc.py`

For each food term in `data/food_terms.txt`, queries PubMed for matching article IDs, retrieves full-text sentences from the local BioC-PMC corpus, and applies fuzzy matching to retain sentences likely to contain food-chemical relationships. Results are written as chunked TSV files, then merged into a single file for BioBERT.

Key outputs in `outputs/text_parser/{DATE}/retrieved_sentences/`:
- `result_{i}.tsv` — chunked sentence batches
- `sentence_filtering_input.tsv` — merged input for BioBERT


### Step 3 — BioBERT sentence filtering
`src/Lit2_KG/2_run_sentence_filtering.py`

Runs the fine-tuned BioBERT binary classifier over `sentence_filtering_input.tsv` to score each sentence as food-chemical relevant or not. Sentences are processed in chunks of 10,000 to manage memory.

Key arguments:
- `--model_dir outputs/biobert_binary_prod`
- `--chunk_size 10000`
- `--batch_size 64`

Output: `outputs/text_parser/{DATE}/sentence_filtering/*.tsv`


### Step 4 — Aggregate & deduplicate
`src/Lit2_KG/3_aggregate_sentence_filtering_results.py`

Merges all BioBERT chunk outputs, applies a confidence threshold (default `0.99`), then deduplicates against all files in `outputs/past_sentence_filtering_preds/` to exclude sentences already processed in prior runs.

Key outputs in `outputs/text_parser/{DATE}/filtered_sentences/`:
- `filtered_sentence_aggregated.tsv` — all sentences passing the threshold
- `information_extraction_input.tsv` — deduplicated, ready for LLM


### Step 5 — LLM information extraction
`src/Lit2_KG/4_run_infomation_extraction.py`

Sends each sentence to the OpenAI API using the prompt defined in `information_extraction_model_config.py`. The LLM extracts structured triplets:

```
food, food_part, chemical, chemical_concentration
```

Food and chemical are required; food part and concentration are optional. Results are saved to `outputs/past_sentence_filtering_preds/{DATE}_prediction_batch/`.


### Step 6 — Parse predictions
`src/Lit2_KG/5_parse_text_parser_predictions.py`

Parses raw LLM outputs into clean TSV and JSON files, normalising the triplet format across all supported models.

Output in `outputs/past_sentence_filtering_preds/`:
- `text_parser_predicted_{DATE}_{MODEL}.tsv`
- `text_parser_predicted_{DATE}_{MODEL}.json`


---

## Training BioBERT (optional)

To retrain the sentence classifier on new annotations:

```bash
sbatch scripts/train_biobert_binary.sh
```

Two modes are configured inside the script:
- **Development** (commented out): uses train/val/test splits, saves best checkpoint by validation F1 to `outputs/biobert_binary/`.
- **Production** (default): trains on all data for 9 epochs, saves final model to `outputs/biobert_binary_prod/`.
