# Story 1-9: Pipeline Runner + CLI

## Goal

Replace the 7 numbered shell scripts with a Python pipeline runner and Click CLI entry point. This makes the pipeline reproducible, configurable, and deployable.

## Depends On

- Stories 1-1 through 1-8 (all modules must be ported)

## Acceptance Criteria

- [ ] `pipeline/stages.py` — `PipelineStage` enum defining all stages:
  - `DATA_PROCESSING` (stage 00)
  - `KG_INIT` (stage 0)
  - `METADATA_PROCESSING` (stage 1)
  - `TRIPLET_EXPANSION` (stage 2)
  - `POSTPROCESSING` (stage 3)
  - `MERGE_DISEASE` (stage 4)
  - `MERGE_FLAVOR` (stage 5)
- [ ] `pipeline/runner.py` — `PipelineRunner` class
  - `__init__(settings: KGCSettings)`
  - `run(stages: list[PipelineStage] | None = None)` — run all or selected stages
  - `run_stage(stage: PipelineStage)` — run a single stage
  - Logging for stage start/end/duration
  - Validation test call after Stage 2 (matching original behavior)
- [ ] `main.py` — Click CLI with commands:
  - `run` — run pipeline stages (`--stage` option, repeatable)
  - `init` — shortcut for KG initialization only
  - `--config` option to specify `config/defaults.json` override
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - Stage enum completeness
  - Runner executes stages in order
  - CLI parses arguments correctly
  - `--stage` filtering works

## Source Files

| Target | Source |
|--------|--------|
| `pipeline/stages.py` | New (replaces `scripts/0_run_kg_init.sh` through `scripts/5_run_merging_flavor.sh`) |
| `pipeline/runner.py` | New (orchestration logic extracted from shell scripts) |
| `main.py` | Replaces existing stub |

## Notes

- Each shell script maps to one `PipelineStage`. The runner calls the same Python functions the scripts called via `python -m food_atlas.kg.run_*`.
- The runner should produce `version.json` at the end of a full pipeline run.
- **Input format**: JSON preferred, pkl supported for backward compatibility. `--input` accepts path to JSON or pkl file from IE extraction. Auto-detect format from file extension.
- **Output format**: configurable via `--output-format` (json, jsonl, parquet). Default: jsonl. Controlled by `KGCSettings.output_format`.
- Internal pipeline stages can still use DataFrames; serialization to the chosen format happens at the final output step.
