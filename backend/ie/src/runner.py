"""IE pipeline runner — executes stages sequentially."""

from __future__ import annotations

import importlib
import logging
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .stages import ALL_STAGES, IEStage

if TYPE_CHECKING:
    from pathlib import Path

log = logging.getLogger(__name__)

# Numbered filenames can't be imported normally; map to module paths.
_STEP_MODULES = {
    0: "src.lit2kg.0_update_PMC_BioC",
    1: "src.lit2kg.1_search_pubmed_pmc",
    2: "src.lit2kg.2_run_sentence_filtering",
    3: "src.lit2kg.3_aggregate_sentence_filtering_results",
    4: "src.lit2kg.4_run_information_extraction",
    5: "src.lit2kg.5_parse_text_parser_predictions",
}


@dataclass
class IEConfig:
    """Shared configuration for IE pipeline stages."""

    date: str
    model: str
    pipeline_dir: Path
    bioc_pmc_dir: str
    biobert_model_dir: str
    food_terms: str
    threshold: float

    @property
    def run_dir(self) -> Path:
        return self.pipeline_dir / "outputs" / "text_parser" / self.date

    @property
    def preds_dir(self) -> Path:
        return self.pipeline_dir / "outputs" / "past_sentence_filtering_preds"


class IERunner:
    """Execute IE pipeline stages in order."""

    def __init__(self, config: IEConfig) -> None:
        self._config = config

    def run(self, stages: list[IEStage] | None = None) -> None:
        """Run the given stages (or all if None)."""
        to_run = stages or ALL_STAGES
        stage_names = [s.name for s in to_run]
        log.info("IE pipeline starting — stages: %s", stage_names)

        start = time.time()
        for stage in to_run:
            t0 = time.time()
            log.info(">> [START] %s", stage.name)
            self._run_stage(stage)
            log.info(">> [DONE]  %s (%.1fs)", stage.name, time.time() - t0)

        log.info("IE pipeline complete (%.1fs)", time.time() - start)

    def _run_stage(self, stage: IEStage) -> None:
        dispatch = {
            IEStage.DOWNLOAD_PMC_IDS: self._step0_download_pmc_ids,
            IEStage.UPDATE_BIOC: self._step1_update_bioc,
            IEStage.SEARCH_PUBMED: self._step2_search_pubmed,
            IEStage.BIOBERT_FILTER: self._step3_biobert_filter,
            IEStage.AGGREGATE: self._step4_aggregate,
            IEStage.EXTRACT: self._step5_extract,
            IEStage.PARSE: self._step6_parse,
        }
        dispatch[stage]()

    def _step0_download_pmc_ids(self) -> None:
        """Download PMC-ids.csv from NCBI FTP."""
        ncbi_dir = self._config.pipeline_dir / "data" / "NCBI"
        if ncbi_dir.exists():
            shutil.rmtree(ncbi_dir)
        ncbi_dir.mkdir(parents=True)

        subprocess.run(
            [
                "wget",
                "https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz",
                "-P",
                str(ncbi_dir),
            ],
            check=True,
            timeout=7200,
        )
        subprocess.run(
            ["gunzip", "-f", str(ncbi_dir / "PMC-ids.csv.gz")],
            check=True,
            timeout=600,
        )

    def _load_step(self, step: int) -> object:
        """Import a numbered step module via importlib."""
        return importlib.import_module(_STEP_MODULES[step])

    def _step1_update_bioc(self) -> None:
        """Update BioC-PMC corpus."""
        mod = self._load_step(0)
        mod.main(bioc_pmc_dir=self._config.bioc_pmc_dir)  # type: ignore[attr-defined]

    def _step2_search_pubmed(self) -> None:
        """Search PubMed/PMC for food-chemical sentences."""
        c = self._config
        run_dir = c.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "retrieved_sentences").mkdir(exist_ok=True)

        sys.argv = [
            "1_search_pubmed_pmc.py",
            "--query",
            c.food_terms,
            "--query_uid_results_filepath",
            str(run_dir / "query_uid_results.tsv"),
            "--filtered_sentences_filepath",
            str(run_dir / "retrieved_sentences" / "result_{i}.tsv"),
            "--filepath_BioC_PMC",
            c.bioc_pmc_dir,
        ]
        mod = self._load_step(1)
        mod.main()  # type: ignore[attr-defined]

    def _step3_biobert_filter(self) -> None:
        """Run BioBERT sentence filtering."""
        c = self._config
        run_dir = c.run_dir
        (run_dir / "sentence_filtering").mkdir(parents=True, exist_ok=True)

        sys.argv = [
            "2_run_sentence_filtering.py",
            "--input_file_path",
            str(run_dir / "retrieved_sentences" / "sentence_filtering_input.tsv"),
            "--save_file_path",
            str(run_dir / "sentence_filtering"),
            "--model_dir",
            c.biobert_model_dir,
            "--sentence_col",
            "sentence",
            "--chunk_size",
            "10000",
            "--batch_size",
            "64",
        ]
        mod = self._load_step(2)
        mod.main()  # type: ignore[attr-defined]

    def _step4_aggregate(self) -> None:
        """Aggregate and deduplicate filtered sentences."""
        c = self._config
        run_dir = c.run_dir
        (run_dir / "filtered_sentences").mkdir(parents=True, exist_ok=True)

        mod = self._load_step(3)
        mod.aggregate_food_chem_sentences(  # type: ignore[attr-defined]
            input_dir=str(run_dir / "sentence_filtering"),
            aggregated_path=str(
                run_dir / "filtered_sentences" / "filtered_sentence_aggregated.tsv"
            ),
            ie_input_path=str(
                run_dir / "filtered_sentences" / "information_extraction_input.tsv"
            ),
            reference_dir=str(c.preds_dir),
            threshold=c.threshold,
        )

    def _step5_extract(self) -> None:
        """Run LLM information extraction."""
        c = self._config
        run_dir = c.run_dir
        batch_dir = c.preds_dir / f"{c.date}_prediction_batch"

        sys.argv = [
            "4_run_information_extraction.py",
            "--input_path",
            str(run_dir / "filtered_sentences" / "information_extraction_input.tsv"),
            "--output_dir",
            str(batch_dir),
            "--model",
            c.model,
            "--date",
            c.date,
        ]
        mod = self._load_step(4)
        mod.main()  # type: ignore[attr-defined]

    def _step6_parse(self) -> None:
        """Parse LLM prediction outputs."""
        c = self._config
        batch_dir = c.preds_dir / f"{c.date}_prediction_batch"
        output_tsv = c.preds_dir / f"text_parser_predicted_{c.date}.tsv"

        mod = self._load_step(5)
        mod.aggregate_batch_predictions(  # type: ignore[attr-defined]
            batch_input_path=str(batch_dir / f"batch_input_{c.date}.tsv"),
            batch_results_dir=str(batch_dir),
            output_tsv=str(output_tsv),
        )
        mod.tsv_to_json(str(output_tsv))  # type: ignore[attr-defined]
