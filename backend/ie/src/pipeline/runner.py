"""IE pipeline runner — executes stages sequentially."""

from __future__ import annotations

import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from .corpus.runner import main as update_bioc
from .extraction.parse_predictions import (
    aggregate_batch_predictions,
    tsv_to_json,
)
from .extraction.runner import run_extraction
from .filtering.aggregate import aggregate_food_chem_sentences
from .filtering.runner import run_biobert_filter
from .search.runner import run_search
from .stages import ALL_STAGES, IEStage

if TYPE_CHECKING:
    from ..models.settings import IESettings

log = logging.getLogger(__name__)


class IERunner:
    """Execute IE pipeline stages in order."""

    def __init__(self, settings: IESettings) -> None:
        self._s = settings
        self._pipeline_dir = Path.cwd()

    @property
    def _corpus_dir(self) -> Path:
        return self._pipeline_dir / "outputs" / "corpus"

    @property
    def _run_dir(self) -> Path:
        return self._pipeline_dir / "outputs" / "search" / self._s.resolved_date

    @property
    def _extraction_dir(self) -> Path:
        return self._pipeline_dir / "outputs" / "extraction"

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
            IEStage.CORPUS: self._run_corpus,
            IEStage.SEARCH: self._run_search,
            IEStage.FILTERING: self._run_filtering,
            IEStage.EXTRACTION: self._run_extraction,
        }
        dispatch[stage]()

    def _run_corpus(self) -> None:
        """Download PMC-ids.csv and update BioC-PMC corpus."""
        ncbi_dir = self._corpus_dir / "NCBI"
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

        update_bioc(bioc_pmc_dir=self._s.bioc_pmc_dir)

    def _run_search(self) -> None:
        """Search PubMed/PMC for food-chemical sentences."""
        s = self._s
        run_dir = self._run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "retrieved_sentences").mkdir(exist_ok=True)

        run_search(
            query=s.food_terms,
            query_uid_results_filepath=str(run_dir / "query_uid_results.tsv"),
            filtered_sentences_filepath=str(
                run_dir / "retrieved_sentences" / "result_{i}.tsv"
            ),
            filepath_bioc_pmc=s.bioc_pmc_dir,
            filepath_food_names=s.translated_food_terms,
            output_base_dir=str(self._pipeline_dir / "outputs" / "search"),
            current_date=s.resolved_date,
        )

    def _run_filtering(self) -> None:
        """Run BioBERT sentence filtering and aggregate results."""
        s = self._s
        bf = s.pipeline.biobert_filter
        run_dir = self._run_dir
        (run_dir / "sentence_filtering").mkdir(parents=True, exist_ok=True)
        (run_dir / "filtered_sentences").mkdir(parents=True, exist_ok=True)

        run_biobert_filter(
            input_file_path=str(
                run_dir / "retrieved_sentences" / "sentence_filtering_input.tsv"
            ),
            save_file_path=str(run_dir / "sentence_filtering"),
            model_dir=s.biobert_model_dir,
            sentence_col=bf.sentence_col,
            chunk_size=bf.chunk_size,
            batch_size=bf.batch_size,
        )

        aggregate_food_chem_sentences(
            input_dir=str(run_dir / "sentence_filtering"),
            aggregated_path=str(
                run_dir / "filtered_sentences" / "filtered_sentence_aggregated.tsv"
            ),
            ie_input_path=str(
                run_dir / "filtered_sentences" / "information_extraction_input.tsv"
            ),
            reference_dir=str(self._extraction_dir),
            threshold=s.threshold,
        )

    def _run_extraction(self) -> None:
        """Run LLM information extraction and parse outputs."""
        s = self._s
        ex = s.pipeline.extraction
        date = s.resolved_date
        batch_dir = self._extraction_dir / f"{date}_prediction_batch"

        run_extraction(
            input_path=str(
                self._run_dir
                / "filtered_sentences"
                / "information_extraction_input.tsv"
            ),
            output_dir=str(batch_dir),
            model=s.model,
            date=date,
            prompt_version=ex.prompt_version,
            system_prompt=ex.system_prompt,
            temperature=ex.temperature,
            max_new_tokens=ex.max_new_tokens,
        )

        output_tsv = self._extraction_dir / f"extraction_predicted_{date}.tsv"
        aggregate_batch_predictions(
            batch_input_path=str(batch_dir / f"batch_input_{date}.tsv"),
            results_dir=str(batch_dir),
            output_path=str(output_tsv),
        )
        tsv_to_json(str(output_tsv))
