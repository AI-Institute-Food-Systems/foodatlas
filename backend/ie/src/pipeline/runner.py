"""IE pipeline runner — executes stages sequentially."""

from __future__ import annotations

import logging
import re
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

    _DATE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")
    _STAGING_RE = re.compile(r"^_\d{4}_\d{2}_\d{2}$")

    def __init__(self, settings: IESettings) -> None:
        self._s = settings
        self._pipeline_dir = Path.cwd()

    @property
    def _corpus_dir(self) -> Path:
        return self._pipeline_dir / "outputs" / "corpus"

    @property
    def _search_base(self) -> Path:
        return self._pipeline_dir / "outputs" / "search"

    @property
    def _filtering_base(self) -> Path:
        return self._pipeline_dir / "outputs" / "filtering"

    @property
    def _extraction_dir(self) -> Path:
        return self._pipeline_dir / "outputs" / "extraction"

    def _run_date(self) -> str:
        """Determine the run date from an explicit setting or existing state.

        Priority:
        1. Explicit ``date`` in settings
        2. Existing search staging dir (``_YYYY_MM_DD``)
        3. Latest completed search dir (``YYYY_MM_DD``)
        4. Today's date (UTC)
        """
        if self._s.date:
            return self._s.date

        # Check for staging dir
        staging = self._find_staging()
        if staging:
            return staging.name.lstrip("_")

        # Check for latest completed search dir
        latest = self._latest_completed_search()
        if latest:
            return latest

        return self._s.resolved_date

    def _find_staging(self) -> Path | None:
        """Find an existing search staging dir, if any."""
        base = self._search_base
        if not base.is_dir():
            return None
        existing = sorted(
            d for d in base.iterdir() if d.is_dir() and self._STAGING_RE.match(d.name)
        )
        if len(existing) > 1:
            names = [d.name for d in existing]
            msg = f"Multiple staging dirs found: {names} — resolve manually"
            raise RuntimeError(msg)
        return existing[0] if existing else None

    def _latest_completed_search(self) -> str | None:
        """Find the most recent completed YYYY_MM_DD search folder."""
        base = self._search_base
        if not base.is_dir():
            return None
        dates = sorted(
            d.name
            for d in base.iterdir()
            if d.is_dir()
            and self._DATE_RE.match(d.name)
            and (d / "retrieved_sentences" / "sentence_filtering_input.tsv").exists()
        )
        return dates[-1] if dates else None

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
        """Search PubMed/PMC for food-chemical sentences.

        Work is done inside a staging directory (``_YYYY_MM_DD``).
        On success the staging dir is renamed to the final name
        (``YYYY_MM_DD``), making it visible to downstream stages.
        If the run is aborted, the staging dir remains and will be
        resumed on the next invocation.
        """
        s = self._s
        run_date = self._run_date()
        staging = self._find_staging() or (self._search_base / f"_{run_date}")
        final = self._search_base / run_date

        if final.exists():
            msg = f"Finalized run dir already exists: {final}"
            raise FileExistsError(msg)

        staging.mkdir(parents=True, exist_ok=True)
        (staging / "retrieved_sentences").mkdir(exist_ok=True)

        run_search(
            query=s.food_terms,
            query_uid_results_filepath=str(staging / "query_uid_results.tsv"),
            filtered_sentences_filepath=str(
                staging / "retrieved_sentences" / "result_{i}.tsv"
            ),
            filepath_bioc_pmc=s.bioc_pmc_dir,
            filepath_food_names=s.translated_food_terms,
            output_base_dir=str(self._search_base),
            current_date=run_date,
        )

        staging.rename(final)
        log.info("Promoted staging dir %s -> %s", staging, final)

    def _run_filtering(self) -> None:
        """Run BioBERT sentence filtering and aggregate results."""
        s = self._s
        bf = s.pipeline.biobert_filter
        run_date = self._run_date()
        search_dir = self._search_base / run_date
        filter_dir = self._filtering_base / run_date

        if not search_dir.exists():
            msg = f"Search results not found: {search_dir}"
            raise FileNotFoundError(msg)

        (filter_dir / "sentence_filtering").mkdir(parents=True, exist_ok=True)
        (filter_dir / "filtered_sentences").mkdir(parents=True, exist_ok=True)

        run_biobert_filter(
            input_file_path=str(
                search_dir / "retrieved_sentences" / "sentence_filtering_input.tsv"
            ),
            save_file_path=str(filter_dir / "sentence_filtering"),
            model_dir=s.biobert_model_dir,
            sentence_col=bf.sentence_col,
            chunk_size=bf.chunk_size,
            batch_size=bf.batch_size,
        )

        aggregate_food_chem_sentences(
            input_dir=str(filter_dir / "sentence_filtering"),
            aggregated_path=str(
                filter_dir / "filtered_sentences" / "filtered_sentence_aggregated.tsv"
            ),
            ie_input_path=str(
                filter_dir / "filtered_sentences" / "information_extraction_input.tsv"
            ),
            reference_dir=str(self._extraction_dir),
            threshold=s.threshold,
        )

    def _run_extraction(self) -> None:
        """Run LLM information extraction and parse outputs."""
        s = self._s
        ex = s.pipeline.extraction
        run_date = self._run_date()
        filter_dir = self._filtering_base / run_date

        if not filter_dir.exists():
            msg = f"Filtering results not found: {filter_dir}"
            raise FileNotFoundError(msg)

        batch_dir = self._extraction_dir / f"{run_date}_prediction_batch"

        run_extraction(
            input_path=str(
                filter_dir / "filtered_sentences" / "information_extraction_input.tsv"
            ),
            output_dir=str(batch_dir),
            model=s.model,
            date=run_date,
            prompt_version=ex.prompt_version,
            system_prompt=ex.system_prompt,
            temperature=ex.temperature,
            max_new_tokens=ex.max_new_tokens,
        )

        output_tsv = self._extraction_dir / f"extraction_predicted_{run_date}.tsv"
        aggregate_batch_predictions(
            batch_input_path=str(batch_dir / f"batch_input_{run_date}.tsv"),
            results_dir=str(batch_dir),
            output_path=str(output_tsv),
        )
        tsv_to_json(str(output_tsv))
