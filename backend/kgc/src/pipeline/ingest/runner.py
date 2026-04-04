"""IngestRunner — orchestrates Phase 1 source adapters in parallel."""

from __future__ import annotations

import logging
import multiprocessing
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tqdm import tqdm

from .adapters.cdno import CDNOAdapter
from .adapters.chebi import ChEBIAdapter
from .adapters.ctd import CTDAdapter
from .adapters.dmd import DMDAdapter
from .adapters.fdc import FDCAdapter
from .adapters.flavordb import FlavorDBAdapter
from .adapters.foodon import FoodOnAdapter
from .adapters.mesh import MeSHAdapter
from .adapters.pubchem import PubChemAdapter

if TYPE_CHECKING:
    from ...models.ingest import SourceManifest
    from ...models.settings import KGCSettings
    from .protocol import ProgressCallback, SourceAdapter

logger = logging.getLogger(__name__)

ALL_ADAPTERS: list[type] = [
    FoodOnAdapter,
    ChEBIAdapter,
    CDNOAdapter,
    CTDAdapter,
    MeSHAdapter,
    PubChemAdapter,
    FlavorDBAdapter,
    FDCAdapter,
    DMDAdapter,
]


def _make_queue_callback(
    queue: Any,
    source_id: str,
    throttle: int = 500,
) -> ProgressCallback:
    """Return a throttled callback that pushes ``(source_id, current, total)``.

    Only sends a message every *throttle* calls (or when current == total).
    """
    call_count = [0]

    def _cb(current: int, total: int) -> None:
        call_count[0] += 1
        if current == total or call_count[0] % throttle == 0:
            queue.put((source_id, current, total))

    return _cb


def _run_single_adapter(
    adapter_cls: type,
    raw_dir: str,
    output_dir: str,
    queue: Any,
) -> SourceManifest | str:
    """Run one adapter in a child process. Returns manifest or error string."""
    adapter: SourceAdapter = adapter_cls()
    try:
        out = Path(output_dir) / adapter.source_id
        cb = _make_queue_callback(queue, adapter.source_id)
        manifest = adapter.ingest(Path(raw_dir), out, progress=cb)
        queue.put((adapter.source_id, -1, -1))  # sentinel: done
        return manifest
    except Exception:
        tb = traceback.format_exc()
        queue.put((adapter.source_id, -2, -2))  # sentinel: error
        return f"{adapter.source_id} failed:\n{tb}"


class IngestRunner:
    """Run all source adapters, producing standardized parquet."""

    def __init__(self, settings: KGCSettings) -> None:
        self._settings = settings

    def run(
        self,
        sources: list[str] | None = None,
    ) -> dict[str, SourceManifest]:
        """Run adapters in parallel with per-source progress bars."""
        raw_dir = self._settings.data_dir
        output_dir = self._settings.ingest_dir

        adapters_to_run = ALL_ADAPTERS
        if sources:
            adapters_to_run = [
                cls for cls in ALL_ADAPTERS if cls().source_id in sources
            ]

        logger.info("Launching %d ingest adapters.", len(adapters_to_run))

        results = _run_with_progress(adapters_to_run, raw_dir, output_dir)

        logger.info("All ingest adapters complete.")
        return results


def _create_bars(source_ids: list[str]) -> dict[str, tqdm[Any]]:
    """Create one tqdm progress bar per source (total set dynamically)."""
    bars: dict[str, tqdm[Any]] = {}
    for i, sid in enumerate(source_ids):
        bars[sid] = tqdm(
            total=0,
            desc=f"{sid:<10}",
            position=i,
            leave=False,
            unit=" rows",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}{unit} [{elapsed}]",
        )
    return bars


def _drain_queue(
    queue: Any,
    bars: dict[str, tqdm[Any]],
    n_adapters: int,
) -> None:
    """Read progress messages until all adapters finish or error."""
    done_count = 0
    finished: list[str] = []
    while done_count < n_adapters:
        source_id, current, total = queue.get()
        bar = bars[source_id]
        if current == -1:
            if bar.total and bar.n < bar.total:
                bar.update(bar.total - bar.n)
            finished.append(source_id)
            done_count += 1
        elif current == -2:
            bar.set_postfix_str("ERROR")
            finished.append(source_id)
            done_count += 1
        else:
            if total > 0 and bar.total != total:
                bar.total = total
                bar.refresh()
            bar.n = current
            bar.refresh()

    for bar in bars.values():
        bar.close()
    for sid in finished:
        bar = bars[sid]
        total = bar.total or 0
        tqdm.write(f"{sid:<10}: {total} rows")


def _run_with_progress(
    adapters: list[type],
    raw_dir: str,
    output_dir: str,
) -> dict[str, SourceManifest]:
    """Spawn adapter processes and drive tqdm bars from the queue."""
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    root_logger = logging.getLogger()
    prev_level = root_logger.level
    root_logger.setLevel(logging.WARNING)

    source_ids = [cls().source_id for cls in adapters]
    bars = _create_bars(source_ids)
    pool = multiprocessing.Pool(processes=len(adapters))

    async_results = {
        cls().source_id: pool.apply_async(
            _run_single_adapter, (cls, raw_dir, output_dir, queue)
        )
        for cls in adapters
    }

    _drain_queue(queue, bars, len(adapters))
    pool.close()
    pool.join()

    results: dict[str, SourceManifest] = {}
    for sid, ar in async_results.items():
        result = ar.get()
        if isinstance(result, str):
            logger.error(result)
        else:
            results[sid] = result

    root_logger.setLevel(prev_level)
    return results
