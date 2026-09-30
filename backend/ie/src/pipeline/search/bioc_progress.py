"""Progress reporting for the on-demand BioC fetcher: tqdm bar + a run log file.

The fetch runs for hours under nohup, so a terminal progress bar alone is not
enough — ``start_logger`` opens a per-run log the operator can tail, and
``consume_futures`` appends a rate line to it every ``log_interval_seconds``.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import as_completed
from typing import TYPE_CHECKING

from tqdm import tqdm

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path

    from .bioc_fetch import FetchResult

_POSTFIX_EVERY = 200  # articles between tqdm postfix refreshes


def consume_futures(
    futures: dict[Future[tuple[str, str]], str],
    result: FetchResult,
    total: int,
    progress_logger: logging.Logger | None,
    log_interval_seconds: float,
    t_start: float,
) -> None:
    """Tally each fetch as it lands, driving the progress bar and the log file."""
    last_log_t = t_start
    pbar = tqdm(total=total, unit="article", smoothing=0.05)
    try:
        for fut in as_completed(futures):
            pmcid, status = fut.result()
            result.record(pmcid, status)
            pbar.update(1)

            now = time.monotonic()
            if progress_logger and (now - last_log_t) >= log_interval_seconds:
                emit(progress_logger, result, total, now - t_start)
                last_log_t = now

            if (result.fetched + result.cached) % _POSTFIX_EVERY == 0:
                pbar.set_postfix_str(
                    f"ok={result.fetched} oa_miss={len(result.not_in_oa)}"
                    f" err={len(result.errors)}"
                )
    finally:
        pbar.close()


def start_logger(
    log_path: Path | None,
    total: int,
    max_workers: int,
    out_dir: Path,
) -> tuple[logging.Logger | None, logging.FileHandler | None]:
    """Open the run log and write its start line; (None, None) when unlogged."""
    if log_path is None:
        return None, None
    logger, handler = _make_logger(log_path)
    logger.info("start total=%d workers=%d out_dir=%s", total, max_workers, out_dir)
    return logger, handler


def close_logger(
    progress_logger: logging.Logger | None,
    progress_handler: logging.FileHandler | None,
    result: FetchResult,
    total: int,
    t_start: float,
) -> None:
    """Write the done line and release the file handler. Safe to call unlogged."""
    if progress_logger is None or progress_handler is None:
        return
    emit(progress_logger, result, total, time.monotonic() - t_start, tag="done")
    progress_handler.close()
    progress_logger.removeHandler(progress_handler)


def _make_logger(log_path: Path) -> tuple[logging.Logger, logging.FileHandler]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    # A bare Logger (not getLogger) keeps these lines out of the root handlers,
    # so the run log stays separate from the pipeline's stderr logging.
    logger = logging.Logger(f"pmc_fetch.progress[{log_path.name}]")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger, handler


def emit(
    logger: logging.Logger,
    result: FetchResult,
    total: int,
    elapsed: float,
    tag: str = "progress",
) -> None:
    done = result.fetched + result.cached + len(result.not_in_oa) + len(result.errors)
    rate = done / elapsed if elapsed > 0 else 0.0
    logger.info(
        "%s %d/%d rate=%.1f/s ok=%d cached=%d oa_miss=%d err=%d",
        tag,
        done,
        total,
        rate,
        result.fetched,
        result.cached,
        len(result.not_in_oa),
        len(result.errors),
    )
