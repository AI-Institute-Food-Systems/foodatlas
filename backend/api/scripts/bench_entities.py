"""Latency benchmark and golden-response check for the entity endpoints.

Usage (from backend/api):
    uv run python -m scripts.bench_entities bench [--runs 5] [--out f.json]
    uv run python -m scripts.bench_entities golden record DIR
    uv run python -m scripts.bench_entities golden check DIR
    uv run python -m scripts.bench_entities pages DIR

Reads the Bearer key from FOODATLAS_API_KEY and the base URL from
FOODATLAS_API_URL (default http://127.0.0.1:8000). Golden files hold
canonical JSON (sorted keys), so a check fails on any change to the data
and passes on transport-only changes such as compression.
"""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import click
import httpx

_HEAVY_BIOACTIVITIES = ("antiviral", "anticancer")
_HEAVY_DISEASES = ("asthma", "obesity")
_HEAVY_CHEMICALS = ("quercetin", "kaempferol")
_HEAVY_FOODS = ("tomato", "mango")

# Paginated list endpoints whose full row set the `pages` command walks.
_PAGED: tuple[tuple[str, dict[str, str]], ...] = tuple(
    (f"/bioactivity/{tab}", {"common_name": name, "sort_dir": d})
    for tab in ("chemicals", "foods")
    for name in ("antiviral",)
    for d in ("desc", "asc")
)


def _urls() -> list[str]:
    """Fixed request set: what a cold render of each entity page fetches."""
    reqs: list[tuple[str, dict[str, str]]] = [("/metadata/statistics", {})]
    for name in _HEAVY_BIOACTIVITIES:
        reqs += [
            ("/bioactivity/metadata", {"common_name": name}),
            ("/bioactivity/diseases", {"common_name": name}),
        ]
        for tab in ("chemicals", "foods"):
            for page in ("1", "2"):
                reqs.append(
                    (f"/bioactivity/{tab}", {"common_name": name, "page": page})
                )
            reqs.append(
                (f"/bioactivity/{tab}", {"common_name": name, "sort_dir": "asc"})
            )
    for name in _HEAVY_DISEASES:
        reqs += [
            ("/disease/metadata", {"common_name": name}),
            ("/disease/taxonomy", {"common_name": name}),
            ("/disease/chemical-associations", {"common_name": name}),
            ("/disease/correlation", {"common_name": name}),
            ("/disease/correlation", {"common_name": name, "page": "2"}),
            ("/disease/correlation", {"common_name": name, "sort_by": "name"}),
        ]
    for name in _HEAVY_CHEMICALS:
        reqs += [
            ("/chemical/metadata", {"common_name": name}),
            ("/chemical/taxonomy", {"common_name": name}),
            ("/chemical/composition", {"common_name": name}),
            ("/chemical/bioactivities", {"common_name": name}),
            ("/chemical/correlation", {"common_name": name}),
            ("/chemical/disease-associations", {"common_name": name}),
        ]
    for name in _HEAVY_FOODS:
        reqs += [
            ("/food/metadata", {"common_name": name}),
            ("/food/taxonomy", {"common_name": name}),
            ("/food/composition", {"common_name": name}),
            ("/food/composition", {"common_name": name, "page": "2"}),
            ("/food/bioactivities", {"common_name": name}),
            ("/food/inferred-bioactivities", {"common_name": name}),
        ]
    return [_url(path, params) for path, params in reqs]


def _url(path: str, params: dict[str, str]) -> str:
    return f"{path}?{urlencode(params)}" if params else path


def _client() -> httpx.Client:
    key = os.environ.get("FOODATLAS_API_KEY", "")
    if not key:
        raise click.ClickException("Set FOODATLAS_API_KEY.")
    return httpx.Client(
        base_url=os.environ.get("FOODATLAS_API_URL", "http://127.0.0.1:8000"),
        headers={"Authorization": f"Bearer {key}", "Accept-Encoding": "gzip"},
        timeout=60.0,
    )


def _timed_get(client: httpx.Client, url: str) -> tuple[float, float, int, bytes]:
    """Return (ttfb_s, total_s, wire_bytes, body) for one GET."""
    start = time.perf_counter()
    with client.stream("GET", url) as resp:
        ttfb = time.perf_counter() - start
        body = resp.read()
        total = time.perf_counter() - start
        resp.raise_for_status()
        return ttfb, total, resp.num_bytes_downloaded, body


def _canonical(body: bytes) -> str:
    return json.dumps(json.loads(body), sort_keys=True, separators=(",", ":"))


def _slug(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


@click.group()
def cli() -> None:
    """Benchmark and golden-check the entity endpoints."""


@cli.command()
@click.option("--runs", default=5, show_default=True)
@click.option("--out", type=click.Path(path_type=Path), default=None)
def bench(runs: int, out: Path | None) -> None:
    """Time every URL `runs` times after one warm-up request."""
    results: list[dict[str, Any]] = []
    with _client() as client:
        for url in _urls():
            first = _timed_get(client, url)
            samples = [_timed_get(client, url) for _ in range(runs)]
            totals = sorted(s[1] for s in samples)
            row = {
                "url": url,
                "first_s": round(first[1], 3),
                "p50_s": round(statistics.median(totals), 3),
                "max_s": round(totals[-1], 3),
                "ttfb_p50_s": round(statistics.median(s[0] for s in samples), 3),
                "wire_kb": round(samples[0][2] / 1024, 1),
                "body_kb": round(len(samples[0][3]) / 1024, 1),
            }
            results.append(row)
            click.echo(
                f"{row['p50_s']:6.3f}s p50  {row['ttfb_p50_s']:6.3f}s ttfb  "
                f"{row['first_s']:6.3f}s first  {row['wire_kb']:8.1f}KB wire  "
                f"{url}"
            )
    if out:
        out.write_text(json.dumps(results, indent=2))


@cli.group()
def golden() -> None:
    """Record or check canonical response bodies."""


@golden.command("record")
@click.argument("directory", type=click.Path(path_type=Path))
def golden_record(directory: Path) -> None:
    """Write one canonical JSON file per URL plus an index."""
    directory.mkdir(parents=True, exist_ok=True)
    index: dict[str, str] = {}
    with _client() as client:
        for url in _urls():
            body = _canonical(_timed_get(client, url)[3])
            (directory / f"{_slug(url)}.json").write_text(body)
            index[url] = hashlib.sha256(body.encode()).hexdigest()
    (directory / "index.json").write_text(json.dumps(index, indent=2))
    click.echo(f"Recorded {len(index)} responses in {directory}")


@golden.command("check")
@click.argument("directory", type=click.Path(exists=True, path_type=Path))
def golden_check(directory: Path) -> None:
    """Fail when any response differs from the recording."""
    index: dict[str, str] = json.loads((directory / "index.json").read_text())
    failed: list[str] = []
    with _client() as client:
        for url, digest in index.items():
            body = _canonical(_timed_get(client, url)[3])
            if hashlib.sha256(body.encode()).hexdigest() != digest:
                failed.append(url)
                click.echo(f"DIFF  {url}")
    click.echo(f"{len(index) - len(failed)}/{len(index)} identical")
    if failed:
        sys.exit(1)


def _all_rows(client: httpx.Client, path: str, params: dict[str, str]) -> list[str]:
    """Walk every page and return each row as canonical JSON."""
    rows: list[str] = []
    page = 1
    while True:
        url = _url(path, {**params, "page": str(page)})
        payload = json.loads(_timed_get(client, url)[3])
        rows += [json.dumps(r, sort_keys=True) for r in payload["data"]]
        if page >= int(payload["metadata"]["total_pages"]):
            return rows
        page += 1


@cli.command()
@click.argument("directory", type=click.Path(path_type=Path))
@click.option("--record", is_flag=True, help="Save the row sets as the baseline.")
def pages(directory: Path, record: bool) -> None:
    """Check paginated row sets: no duplicates, same multiset as baseline.

    Use this where only the order of tied rows may change.
    """
    directory.mkdir(parents=True, exist_ok=True)
    failed = False
    with _client() as client:
        for path, params in _PAGED:
            rows = _all_rows(client, path, params)
            name = _url(path, params)
            target = directory / f"pages-{_slug(name)}.json"
            dupes = len(rows) - len(set(rows))
            if record:
                target.write_text(json.dumps(sorted(rows)))
                click.echo(f"recorded {len(rows)} rows ({dupes} dupes)  {name}")
                continue
            same = sorted(rows) == json.loads(target.read_text())
            ok = same and dupes == 0
            failed |= not ok
            click.echo(
                f"{'OK  ' if ok else 'FAIL'}  {len(rows)} rows, {dupes} dupes, "
                f"multiset {'same' if same else 'DIFFERENT'}  {name}"
            )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    cli()
