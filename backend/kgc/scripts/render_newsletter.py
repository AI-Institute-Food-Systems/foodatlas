#!/usr/bin/env python3
"""Render the weekly newsletter HTML from newsletter.json + the HTML template.

Reads the statistics payload emitted by the KGC NEWSLETTER stage and fills the
Jinja2 template at ``data/newsletter.html``, writing the digest to ``outputs/``.
Data (the JSON) and presentation (the template) stay separate, so the look can
change without re-running the pipeline.

Usage:
    python render_newsletter.py [--json PATH] [--template PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import jinja2

_KGC_DIR = Path(__file__).resolve().parents[1]
_DEFAULT_JSON = _KGC_DIR / "outputs" / "kg" / "newsletter.json"
_DEFAULT_TEMPLATE = _KGC_DIR / "data" / "newsletter.html"
_DEFAULT_OUTPUT = _KGC_DIR / "outputs" / "newsletter.html"


def main() -> int:
    args = parse_arguments()
    payload = json.loads(args.json.read_text(encoding="utf-8"))
    html = render(args.template, payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(f"Newsletter written to {args.output}")
    return 0


def render(template_path: Path, payload: dict) -> str:
    """Fill the Jinja2 template with the stats payload."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(template_path.parent)),
        autoescape=jinja2.select_autoescape(["html"]),
    )
    env.filters["commafy"] = lambda n: f"{int(n):,}"
    env.filters["signed"] = lambda n: f"{int(n):+,}"
    return env.get_template(template_path.name).render(**payload)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render newsletter.json into HTML")
    parser.add_argument("--json", type=Path, default=_DEFAULT_JSON)
    parser.add_argument("--template", type=Path, default=_DEFAULT_TEMPLATE)
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
