"""Consistent JSON read/write helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json(path: Path | str) -> Any:
    """Read a JSON file and return its contents."""
    with Path(path).open() as f:
        return json.load(f)


def write_json(path: Path | str, data: Any) -> None:
    """Write data to a JSON file with indent=2 and ensure_ascii=False."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
