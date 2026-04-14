"""Bulk insert DataFrames into PostgreSQL using COPY."""

import io
import json
import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _pg_array_literal(vals: list) -> str:
    """Convert a Python list to a PostgreSQL array literal for COPY text format.

    Two escaping levels are applied:
    1. Array element: ``\\`` and ``\\"`` inside quoted elements.
    2. COPY text: backslashes doubled again so the COPY parser restores level-1.
    """
    parts: list[str] = []
    for v in vals:
        if v is None:
            parts.append("NULL")
            continue
        s = str(v)
        # Level 1 -array quoted-element escaping
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        parts.append('"' + s + '"')
    literal = "{" + ",".join(parts) + "}"
    # Level 2 -COPY text escaping (backslash is special in COPY text format)
    literal = (
        literal.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return literal


def _copy_text_escape(s: str) -> str:
    """Escape a string for PostgreSQL COPY text format."""
    return (
        s.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _serialize_value(val: object) -> str:
    """Serialize a Python value for PostgreSQL COPY format."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return r"\N"
    if isinstance(val, bool | np.bool_):
        return "t" if val else "f"
    if isinstance(val, list):
        return _pg_array_literal(val)
    if isinstance(val, dict):
        return _copy_text_escape(json.dumps(val))
    return _copy_text_escape(str(val))


def _serialize_column(series: pd.Series) -> pd.Series:
    """Vectorize serialization for common column types."""
    if series.empty:
        return series.astype(str)

    sample = series.dropna().iloc[0] if not series.dropna().empty else None

    # Fast path: plain strings (most text columns)
    if isinstance(sample, str):
        result = series.fillna(r"\N")
        mask = result != r"\N"
        result[mask] = result[mask].apply(_copy_text_escape)
        return result

    # Fast path: numeric (int/float)
    if isinstance(sample, int | float | np.integer | np.floating):
        return series.apply(
            lambda v: (
                r"\N" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
            )
        )

    # Fast path: booleans
    if isinstance(sample, bool | np.bool_):
        return series.apply(lambda v: r"\N" if v is None else ("t" if v else "f"))

    # Fallback: lists, dicts, mixed types
    return series.apply(_serialize_value)


def _df_to_copy_buffer(df: pd.DataFrame, columns: list[str]) -> io.StringIO:
    """Convert a DataFrame subset to a tab-separated StringIO buffer."""
    serialized = {col: _serialize_column(df[col]) for col in columns}
    frame = pd.DataFrame(serialized)
    buf = io.StringIO()
    frame.to_csv(buf, sep="\t", header=False, index=False, quoting=3)
    buf.seek(0)
    return buf


def bulk_copy(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
    chunk_size: int = 50_000,
) -> int:
    """COPY a DataFrame into a table in chunks.

    Uses psycopg3's COPY FROM STDIN for fast bulk loading.
    Returns the total number of rows inserted.
    """
    raw = conn.connection  # unwrap to psycopg connection
    total = 0
    col_list = ", ".join(f'"{c}"' for c in columns)
    copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN"
    with tqdm(total=len(df), desc=table_name, unit="rows", leave=True) as pbar:
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            buf = _df_to_copy_buffer(chunk, columns)
            with raw.cursor() as cur, cur.copy(copy_sql) as copy:
                for line in buf:
                    copy.write(line)
            total += len(chunk)
            pbar.update(len(chunk))
    logger.info("Inserted %d rows into %s", total, table_name)
    return total


def truncate_tables(conn: Connection, table_names: list[str]) -> None:
    """Truncate tables in order (CASCADE to handle FKs)."""
    for name in table_names:
        conn.execute(text(f"TRUNCATE TABLE {name} CASCADE"))
    logger.info("Truncated tables: %s", ", ".join(table_names))
