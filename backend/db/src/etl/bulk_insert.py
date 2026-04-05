"""Bulk insert DataFrames into PostgreSQL using COPY."""

import io
import json
import logging

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def _serialize_value(val: object) -> str:
    """Serialize a Python value for PostgreSQL COPY format."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return r"\N"
    if isinstance(val, bool | np.bool_):
        return "t" if val else "f"
    if isinstance(val, (list, dict)):
        return json.dumps(val)
    return str(val)


def _df_to_copy_buffer(df: pd.DataFrame, columns: list[str]) -> io.StringIO:
    """Convert a DataFrame subset to a tab-separated StringIO buffer."""
    buf = io.StringIO()
    for _, row in df[columns].iterrows():
        line = "\t".join(_serialize_value(row[col]) for col in columns)
        buf.write(line + "\n")
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
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        buf = _df_to_copy_buffer(chunk, columns)
        col_list = ", ".join(f'"{c}"' for c in columns)
        copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN"
        with raw.cursor() as cur, cur.copy(copy_sql) as copy:
            for line in buf:
                copy.write(line)
        total += len(chunk)
    logger.info("Inserted %d rows into %s", total, table_name)
    return total


def truncate_tables(conn: Connection, table_names: list[str]) -> None:
    """Truncate tables in order (CASCADE to handle FKs)."""
    for name in table_names:
        conn.execute(text(f"TRUNCATE TABLE {name} CASCADE"))
    logger.info("Truncated tables: %s", ", ".join(table_names))
