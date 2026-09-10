"""A real Postgres, for testing query predicates by executing them.

Every other test in this suite hands the repositories an ``AsyncMock``
session. That is fine for route wiring and for pure Python, and useless
for the thing that actually broke: a WHERE clause that is syntactically
valid, executes without error, and selects the wrong rows.

`test_composition_sources.py` closed that hole for the composition source
filter using an in-memory SQLite table. SQLite gets you as far as
``IS NOT NULL`` and boolean precedence. It cannot run
``EXISTS (SELECT 1 FROM jsonb_array_elements(measurements) m WHERE
m->>'evidence_type' = ANY(:ets))``, and it has no ``text[]`` so it cannot
run the ``&&`` array-overlap the category filter uses — which is most of
the bioactivity filter surface. Those need the real engine.

So: one throwaway Postgres container per test session, the handful of
materialized views the filter code reads, and a fixture small enough to
enumerate by hand. Tests then call the *production* repository functions
and compare their output to a Python oracle over the same fixture.

Skipping: if Docker is not available the DB tests skip, so a laptop
without Docker running can still run the rest of the suite. CI sets
``REQUIRE_DB_TESTS=1``, which turns that skip into a hard failure —
otherwise the one suite that can catch this bug class would quietly stop
running and nobody would notice until the next incident.
"""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

# Columns the bioactivity filter code actually reads. Deliberately not the
# full production MV — a fixture you cannot hold in your head stops being
# an oracle and becomes a second implementation.
_CHEM_BIOACT_DDL = """
CREATE TABLE mv_chemical_bioactivity (
    id                    INTEGER,
    bioactivity_name      TEXT,
    bioactivity_foodatlas_id TEXT,
    chemical_name         TEXT,
    chemical_foodatlas_id TEXT,
    measurement_count     INTEGER,
    active_count          INTEGER DEFAULT 0,
    inactive_count        INTEGER DEFAULT 0,
    unspecified_count     INTEGER DEFAULT 0,
    inconclusive_count    INTEGER DEFAULT 0,
    n_foods               INTEGER DEFAULT 0,
    measurements          JSONB
)
"""

_CHEM_ENTITIES_DDL = """
CREATE TABLE mv_chemical_entities (
    foodatlas_id            TEXT,
    common_name             TEXT,
    chemical_classification TEXT[]
)
"""

_FOOD_ENTITIES_DDL = """
CREATE TABLE mv_food_entities (
    foodatlas_id        TEXT,
    entity_type         TEXT,
    common_name         TEXT,
    scientific_name     TEXT,
    synonyms            TEXT[],
    external_ids        JSONB,
    food_classification TEXT[],
    ambiguity_siblings  JSONB
)
"""

_FOOD_COMPOSITION_DDL = """
CREATE TABLE mv_food_chemical_composition (
    id                      INTEGER,
    food_name               TEXT,
    food_foodatlas_id       TEXT,
    chemical_name           TEXT,
    chemical_foodatlas_id   TEXT,
    chemical_classification TEXT[],
    median_concentration    JSONB,
    fdc_evidences           JSONB,
    foodatlas_evidences     JSONB,
    ptfi_evidences          JSONB,
    dmd_evidences           JSONB
)
"""

# The composition path resolves per-attestation trust scores before it
# can decide which rows survive, so the table has to exist even when a
# test has no low-trust data — an empty table means "nothing is
# low-trust", which is the default state we want most fixtures in.
_TRUST_SIGNALS_DDL = """
CREATE TABLE base_trust_signals (
    attestation_id TEXT,
    signal_kind    TEXT,
    score          NUMERIC,
    created_at     TIMESTAMP DEFAULT NOW()
)
"""

DDL: tuple[str, ...] = (
    _CHEM_BIOACT_DDL,
    _CHEM_ENTITIES_DDL,
    _FOOD_ENTITIES_DDL,
    _FOOD_COMPOSITION_DDL,
    _TRUST_SIGNALS_DDL,
)

# Which of the above mirror a table owned by backend/db. `base_trust_signals`
# lives on a separate TrustBase and is excluded — see test_schema_drift.
MIRRORED_TABLES: tuple[str, ...] = (
    "mv_chemical_bioactivity",
    "mv_chemical_entities",
    "mv_food_entities",
    "mv_food_chemical_composition",
)


def docker_unavailable() -> str:
    """Reason to skip, or "" when the DB tests can run.

    Shells out rather than using the docker SDK. `docker.from_env()`
    opens its HTTP socket before it can fail, and leaves it unclosed on
    failure — this suite runs with `filterwarnings = ["error"]`, so that
    leak surfaced as a ResourceWarning-turned-ERROR at the teardown of an
    unrelated test. A subprocess has no in-process socket to leak.

    Returns a reason rather than raising so the caller can decide between
    skipping (developer laptop) and failing (CI).
    """
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            timeout=20,
            check=False,
        )
    except FileNotFoundError:
        return "docker CLI not installed"
    except subprocess.TimeoutExpired:
        return "docker daemon did not respond within 20s"
    if proc.returncode != 0:
        return "docker daemon unreachable"
    return ""


def require_db_tests() -> bool:
    """True when a missing Docker must fail rather than skip.

    Set in CI. Without it, a broken Docker layer in the pipeline would
    silently reduce this suite to zero tests and still report green.
    """
    return os.environ.get("REQUIRE_DB_TESTS", "").lower() in {"1", "true", "yes"}


def measurement(
    *,
    evidence_type: str = "in vitro",
    evidence_source: str = "experimental",
    endpoint: str = "IC50",
    unit: str = "uM",
    value: float = 1.0,
) -> dict:
    """One entry of the `measurements` JSONB array.

    Only the keys the filters read. `evidence_source` is matched by the
    SQL with ``LIKE 'exp%'`` / ``'pred%'`` / ``'comp%'`` and by Python
    with ``str.startswith``, so values here are chosen to exercise both
    the matching and the non-matching side of that.
    """
    return {
        "evidence_type": evidence_type,
        "evidence_source": evidence_source,
        "endpoint": endpoint,
        "unit": unit,
        "value": value,
    }


def chem_bioactivity_row(
    *,
    bioactivity: str,
    chemical: str,
    chem_id: str,
    measurements: Sequence[dict],
) -> dict:
    return {
        "bioactivity_name": bioactivity,
        "bioactivity_foodatlas_id": "b1",
        "chemical_name": chemical,
        "chemical_foodatlas_id": chem_id,
        "measurement_count": len(measurements),
        "active_count": 0,
        "inactive_count": 0,
        "unspecified_count": 0,
        "inconclusive_count": 0,
        "n_foods": 0,
        "measurements": list(measurements),
    }
