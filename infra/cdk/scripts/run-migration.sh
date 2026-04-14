#!/bin/bash
# Run Alembic migrations against RDS via a one-off Fargate task.
#
# Usage: ./run-migration.sh [revision]
#   revision: target Alembic revision, defaults to "head"

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=_lib.sh
source ./_lib.sh

REVISION="${1:-head}"
COMMAND_JSON="[\"alembic\",\"upgrade\",\"$REVISION\"]"

run_jobs_task "$COMMAND_JSON" "alembic upgrade $REVISION"
