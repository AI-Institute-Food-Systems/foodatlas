#!/bin/bash
# Run a one-shot db migration command via the Jobs Fargate task
# definition. Same shape as run-data-load.sh — just a different db CLI
# subcommand and no default stack.
#
# Usage:
#   STACK=FoodAtlasJobsStack ./run-migration.sh migrate-bioact-perf
#   STACK=FoodAtlasJobsStack ./run-migration.sh --dry-run migrate-bioact-perf
#
# STACK is required so a migration on prod is always a deliberate choice.

set -euo pipefail

cd "$(dirname "$0")"

STACK="${STACK:?set STACK explicitly, e.g. STACK=FoodAtlasJobsStack}"
export STACK

# shellcheck source=_lib.sh
source ./_lib.sh

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN=1; shift; fi

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--dry-run] <db-cli-subcommand> [args...]" >&2
    echo "  e.g.  $0 migrate-bioact-perf" >&2
    exit 1
fi

# Build the JSON command array (python main.py <args...>).
cmd_json='["python","main.py"'
for arg in "$@"; do
    # Escape backslashes + double-quotes for JSON safety.
    escaped=${arg//\\/\\\\}
    escaped=${escaped//\"/\\\"}
    cmd_json+=",\"$escaped\""
done
cmd_json+=']'

description="$* (stack=$STACK)"

if [[ -n "$DRY_RUN" ]]; then
    load_jobs_stack_outputs
    echo "[dry-run] stack:       $STACK"
    echo "[dry-run] cluster:     $CLUSTER"
    echo "[dry-run] task def:    $TASK_DEF"
    echo "[dry-run] subnets:     $SUBNET_IDS"
    echo "[dry-run] security:    $SG_ID"
    echo "[dry-run] container:   $CONTAINER_NAME"
    echo "[dry-run] command:     $cmd_json"
    echo "[dry-run] description: $description"
    exit 0
fi

run_jobs_task "$cmd_json" "$description"
