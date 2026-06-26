#!/usr/bin/env bash
# Query the FoodAtlas *bioactivity* API (staging preview) and save example
# responses you can inspect, diff, and hand to your UI as fixtures.
#
# QUICK START
#   ./query-bioactivity-api.sh                 # runs the default example queries
#   ./query-bioactivity-api.sh --chemical resveratrol --food garlic --bioactivity anticancer
#
# Change the query in EITHER way:
#   1. edit the three variables just below, or
#   2. pass --bioactivity / --chemical / --food on the command line.
#
# Point at a different backend (e.g. production, later) without editing:
#   API_URL=https://... API_KEY=... ./query-bioactivity-api.sh
#
# Each endpoint's JSON response is pretty-printed to:
#   ./bioactivity-api-examples/<name>.json
# and a one-line summary (HTTP status + row count) is printed to the terminal.

set -euo pipefail

# ---- target backend — NO secrets baked in; provide URL + key one of three ways:
#   • export API_URL=... API_KEY=...     (see the guide → Getting access & credentials)
#   • create a gitignored staging.env next to this script: API_URL=... / API_KEY=...
#   • pass --url / --key flags
API_URL="${API_URL:-}"
API_KEY="${API_KEY:-}"

# ---- queries — CHANGE THESE (or pass --flags) ------------------------------
BIOACTIVITY="antioxidant"   # used by /bioactivity/{metadata,foods,chemicals}
CHEMICAL="quercetin"        # used by /chemical/bioactivities
FOOD="onion"                # used by /food/bioactivities

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bioactivity) BIOACTIVITY="$2"; shift 2 ;;
    --chemical)    CHEMICAL="$2";    shift 2 ;;
    --food)        FOOD="$2";        shift 2 ;;
    --url)         API_URL="$2";     shift 2 ;;
    --key)         API_KEY="$2";     shift 2 ;;
    -h|--help)     grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1 (try --help)" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# fall back to a local, gitignored staging.env (lines: API_URL=... / API_KEY=...)
if [[ -z "$API_URL" || -z "$API_KEY" ]] && [[ -f "$SCRIPT_DIR/staging.env" ]]; then
  set -a; . "$SCRIPT_DIR/staging.env"; set +a
fi
if [[ -z "$API_URL" || -z "$API_KEY" ]]; then
  echo "error: API_URL and API_KEY are not set." >&2
  echo "  export API_URL=... API_KEY=...   (see the guide -> Getting access & credentials)" >&2
  echo "  or create $SCRIPT_DIR/staging.env with API_URL=... and API_KEY=..." >&2
  exit 1
fi

OUT_DIR="$SCRIPT_DIR/bioactivity-api-examples"
mkdir -p "$OUT_DIR"

# call <output-name> <endpoint-path> <common_name-value>
call() {
  local name="$1" endpoint="$2" value="$3"
  local file="$OUT_DIR/$name.json"
  local http
  http=$(curl -s -G "$API_URL$endpoint" \
    --data-urlencode "common_name=$value" \
    -H "Authorization: Bearer $API_KEY" \
    -w '%{http_code}' -o "$file.tmp" || echo "000")

  # pretty-print + count rows with python (portable; no jq dependency)
  local rows
  if rows=$(python3 - "$file.tmp" "$file" <<'PY' 2>/dev/null
import json, sys
with open(sys.argv[1]) as fh:
    body = json.load(fh)
with open(sys.argv[2], "w") as fh:
    json.dump(body, fh, indent=2, ensure_ascii=False)
rows = body.get("data") if isinstance(body, dict) else body
print(len(rows) if isinstance(rows, list) else "obj")
PY
  ); then rm -f "$file.tmp"; else mv "$file.tmp" "$file"; rows="raw"; fi

  printf "  %-22s GET %-26s ?common_name=%-12s  → HTTP %s  (%s rows)\n" \
    "$name" "$endpoint" "$value" "$http" "$rows"
}

echo "Backend: $API_URL"
echo "Saving responses to: $OUT_DIR/"
echo
echo "Concept pages  (common_name=$BIOACTIVITY)"
call "bioactivity-metadata"   "/bioactivity/metadata"   "$BIOACTIVITY"
call "bioactivity-foods"      "/bioactivity/foods"      "$BIOACTIVITY"
call "bioactivity-chemicals"  "/bioactivity/chemicals"  "$BIOACTIVITY"
echo
echo "Chemical page  (common_name=$CHEMICAL)"
call "chemical-bioactivities" "/chemical/bioactivities" "$CHEMICAL"
echo
echo "Food page      (common_name=$FOOD)"
call "food-bioactivities"     "/food/bioactivities"     "$FOOD"
echo
echo "Done. Open the JSON files in $OUT_DIR/ — re-run with different --flags to refresh them."
