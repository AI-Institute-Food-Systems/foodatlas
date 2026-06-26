#!/bin/bash
# Publish a public FoodAtlas release from a KGC run already in the
# private KGC bucket.
#
# Source: s3://<kgc-bucket>/outputs/<run>/kg/   (parquets + CHANGELOG.md)
# Target: s3://<downloads-bucket>/bundles/foodatlas-<version>/
#         ├── foodatlas-<version>.zip   (parquets + CHANGELOG + SUMMARY + README)
#         └── SUMMARY.md                (standalone, fetched by the UI)
#
# Also updates s3://<downloads-bucket>/bundles/index.json — the manifest
# the API reads — adding/replacing an entry for <version> with the
# release date, file size, public download URL, and the originating
# kgc_run id (so any release is traceable back to a specific KGC run).
#
# Usage:
#   publish-bundle.sh <version> <summary-file> [--kgc-run <id>] [--release-date <YYYY-MM-DD>]
#
# Example:
#   publish-bundle.sh v1.0 ./release-notes/SUMMARY-v1.0.md
#   publish-bundle.sh v1.1 ./SUMMARY.md --kgc-run 20260420T173000Z

set -euo pipefail

VERSION=""
SUMMARY_FILE=""
KGC_RUN=""
RELEASE_DATE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --kgc-run)
            KGC_RUN="$2"; shift 2 ;;
        --release-date)
            RELEASE_DATE="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,17p' "$0" | sed 's/^# *//'; exit 0 ;;
        --*)
            echo "Unknown option: $1" >&2; exit 1 ;;
        *)
            if [[ -z "$VERSION" ]]; then
                VERSION="$1"
            elif [[ -z "$SUMMARY_FILE" ]]; then
                SUMMARY_FILE="$1"
            else
                echo "Unexpected argument: $1" >&2; exit 1
            fi
            shift ;;
    esac
done

if [[ -z "$VERSION" || -z "$SUMMARY_FILE" ]]; then
    echo "Usage: $0 <version> <summary-file> [--kgc-run <id>] [--release-date <YYYY-MM-DD>]" >&2
    exit 1
fi

if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "Error: summary file not found: $SUMMARY_FILE" >&2
    exit 1
fi

RELEASE_DATE="${RELEASE_DATE:-$(date -u +%Y-%m-%d)}"
REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || echo us-west-1)}"

_cfn_output() {
    aws cloudformation describe-stacks \
        --stack-name "$1" \
        --region "$REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" \
        --output text
}

KGC_BUCKET=$(_cfn_output FoodAtlasStorageStack KgcBucketName)
DOWNLOADS_BUCKET=$(_cfn_output FoodAtlasDownloadsStack DownloadsBucketName)

for v in KGC_BUCKET DOWNLOADS_BUCKET; do
    if [[ -z "${!v}" || "${!v}" == "None" ]]; then
        echo "Error: could not resolve $v from CloudFormation outputs." >&2
        exit 1
    fi
done

if [[ -z "$KGC_RUN" ]]; then
    KGC_RUN=$(aws s3 cp "s3://$KGC_BUCKET/outputs/LATEST" - --region "$REGION" 2>/dev/null || true)
    if [[ -z "$KGC_RUN" ]]; then
        echo "Error: s3://$KGC_BUCKET/outputs/LATEST is missing or empty." >&2
        echo "Pass --kgc-run <id> or run sync-outputs-to-s3.sh first." >&2
        exit 1
    fi
    echo "Resolved KGC run from LATEST: $KGC_RUN"
fi

KGC_PREFIX="s3://$KGC_BUCKET/outputs/$KGC_RUN/kg"
if ! aws s3 ls "$KGC_PREFIX/CHANGELOG.md" --region "$REGION" >/dev/null 2>&1; then
    cat >&2 <<MISSING
Error: $KGC_PREFIX/CHANGELOG.md does not exist.

That KGC run was published before CHANGELOG.md became required.
Backfill it (regenerate the changelog and upload to that run's kg/
prefix) before publishing a public release from it.
MISSING
    exit 1
fi

BUNDLE_NAME="foodatlas-${VERSION}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
STAGE_DIR="$TMP_DIR/$BUNDLE_NAME"
mkdir -p "$STAGE_DIR"

echo "Downloading $KGC_PREFIX/ -> staging..."
aws s3 sync "$KGC_PREFIX/" "$STAGE_DIR/" \
    --region "$REGION" \
    --exclude "*" \
    --include "*.parquet" \
    --include "CHANGELOG.md" \
    --exclude "entity_registry.parquet" \
    --exclude "checkpoints/*" \
    --exclude "diagnostics/*" \
    --exclude "intermediate/*"

cp "$SUMMARY_FILE" "$STAGE_DIR/SUMMARY.md"

cat > "$STAGE_DIR/README.md" <<EOF
# FoodAtlas Data Bundle — ${VERSION}

Version-controlled snapshot of the FoodAtlas knowledge graph as parquet
files. Released under the Apache-2.0 license.

See \`SUMMARY.md\` for a short release blurb and \`CHANGELOG.md\` for
the full KG-level diff against the previous release.

Source KGC run: \`${KGC_RUN}\`
Release date:   \`${RELEASE_DATE}\`

## Loading

Use any parquet-aware tool (pandas, polars, DuckDB, Spark). The
FoodAtlas API loads these into PostgreSQL via \`backend/db/main.py load\`.
EOF

ZIP_PATH="$TMP_DIR/${BUNDLE_NAME}.zip"
(cd "$TMP_DIR" && zip -qr "$ZIP_PATH" "$BUNDLE_NAME")

ZIP_KEY="bundles/${BUNDLE_NAME}/${BUNDLE_NAME}.zip"
SUMMARY_KEY="bundles/${BUNDLE_NAME}/SUMMARY.md"
MANIFEST_KEY="bundles/index.json"
BASE_URL="https://${DOWNLOADS_BUCKET}.s3.${REGION}.amazonaws.com"

FILE_SIZE_BYTES=$(stat -c %s "$ZIP_PATH" 2>/dev/null || stat -f %z "$ZIP_PATH")
FILE_SIZE_HUMAN=$(python3 -c "
n = $FILE_SIZE_BYTES
for unit in ('B','KB','MB','GB','TB'):
    if n < 1024 or unit == 'TB':
        print(f'{n:.1f} {unit}' if unit != 'B' else f'{n} {unit}')
        break
    n /= 1024
")

echo
echo "Publishing"
echo "  version:   $VERSION"
echo "  date:      $RELEASE_DATE"
echo "  size:      $FILE_SIZE_HUMAN"
echo "  kgc_run:   $KGC_RUN"
echo "  to:        s3://$DOWNLOADS_BUCKET/bundles/${BUNDLE_NAME}/"
echo

aws s3 cp "$ZIP_PATH" "s3://${DOWNLOADS_BUCKET}/${ZIP_KEY}" --region "$REGION"
aws s3 cp "$STAGE_DIR/SUMMARY.md" "s3://${DOWNLOADS_BUCKET}/${SUMMARY_KEY}" \
    --content-type "text/markdown; charset=utf-8" \
    --cache-control "public, max-age=300" \
    --region "$REGION"

MANIFEST_PATH="$TMP_DIR/index.json"
if aws s3api head-object --bucket "$DOWNLOADS_BUCKET" --key "$MANIFEST_KEY" --region "$REGION" >/dev/null 2>&1; then
    aws s3 cp "s3://${DOWNLOADS_BUCKET}/${MANIFEST_KEY}" "$MANIFEST_PATH" --region "$REGION"
else
    echo "[]" > "$MANIFEST_PATH"
fi

python3 - "$MANIFEST_PATH" "$VERSION" "$RELEASE_DATE" "$FILE_SIZE_HUMAN" \
    "${BASE_URL}/${ZIP_KEY}" "${BASE_URL}/${SUMMARY_KEY}" "$KGC_RUN" <<'PY'
import json
import sys
from pathlib import Path

path, version, release_date, file_size, zip_url, summary_url, kgc_run = sys.argv[1:8]
manifest_path = Path(path)

try:
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, list):
        raise ValueError
except (json.JSONDecodeError, ValueError):
    manifest = []

manifest = [e for e in manifest if e.get("version") != version]
manifest.append(
    {
        "version": version,
        "release_date": release_date,
        "file_size": file_size,
        "kgc_run": kgc_run,
        "download_link": zip_url,
        "summary_link": summary_url,
    }
)
manifest.sort(key=lambda e: e["release_date"], reverse=True)

manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
PY

aws s3 cp "$MANIFEST_PATH" "s3://${DOWNLOADS_BUCKET}/${MANIFEST_KEY}" \
    --content-type "application/json" \
    --cache-control "public, max-age=300" \
    --region "$REGION"

echo
echo "Published:"
echo "  zip:       ${BASE_URL}/${ZIP_KEY}"
echo "  summary:   ${BASE_URL}/${SUMMARY_KEY}"
echo "  manifest:  ${BASE_URL}/${MANIFEST_KEY}"
