#!/bin/bash
# Downloads and extracts text_parser_predictions.zip from Box
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ZIP_PATH="${SCRIPT_DIR}/text_parser_predictions.zip"

echo "Downloading text_parser_predictions.zip..."
curl -L "https://ucdavis.box.com/s/t8tzp9tq4vgrkbay11i5ubvhtvzpvuhn?dl=1" -o "${ZIP_PATH}"

echo "Extracting..."
unzip -o "${ZIP_PATH}" -d "${SCRIPT_DIR}"

echo "Done. Removing zip..."
rm "${ZIP_PATH}"
