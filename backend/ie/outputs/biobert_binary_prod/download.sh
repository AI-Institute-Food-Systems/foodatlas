#!/bin/bash
# Downloads and extracts biobert_binary_prod.zip from Box
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ZIP_PATH="${SCRIPT_DIR}/biobert_binary_prod.zip"

echo "Downloading biobert_binary_prod.zip..."
curl -L "https://ucdavis.box.com/s/18sm7wu3nvklb5rj07kjqlx9xu0cci18?dl=1" -o "${ZIP_PATH}"

echo "Extracting..."
unzip -o "${ZIP_PATH}" -d "${SCRIPT_DIR}/.."

echo "Done. Removing zip..."
rm "${ZIP_PATH}"
