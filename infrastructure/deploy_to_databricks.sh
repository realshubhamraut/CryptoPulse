#!/bin/bash
# =============================================================================
# CryptoPulse - Deploy to Databricks
# Quick deploy: rebuild wheel + re-import notebooks
#
# Usage:
#   bash infrastructure/deploy_to_databricks.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "─── Deploying CryptoPulse to Databricks ───"

# 1. Rebuild wheel
echo "[1/2] Building wheel..."
cd "$PROJECT_ROOT"
python -m build --wheel --outdir dist/ 2>/dev/null
WHEEL_FILE=$(ls -t dist/cryptopulse-*.whl | head -1)
databricks fs cp "$WHEEL_FILE" "dbfs:/cryptopulse/libs/" --overwrite
echo "  ✓ Wheel uploaded"

# 2. Re-import notebooks
echo "[2/2] Importing notebooks..."
for NB in "$PROJECT_ROOT"/notebooks/*.ipynb; do
    NB_NAME=$(basename "$NB" .ipynb)
    databricks workspace import "$NB" "/CryptoPulse/notebooks/${NB_NAME}" \
        --language PYTHON --format JUPYTER --overwrite 2>/dev/null || true
    echo "  ${NB_NAME}"
done

echo ""
echo "✓ Deployment complete!"
