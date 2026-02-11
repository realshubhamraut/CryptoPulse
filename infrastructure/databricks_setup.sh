#!/bin/bash
# =============================================================================
# CryptoPulse - Databricks Workspace Setup
# Configures Databricks: uploads project, imports notebooks, creates cluster
#
# Prerequisites:
#   - Run azure_setup.sh first
#   - Databricks CLI configured: databricks configure --token
#
# Usage:
#   bash infrastructure/databricks_setup.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV="${CRYPTOPULSE_ENV:-dev}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║       CryptoPulse Databricks Setup              ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── Check Prerequisites ─────────────────────────────────────────────────────
if ! command -v databricks &> /dev/null; then
    echo "✗ Databricks CLI not found. Install: pip install databricks-cli"
    exit 1
fi
echo "✓ Databricks CLI found"

# Test connection
databricks workspace ls / > /dev/null 2>&1 || {
    echo "✗ Databricks CLI not configured. Run: databricks configure --token"
    exit 1
}
echo "✓ Databricks CLI connected"

# ─── 1. Create DBFS Directories ──────────────────────────────────────────────
echo ""
echo "─── [1/4] Creating DBFS directories ───"

for DIR in /cryptopulse /cryptopulse/checkpoints /cryptopulse/models /cryptopulse/libs; do
    databricks fs mkdirs "dbfs:${DIR}" 2>/dev/null || true
    echo "  ✓ dbfs:${DIR}"
done

# ─── 2. Build & Upload Project Wheel ─────────────────────────────────────────
echo ""
echo "─── [2/4] Building and uploading project wheel ───"

cd "$PROJECT_ROOT"
pip install build --quiet
python -m build --wheel --outdir dist/ 2>/dev/null

WHEEL_FILE=$(ls -t dist/cryptopulse-*.whl 2>/dev/null | head -1)
if [ -n "$WHEEL_FILE" ]; then
    databricks fs cp "$WHEEL_FILE" "dbfs:/cryptopulse/libs/" --overwrite
    echo "  ✓ Uploaded: ${WHEEL_FILE} → dbfs:/cryptopulse/libs/"
else
    echo "  ⚠ No wheel built. Skipping upload."
fi

# ─── 3. Import Notebooks ─────────────────────────────────────────────────────
echo ""
echo "─── [3/4] Importing notebooks ───"

# Create workspace directory
databricks workspace mkdirs /CryptoPulse 2>/dev/null || true
databricks workspace mkdirs /CryptoPulse/notebooks 2>/dev/null || true

# Import each notebook (.ipynb format)
if [ -d "$PROJECT_ROOT/notebooks" ]; then
    for NB in "$PROJECT_ROOT"/notebooks/*.ipynb; do
        NB_NAME=$(basename "$NB" .ipynb)
        databricks workspace import "$NB" "/CryptoPulse/notebooks/${NB_NAME}" \
            --language PYTHON --format JUPYTER --overwrite 2>/dev/null || true
        echo "  ✓ Imported: ${NB_NAME}"
    done
else
    echo "  No notebooks directory found. Skipping."
fi

# ─── 4. Create Cluster ───────────────────────────────────────────────────────
echo ""
echo "─── [4/4] Creating compute cluster ───"

CLUSTER_CONFIG=$(cat <<EOF
{
    "cluster_name": "cryptopulse-${ENV}",
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "autotermination_minutes": 120,
    "num_workers": 1,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    "spark_env_vars": {
        "CRYPTOPULSE_ENV": "${ENV}"
    },
    "custom_tags": {
        "project": "CryptoPulse",
        "environment": "${ENV}"
    }
}
EOF
)

# Check if cluster already exists
EXISTING_CLUSTER=$(databricks clusters list --output JSON 2>/dev/null | \
    python3 -c "import json,sys; clusters=json.load(sys.stdin).get('clusters',[]); print(next((c['cluster_id'] for c in clusters if c['cluster_name']=='cryptopulse-${ENV}'), ''))" 2>/dev/null || echo "")

if [ -n "$EXISTING_CLUSTER" ]; then
    echo "  ✓ Cluster already exists: ${EXISTING_CLUSTER}"
else
    echo "$CLUSTER_CONFIG" | databricks clusters create --json-file /dev/stdin 2>/dev/null || {
        echo "  ⚠ Cluster creation requires Databricks workspace UI or API token with permissions."
        echo "  Create manually in your workspace with:"
        echo "    - Name: cryptopulse-${ENV}"
        echo "    - Runtime: 14.3 LTS (Scala 2.12, Spark 3.5.0)"
        echo "    - Node type: Standard_DS3_v2"
        echo "    - Workers: 1"
        echo "    - Auto-terminate: 120 minutes"
    }
fi

# ─── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║         Databricks Setup Complete!               ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Your notebooks are at: /CryptoPulse/ in the Databricks workspace"
echo ""
echo "Next steps:"
echo "  1. Open Databricks workspace"
echo "  2. Attach cluster 'cryptopulse-${ENV}' to notebooks"
echo "  3. Run notebooks in order: 01 → 02 → ... → 10"
echo ""
