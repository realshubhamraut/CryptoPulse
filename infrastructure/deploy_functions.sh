#!/bin/bash
# =============================================================================
# CryptoPulse - Deploy Azure Functions
# =============================================================================
# Deploys the timer-triggered ingestion functions (Binance + News)
# to the Azure Function App.
#
# Prerequisites:
#   - azure_setup.sh completed (Function App created)
#   - Azure Functions Core Tools: brew install azure-functions-core-tools@4
#
# Usage:
#   bash infrastructure/deploy_functions.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV="${CRYPTOPULSE_ENV:-dev}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"
FUNCAPP_NAME="func-${PREFIX}-${ENV}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║      CryptoPulse Azure Functions Deploy          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── Prerequisites ───────────────────────────────────────────────────────────
if ! command -v func &> /dev/null; then
    echo "✗ Azure Functions Core Tools not found."
    echo "  Install: brew install azure-functions-core-tools@4"
    exit 1
fi
echo "✓ Azure Functions Core Tools found"

# Verify Function App exists
FUNC_STATUS=$(az functionapp show --name "$FUNCAPP_NAME" --resource-group "$RG_NAME" \
    --query state -o tsv 2>/dev/null || echo "")
if [ -z "$FUNC_STATUS" ]; then
    echo "✗ Function App '${FUNCAPP_NAME}' not found. Run azure_setup.sh first."
    exit 1
fi
echo "✓ Function App: ${FUNCAPP_NAME} (state: ${FUNC_STATUS})"

# ─── Prepare Function Project ────────────────────────────────────────────────
echo ""
echo "─── [1/3] Preparing function project ───"

# Create a temporary deployment directory with the correct Azure Functions structure
DEPLOY_DIR=$(mktemp -d)
trap "rm -rf $DEPLOY_DIR" EXIT

# Copy function files
cp "${PROJECT_ROOT}/functions/host.json" "$DEPLOY_DIR/"
cp -r "${PROJECT_ROOT}/functions/binance_trigger" "$DEPLOY_DIR/"
cp -r "${PROJECT_ROOT}/functions/news_trigger" "$DEPLOY_DIR/"

# Copy function code
cp "${PROJECT_ROOT}/functions/binance_ingestion.py" "$DEPLOY_DIR/"
cp "${PROJECT_ROOT}/functions/news_ingestion.py" "$DEPLOY_DIR/"
cp "${PROJECT_ROOT}/functions/__init__.py" "$DEPLOY_DIR/functions_init.py"

# Copy requirements
cp "${PROJECT_ROOT}/functions/requirements.txt" "$DEPLOY_DIR/"

# Copy required library modules
cp -r "${PROJECT_ROOT}/cryptopulse" "$DEPLOY_DIR/"

echo "  ✓ Function project prepared in ${DEPLOY_DIR}"

# ─── Configure App Settings ──────────────────────────────────────────────────
echo ""
echo "─── [2/3] Updating app settings ───"

# Read .env if available and set any GROQ/API keys
if [ -f "${PROJECT_ROOT}/.env" ]; then
    GROQ_KEY=$(grep "^GROQ_API_KEY=" "${PROJECT_ROOT}/.env" 2>/dev/null | cut -d'=' -f2 || echo "")
    NEWS_API_KEY=$(grep "^CRYPTOCOMPARE_API_KEY=" "${PROJECT_ROOT}/.env" 2>/dev/null | cut -d'=' -f2 || echo "")

    SETTINGS=""
    [ -n "$GROQ_KEY" ] && SETTINGS="GROQ_API_KEY=${GROQ_KEY}"
    [ -n "$NEWS_API_KEY" ] && SETTINGS="${SETTINGS} CRYPTOCOMPARE_API_KEY=${NEWS_API_KEY}"

    if [ -n "$SETTINGS" ]; then
        az functionapp config appsettings set \
            --name "$FUNCAPP_NAME" \
            --resource-group "$RG_NAME" \
            --settings $SETTINGS \
            --output none
        echo "  ✓ API keys configured from .env"
    fi
fi
echo "  ✓ App settings verified"

# ─── Deploy ──────────────────────────────────────────────────────────────────
echo ""
echo "─── [3/3] Deploying functions ───"

cd "$DEPLOY_DIR"
func azure functionapp publish "$FUNCAPP_NAME" --python

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║       Azure Functions Deployed!                  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Functions:"
echo "  • Binance Ingestion (timer: every 1 min)"
echo "    URL: https://${FUNCAPP_NAME}.azurewebsites.net/api/binance_trigger"
echo ""
echo "  • News Ingestion (timer: every 5 min)"
echo "    URL: https://${FUNCAPP_NAME}.azurewebsites.net/api/news_trigger"
echo ""
echo "View logs:"
echo "  func azure functionapp logstream ${FUNCAPP_NAME}"
echo ""
echo "Next: bash infrastructure/deploy_apps.sh"
echo ""
