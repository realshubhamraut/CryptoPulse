#!/bin/bash
# =============================================================================
# CryptoPulse - Deploy Azure Functions (via zip deploy)
# =============================================================================
# Deploys the timer-triggered ingestion functions (Binance + News)
# using az CLI zip deployment (no func CLI needed).
#
# Prerequisites:
#   - azure_setup.sh completed (Function App created)
#   - Azure CLI installed
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
# Verify Function App exists
FUNC_STATUS=$(az functionapp show --name "$FUNCAPP_NAME" --resource-group "$RG_NAME" \
    --query state -o tsv 2>/dev/null || echo "")
if [ -z "$FUNC_STATUS" ]; then
    echo "✗ Function App '${FUNCAPP_NAME}' not found. Run azure_setup.sh first."
    exit 1
fi
echo "✓ Function App: ${FUNCAPP_NAME} (state: ${FUNC_STATUS})"

# ─── Build Deployment Package ────────────────────────────────────────────────
echo ""
echo "─── [1/3] Building deployment package ───"

DEPLOY_DIR=$(mktemp -d)
trap "rm -rf $DEPLOY_DIR" EXIT

# Copy function host config
cp "${PROJECT_ROOT}/functions/host.json" "$DEPLOY_DIR/"

# Copy function trigger configs
cp -r "${PROJECT_ROOT}/functions/binance_trigger" "$DEPLOY_DIR/"
cp -r "${PROJECT_ROOT}/functions/news_trigger" "$DEPLOY_DIR/"

# Copy function handler code
cp "${PROJECT_ROOT}/functions/binance_ingestion.py" "$DEPLOY_DIR/"
cp "${PROJECT_ROOT}/functions/news_ingestion.py" "$DEPLOY_DIR/"
cp "${PROJECT_ROOT}/functions/__init__.py" "$DEPLOY_DIR/"

# Copy requirements
cp "${PROJECT_ROOT}/functions/requirements.txt" "$DEPLOY_DIR/"

# Copy required library modules
cp -r "${PROJECT_ROOT}/cryptopulse" "$DEPLOY_DIR/"

# Create zip package
ZIP_FILE="${DEPLOY_DIR}/deploy.zip"
cd "$DEPLOY_DIR"
zip -r "$ZIP_FILE" . -x "deploy.zip" > /dev/null
echo "  ✓ Package created ($(du -h "$ZIP_FILE" | cut -f1))"

# ─── Configure App Settings ──────────────────────────────────────────────────
echo ""
echo "─── [2/3] Updating app settings ───"

# Read .env if available and set any API keys
if [ -f "${PROJECT_ROOT}/.env" ]; then
    SETTINGS=""
    for KEY in GROQ_API_KEY CRYPTOCOMPARE_API_KEY BINANCE_API_KEY NEWS_API_KEY; do
        VALUE=$(grep "^${KEY}=" "${PROJECT_ROOT}/.env" 2>/dev/null | cut -d'=' -f2 || echo "")
        if [ -n "$VALUE" ]; then
            SETTINGS="${SETTINGS} ${KEY}=${VALUE}"
        fi
    done

    if [ -n "$SETTINGS" ]; then
        az functionapp config appsettings set \
            --name "$FUNCAPP_NAME" \
            --resource-group "$RG_NAME" \
            --settings $SETTINGS \
            --output none 2>/dev/null || true
        echo "  ✓ API keys configured from .env"
    fi
fi
echo "  ✓ App settings verified"

# ─── Deploy via Zip ──────────────────────────────────────────────────────────
echo ""
echo "─── [3/3] Deploying via zip deploy ───"

az functionapp deployment source config-zip \
    --name "$FUNCAPP_NAME" \
    --resource-group "$RG_NAME" \
    --src "$ZIP_FILE" \
    --output none

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
echo "  az functionapp log tail --name ${FUNCAPP_NAME} --resource-group ${RG_NAME}"
echo ""
echo "Next: bash infrastructure/deploy_apps.sh"
echo ""
