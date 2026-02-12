#!/bin/bash
# =============================================================================
# CryptoPulse - Configure Secrets via Key Vault
# =============================================================================
# Wires Key Vault secrets into Function App and Container Apps,
# and generates a .env.azure file for local development.
#
# Prerequisites:
#   - azure_setup.sh completed
#   - All services deployed
#
# Usage:
#   bash infrastructure/configure_secrets.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV="${CRYPTOPULSE_ENV:-dev}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"
KEYVAULT_NAME="kv-${PREFIX}-${ENV}"
FUNCAPP_NAME="func-${PREFIX}-${ENV}"
STORAGE_NAME="st${PREFIX}${ENV}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║      CryptoPulse Secrets Configuration           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── 1. Enable System Managed Identity ───────────────────────────────────────
echo "─── [1/4] Enabling Managed Identity ───"

# Function App
az functionapp identity assign \
    --name "$FUNCAPP_NAME" \
    --resource-group "$RG_NAME" \
    --output none 2>/dev/null || true
FUNC_IDENTITY=$(az functionapp identity show \
    --name "$FUNCAPP_NAME" \
    --resource-group "$RG_NAME" \
    --query principalId -o tsv 2>/dev/null || echo "")
echo "  ✓ Function App identity: ${FUNC_IDENTITY:0:12}..."

# Container Apps
for APP in cryptopulse-api cryptopulse-frontend; do
    az containerapp identity assign \
        --name "$APP" \
        --resource-group "$RG_NAME" \
        --system-assigned \
        --output none 2>/dev/null || true
done
echo "  ✓ Container App identities assigned"

# ─── 2. Grant Key Vault Access ───────────────────────────────────────────────
echo ""
echo "─── [2/4] Granting Key Vault access ───"

for PRINCIPAL in "$FUNC_IDENTITY"; do
    if [ -n "$PRINCIPAL" ]; then
        az keyvault set-policy \
            --name "$KEYVAULT_NAME" \
            --object-id "$PRINCIPAL" \
            --secret-permissions get list \
            --output none 2>/dev/null || true
    fi
done
echo "  ✓ Key Vault policies set"

# ─── 3. Store Additional Secrets ─────────────────────────────────────────────
echo ""
echo "─── [3/4] Storing additional secrets ───"

# Read from .env if available
if [ -f "${PROJECT_ROOT}/.env" ]; then
    echo "  Reading secrets from .env..."

    for KEY in GROQ_API_KEY BINANCE_API_KEY BINANCE_API_SECRET CRYPTOCOMPARE_API_KEY CRYPTOPANIC_API_KEY; do
        VALUE=$(grep "^${KEY}=" "${PROJECT_ROOT}/.env" 2>/dev/null | cut -d'=' -f2 || echo "")
        if [ -n "$VALUE" ]; then
            SECRET_NAME=$(echo "$KEY" | tr '_' '-' | tr '[:upper:]' '[:lower:]')
            az keyvault secret set --vault-name "$KEYVAULT_NAME" \
                --name "$SECRET_NAME" --value "$VALUE" --output none 2>/dev/null || true
            echo "  ✓ Secret: ${SECRET_NAME}"
        fi
    done
fi

# ─── 4. Generate .env.azure ──────────────────────────────────────────────────
echo ""
echo "─── [4/4] Generating .env.azure ───"

STORAGE_KEY=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-key" --query value -o tsv 2>/dev/null || echo "")
EVENTHUB_CONN=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "eventhub-connection-string" --query value -o tsv 2>/dev/null || echo "")
STORAGE_CONN=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "storage-connection-string" --query value -o tsv 2>/dev/null || echo "")
REDIS_CONN=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "redis-connection-string" --query value -o tsv 2>/dev/null || echo "")

# Get Databricks URL
DATABRICKS_URL=$(az databricks workspace show \
    --name "dbw-${PREFIX}-${ENV}" \
    --resource-group "$RG_NAME" \
    --query workspaceUrl -o tsv 2>/dev/null || echo "")

# Get Container App URLs
API_URL=$(az containerapp show --name "cryptopulse-api" --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || echo "")
FRONTEND_URL=$(az containerapp show --name "cryptopulse-frontend" --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || echo "")

# Get Redis host
REDIS_HOST=$(az redis show --name "${PREFIX}-redis-${ENV}" --resource-group "$RG_NAME" \
    --query hostName -o tsv 2>/dev/null || echo "")

ENV_FILE="${PROJECT_ROOT}/.env.azure"
cat > "$ENV_FILE" << ENVEOF
# =============================================================================
# CryptoPulse Azure Environment Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# =============================================================================

CRYPTOPULSE_ENV=production

# ─── Azure Storage (ADLS Gen2) ──────────────────────────────────────────────
AZURE_STORAGE_ACCOUNT_NAME=${STORAGE_NAME}
AZURE_STORAGE_ACCOUNT_KEY=${STORAGE_KEY}
AZURE_STORAGE_CONNECTION_STRING=${STORAGE_CONN}
AZURE_STORAGE_CONTAINER_NAME=cryptopulse-delta

# ─── Azure Event Hubs ──────────────────────────────────────────────────────
AZURE_EVENTHUB_CONNECTION_STRING=${EVENTHUB_CONN}
AZURE_EVENTHUB_NAMESPACE=evhns-${PREFIX}-${ENV}.servicebus.windows.net
AZURE_EVENTHUB_TRADES_NAME=trades
AZURE_EVENTHUB_NEWS_NAME=news

# ─── Azure Databricks ──────────────────────────────────────────────────────
AZURE_DATABRICKS_HOST=https://${DATABRICKS_URL}

# ─── Redis Cache ────────────────────────────────────────────────────────────
REDIS_URL=${REDIS_CONN}

# ─── Service URLs ───────────────────────────────────────────────────────────
API_URL=https://${API_URL}
FRONTEND_URL=https://${FRONTEND_URL}
FUNCAPP_URL=https://func-${PREFIX}-${ENV}.azurewebsites.net

# ─── Key Vault ──────────────────────────────────────────────────────────────
AZURE_KEYVAULT_NAME=${KEYVAULT_NAME}
ENVEOF

echo "  ✓ Generated: ${ENV_FILE}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║         Secrets Configuration Complete!          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Files:"
echo "  • .env.azure — Azure environment variables"
echo ""
echo "Services:"
echo "  • API:       https://${API_URL}"
echo "  • Frontend:  https://${FRONTEND_URL}"
echo "  • Functions: https://func-${PREFIX}-${ENV}.azurewebsites.net"
echo "  • Databricks:https://${DATABRICKS_URL}"
echo "  • Redis:     ${REDIS_HOST}:6380"
echo ""
