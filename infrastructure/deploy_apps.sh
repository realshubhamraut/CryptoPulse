#!/bin/bash
# =============================================================================
# CryptoPulse - Deploy to Azure Container Apps
# =============================================================================
# Deploys the FastAPI and Flask Frontend to Azure Container Apps.
#
# Prerequisites:
#   - azure_setup.sh completed (Container Apps Environment created)
#   - deploy_containers.sh completed (images pushed to ACR)
#
# Usage:
#   bash infrastructure/deploy_apps.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV="${CRYPTOPULSE_ENV:-dev}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"
ACR_NAME="cr${PREFIX}${ENV}"
CAE_NAME="cae-${PREFIX}-${ENV}"
KEYVAULT_NAME="kv-${PREFIX}-${ENV}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║     CryptoPulse Container Apps Deployment        ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── Get resource info ───────────────────────────────────────────────────────
ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --resource-group "$RG_NAME" \
    --query loginServer -o tsv)
ACR_USERNAME=$(az acr credential show --name "$ACR_NAME" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$ACR_NAME" --query "passwords[0].value" -o tsv)

echo "✓ ACR: ${ACR_LOGIN_SERVER}"

# Get secrets from Key Vault
EVENTHUB_CONN=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "eventhub-connection-string" --query value -o tsv)
STORAGE_KEY=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-key" --query value -o tsv)
STORAGE_NAME=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-name" --query value -o tsv)
REDIS_CONN=$(az keyvault secret show --vault-name "$KEYVAULT_NAME" \
    --name "redis-connection-string" --query value -o tsv 2>/dev/null || echo "")

# Read GROQ key from .env
GROQ_KEY=""
if [ -f "${PROJECT_ROOT}/.env" ]; then
    GROQ_KEY=$(grep "^GROQ_API_KEY=" "${PROJECT_ROOT}/.env" 2>/dev/null | cut -d'=' -f2 || echo "")
fi

echo "✓ Secrets loaded from Key Vault"

# ─── Deploy API Container App ────────────────────────────────────────────────
echo ""
echo "─── [1/2] Deploying API Container App ───"

az containerapp create \
    --name "cryptopulse-api" \
    --resource-group "$RG_NAME" \
    --environment "$CAE_NAME" \
    --image "${ACR_LOGIN_SERVER}/cryptopulse-api:latest" \
    --registry-server "$ACR_LOGIN_SERVER" \
    --registry-username "$ACR_USERNAME" \
    --registry-password "$ACR_PASSWORD" \
    --target-port 8000 \
    --ingress external \
    --min-replicas 1 \
    --max-replicas 3 \
    --cpu 0.5 \
    --memory 1.0Gi \
    --env-vars \
        "CRYPTOPULSE_ENV=production" \
        "PYTHONPATH=/app" \
        "API_HOST=0.0.0.0" \
        "API_PORT=8000" \
        "API_WORKERS=4" \
        "API_RELOAD=false" \
        "AZURE_EVENTHUB_CONNECTION_STRING=${EVENTHUB_CONN}" \
        "AZURE_STORAGE_ACCOUNT_NAME=${STORAGE_NAME}" \
        "AZURE_STORAGE_ACCOUNT_KEY=${STORAGE_KEY}" \
        "AZURE_STORAGE_CONTAINER_NAME=cryptopulse-delta" \
        "REDIS_URL=${REDIS_CONN}" \
        "GROQ_API_KEY=${GROQ_KEY}" \
    --tags project=CryptoPulse component=api \
    --output none

API_URL=$(az containerapp show \
    --name "cryptopulse-api" \
    --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv)

echo "✓ API deployed: https://${API_URL}"

# ─── Deploy Frontend Container App ───────────────────────────────────────────
echo ""
echo "─── [2/2] Deploying Frontend Container App ───"

az containerapp create \
    --name "cryptopulse-frontend" \
    --resource-group "$RG_NAME" \
    --environment "$CAE_NAME" \
    --image "${ACR_LOGIN_SERVER}/cryptopulse-frontend:latest" \
    --registry-server "$ACR_LOGIN_SERVER" \
    --registry-username "$ACR_USERNAME" \
    --registry-password "$ACR_PASSWORD" \
    --target-port 5050 \
    --ingress external \
    --min-replicas 1 \
    --max-replicas 2 \
    --cpu 0.25 \
    --memory 0.5Gi \
    --env-vars \
        "PYTHONPATH=/app" \
        "API_URL=https://${API_URL}" \
    --tags project=CryptoPulse component=frontend \
    --output none

FRONTEND_URL=$(az containerapp show \
    --name "cryptopulse-frontend" \
    --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv)

echo "✓ Frontend deployed: https://${FRONTEND_URL}"

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║       Container Apps Deployed!                   ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "┌────────────────────────────────────────────────────────────┐"
echo "│ Service          │ URL                                     │"
echo "├────────────────────────────────────────────────────────────┤"
echo "│ API (FastAPI)    │ https://${API_URL}"
echo "│   └─ Health      │ https://${API_URL}/health"
echo "│   └─ Swagger     │ https://${API_URL}/docs"
echo "│   └─ Analyze     │ https://${API_URL}/api/v1/analyze"
echo "│ Frontend (Flask) │ https://${FRONTEND_URL}"
echo "│   └─ Dashboard   │ https://${FRONTEND_URL}/"
echo "│   └─ News        │ https://${FRONTEND_URL}/news"
echo "│   └─ ML Pipeline │ https://${FRONTEND_URL}/ml-pipeline"
echo "└────────────────────────────────────────────────────────────┘"
echo ""
echo "Next: bash infrastructure/databricks_setup.sh"
echo ""
