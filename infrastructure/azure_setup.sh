#!/bin/bash
# =============================================================================
# CryptoPulse - Azure Infrastructure Setup
# Provisions all required Azure resources via CLI
#
# Prerequisites:
#   - Azure CLI installed: brew install azure-cli
#   - Logged in: az login
#
# Usage:
#   chmod +x infrastructure/azure_setup.sh
#   bash infrastructure/azure_setup.sh
# =============================================================================

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────
ENV="${CRYPTOPULSE_ENV:-dev}"
LOCATION="${AZURE_LOCATION:-eastus}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"
STORAGE_NAME="st${PREFIX}${ENV}"        # must be lowercase, no hyphens
EVENTHUB_NS="evhns-${PREFIX}-${ENV}"
KEYVAULT_NAME="kv-${PREFIX}-${ENV}"
DATABRICKS_NAME="dbw-${PREFIX}-${ENV}"
ACR_NAME="cr${PREFIX}${ENV}"            # must be lowercase, no hyphens
FUNCAPP_NAME="func-${PREFIX}-${ENV}"
REDIS_NAME="${PREFIX}-redis-${ENV}"
CAE_NAME="cae-${PREFIX}-${ENV}"         # Container Apps Environment

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║      CryptoPulse Azure Infrastructure Setup      ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║  Environment:  ${ENV}                            ║"
echo "║  Location:     ${LOCATION}                       ║"
echo "║  Resource Group: ${RG_NAME}                      ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── Check Prerequisites ─────────────────────────────────────────────────────
echo "─── Checking prerequisites ───"

if ! command -v az &> /dev/null; then
    echo "✗ Azure CLI not found. Install: brew install azure-cli"
    exit 1
fi
echo "✓ Azure CLI found"

# Verify login
ACCOUNT=$(az account show --query name -o tsv 2>/dev/null || true)
if [ -z "$ACCOUNT" ]; then
    echo "⚠ Not logged in. Running 'az login'..."
    az login
    ACCOUNT=$(az account show --query name -o tsv)
fi
echo "✓ Logged in to: ${ACCOUNT}"

SUB_ID=$(az account show --query id -o tsv)
echo "✓ Subscription: ${SUB_ID}"
echo ""

# ─── 1. Resource Group ───────────────────────────────────────────────────────
echo "─── [1/9] Creating Resource Group ───"
az group create \
    --name "$RG_NAME" \
    --location "$LOCATION" \
    --tags project=CryptoPulse environment="${ENV}" managed_by=azure-cli \
    --output none
echo "✓ Resource Group: ${RG_NAME}"

# ─── 2. Storage Account (ADLS Gen2) ──────────────────────────────────────────
echo "─── [2/9] Creating Storage Account (ADLS Gen2) ───"
az storage account create \
    --name "$STORAGE_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none
echo "✓ Storage Account: ${STORAGE_NAME} (HNS enabled)"

# Get storage key for container creation
STORAGE_KEY=$(az storage account keys list \
    --account-name "$STORAGE_NAME" \
    --resource-group "$RG_NAME" \
    --query '[0].value' -o tsv)

# Create ADLS Gen2 filesystem (container) for Delta Lake
az storage container create \
    --name "cryptopulse-delta" \
    --account-name "$STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true
echo "  ✓ Container: cryptopulse-delta (Delta Lake tables)"

for CONTAINER in models mlflow; do
    az storage container create \
        --name "$CONTAINER" \
        --account-name "$STORAGE_NAME" \
        --account-key "$STORAGE_KEY" \
        --output none 2>/dev/null || true
    echo "  ✓ Container: ${CONTAINER}"
done

# Get storage connection string
STORAGE_CONN=$(az storage account show-connection-string \
    --name "$STORAGE_NAME" \
    --resource-group "$RG_NAME" \
    --query connectionString -o tsv)

# ─── 3. Event Hubs ───────────────────────────────────────────────────────────
echo "─── [3/9] Creating Event Hubs Namespace ───"
az eventhubs namespace create \
    --name "$EVENTHUB_NS" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku Standard \
    --capacity 1 \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none
echo "✓ Event Hubs Namespace: ${EVENTHUB_NS}"

# Create individual Event Hubs
for EH_NAME in trades news features predictions; do
    PARTITIONS=2
    [ "$EH_NAME" = "trades" ] && PARTITIONS=4
    az eventhubs eventhub create \
        --name "$EH_NAME" \
        --namespace-name "$EVENTHUB_NS" \
        --resource-group "$RG_NAME" \
        --partition-count $PARTITIONS \
        --output none
    echo "  ✓ Event Hub: ${EH_NAME} (${PARTITIONS} partitions)"
done

# Get connection string
EVENTHUB_CONN=$(az eventhubs namespace authorization-rule keys list \
    --resource-group "$RG_NAME" \
    --namespace-name "$EVENTHUB_NS" \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString -o tsv)

# ─── 4. Key Vault ────────────────────────────────────────────────────────────
echo "─── [4/9] Creating Key Vault ───"
az keyvault create \
    --name "$KEYVAULT_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --retention-days 7 \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Key Vault: ${KEYVAULT_NAME}"

# Store secrets in Key Vault
az keyvault secret set --vault-name "$KEYVAULT_NAME" \
    --name "eventhub-connection-string" --value "$EVENTHUB_CONN" --output none
echo "  ✓ Secret: eventhub-connection-string"

az keyvault secret set --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-key" --value "$STORAGE_KEY" --output none
echo "  ✓ Secret: storage-account-key"

az keyvault secret set --vault-name "$KEYVAULT_NAME" \
    --name "storage-connection-string" --value "$STORAGE_CONN" --output none
echo "  ✓ Secret: storage-connection-string"

az keyvault secret set --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-name" --value "$STORAGE_NAME" --output none
echo "  ✓ Secret: storage-account-name"

# ─── 5. Azure Databricks Workspace ───────────────────────────────────────────
echo "─── [5/9] Creating Databricks Workspace ───"
echo "  (this may take 3-5 minutes...)"
az databricks workspace create \
    --name "$DATABRICKS_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku premium \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Databricks Workspace: ${DATABRICKS_NAME}"

DATABRICKS_URL=$(az databricks workspace show \
    --name "$DATABRICKS_NAME" \
    --resource-group "$RG_NAME" \
    --query workspaceUrl -o tsv)

# ─── 6. Azure Container Registry ─────────────────────────────────────────────
echo "─── [6/9] Creating Container Registry ───"
az acr create \
    --name "$ACR_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku Basic \
    --admin-enabled true \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Container Registry: ${ACR_NAME}.azurecr.io"

ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)
ACR_USERNAME=$(az acr credential show --name "$ACR_NAME" --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$ACR_NAME" --query "passwords[0].value" -o tsv)

# Store ACR credentials in Key Vault
az keyvault secret set --vault-name "$KEYVAULT_NAME" \
    --name "acr-password" --value "$ACR_PASSWORD" --output none
echo "  ✓ Secret: acr-password"

# ─── 7. Azure Function App ───────────────────────────────────────────────────
echo "─── [7/9] Creating Function App ───"

# Create Consumption plan storage (Functions need their own)
FUNC_STORAGE="stfunc${PREFIX}${ENV}"
az storage account create \
    --name "$FUNC_STORAGE" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --tags project=CryptoPulse component=functions \
    --output none
echo "  ✓ Function storage: ${FUNC_STORAGE}"

az functionapp create \
    --name "$FUNCAPP_NAME" \
    --resource-group "$RG_NAME" \
    --storage-account "$FUNC_STORAGE" \
    --consumption-plan-location "$LOCATION" \
    --runtime python \
    --runtime-version 3.11 \
    --functions-version 4 \
    --os-type Linux \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Function App: ${FUNCAPP_NAME}"

# Configure Function App settings
az functionapp config appsettings set \
    --name "$FUNCAPP_NAME" \
    --resource-group "$RG_NAME" \
    --settings \
        "AZURE_EVENTHUB_CONNECTION_STRING=${EVENTHUB_CONN}" \
        "AZURE_STORAGE_ACCOUNT_NAME=${STORAGE_NAME}" \
        "AZURE_STORAGE_ACCOUNT_KEY=${STORAGE_KEY}" \
        "AZURE_STORAGE_CONNECTION_STRING=${STORAGE_CONN}" \
        "AZURE_STORAGE_CONTAINER_NAME=cryptopulse-delta" \
        "BINANCE_REST_URL=https://api.binance.com" \
        "BINANCE_TRADING_PAIRS=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT" \
    --output none
echo "  ✓ Function App settings configured"

FUNCAPP_URL="https://${FUNCAPP_NAME}.azurewebsites.net"

# ─── 8. Azure Cache for Redis ────────────────────────────────────────────────
echo "─── [8/9] Creating Redis Cache ───"
echo "  (this may take 5-10 minutes...)"
az redis create \
    --name "$REDIS_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku Basic \
    --vm-size C0 \
    --enable-non-ssl-port false \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Redis Cache: ${REDIS_NAME} (creation initiated)"

# Wait for Redis to finish provisioning (it's async)
echo "  Waiting for Redis provisioning..."
REDIS_READY=false
for i in $(seq 1 30); do
    STATE=$(az redis show --name "$REDIS_NAME" --resource-group "$RG_NAME" \
        --query provisioningState -o tsv 2>/dev/null || echo "NotReady")
    if [ "$STATE" = "Succeeded" ]; then
        REDIS_READY=true
        break
    fi
    echo "  ⏳ Provisioning... (${STATE}) - attempt ${i}/30"
    sleep 30
done

if [ "$REDIS_READY" = true ]; then
    REDIS_HOST=$(az redis show --name "$REDIS_NAME" --resource-group "$RG_NAME" \
        --query hostName -o tsv)
    REDIS_KEY=$(az redis list-keys --name "$REDIS_NAME" --resource-group "$RG_NAME" \
        --query primaryKey -o tsv)
    REDIS_CONN="rediss://:${REDIS_KEY}@${REDIS_HOST}:6380/0"

    az keyvault secret set --vault-name "$KEYVAULT_NAME" \
        --name "redis-connection-string" --value "$REDIS_CONN" --output none
    echo "  ✓ Secret: redis-connection-string"
else
    echo "  ⚠ Redis still provisioning. Run 'bash infrastructure/configure_secrets.sh' later to store the connection string."
    REDIS_HOST="${REDIS_NAME}.redis.cache.windows.net"
    REDIS_CONN=""
fi

# ─── 9. Container Apps Environment ───────────────────────────────────────────
echo "─── [9/9] Creating Container Apps Environment ───"

az containerapp env create \
    --name "$CAE_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none 2>/dev/null || true
echo "✓ Container Apps Environment: ${CAE_NAME}"

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║              CryptoPulse Infrastructure Complete!               ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "┌─────────────────────────────────────────────────────────────────┐"
echo "│ Resource               │ Name / URL                            │"
echo "├─────────────────────────────────────────────────────────────────┤"
echo "│ Resource Group         │ ${RG_NAME}"
echo "│ ADLS Gen2 Storage      │ ${STORAGE_NAME}"
echo "│ Event Hubs Namespace   │ ${EVENTHUB_NS}.servicebus.windows.net"
echo "│ Key Vault              │ https://kv-${PREFIX}-${ENV}.vault.azure.net"
echo "│ Databricks             │ https://${DATABRICKS_URL}"
echo "│ Container Registry     │ ${ACR_LOGIN_SERVER}"
echo "│ Function App           │ ${FUNCAPP_URL}"
echo "│ Redis Cache            │ ${REDIS_HOST}:6380"
echo "│ Container Apps Env     │ ${CAE_NAME}"
echo "└─────────────────────────────────────────────────────────────────┘"
echo ""
echo "─── Next Steps ───"
echo ""
echo "1. Build & push Docker images:"
echo "   bash infrastructure/deploy_containers.sh"
echo ""
echo "2. Deploy Azure Functions:"
echo "   bash infrastructure/deploy_functions.sh"
echo ""
echo "3. Deploy API + Frontend to Container Apps:"
echo "   bash infrastructure/deploy_apps.sh"
echo ""
echo "4. Setup Databricks workspace:"
echo "   bash infrastructure/databricks_setup.sh"
echo ""
echo "5. Verify everything:"
echo "   bash infrastructure/verify_deployment.sh"
echo ""
echo "─── .env values ───"
echo ""
echo "AZURE_STORAGE_ACCOUNT_NAME=${STORAGE_NAME}"
echo "AZURE_STORAGE_ACCOUNT_KEY=${STORAGE_KEY}"
echo "AZURE_STORAGE_CONNECTION_STRING=${STORAGE_CONN}"
echo "AZURE_STORAGE_CONTAINER_NAME=cryptopulse-delta"
echo "AZURE_EVENTHUB_CONNECTION_STRING=${EVENTHUB_CONN}"
echo "DATABRICKS_HOST=https://${DATABRICKS_URL}"
echo "REDIS_URL=${REDIS_CONN}"
echo "ACR_LOGIN_SERVER=${ACR_LOGIN_SERVER}"
echo ""
