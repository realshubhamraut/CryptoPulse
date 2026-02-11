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
echo "─── [1/5] Creating Resource Group ───"
az group create \
    --name "$RG_NAME" \
    --location "$LOCATION" \
    --tags project=CryptoPulse environment="${ENV}" managed_by=azure-cli \
    --output none
echo "✓ Resource Group: ${RG_NAME}"

# ─── 2. Storage Account (ADLS Gen2) ──────────────────────────────────────────
echo "─── [2/5] Creating Storage Account (ADLS Gen2) ───"
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

# Create containers for each Delta Lake layer
for CONTAINER in bronze silver gold checkpoints models mlflow; do
    az storage container create \
        --name "$CONTAINER" \
        --account-name "$STORAGE_NAME" \
        --account-key "$STORAGE_KEY" \
        --output none 2>/dev/null || true
    echo "  ✓ Container: ${CONTAINER}"
done

# ─── 3. Event Hubs ───────────────────────────────────────────────────────────
echo "─── [3/5] Creating Event Hubs Namespace ───"
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
az eventhubs eventhub create \
    --name "trades" \
    --namespace-name "$EVENTHUB_NS" \
    --resource-group "$RG_NAME" \
    --partition-count 4 \
    --output none
echo "  ✓ Event Hub: trades (4 partitions)"

az eventhubs eventhub create \
    --name "news" \
    --namespace-name "$EVENTHUB_NS" \
    --resource-group "$RG_NAME" \
    --partition-count 2 \
    --output none
echo "  ✓ Event Hub: news (2 partitions)"

# Get connection string
EVENTHUB_CONN=$(az eventhubs namespace authorization-rule keys list \
    --resource-group "$RG_NAME" \
    --namespace-name "$EVENTHUB_NS" \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString -o tsv)

# ─── 4. Key Vault ────────────────────────────────────────────────────────────
echo "─── [4/5] Creating Key Vault ───"
az keyvault create \
    --name "$KEYVAULT_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --retention-days 7 \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none
echo "✓ Key Vault: ${KEYVAULT_NAME}"

# Store Event Hub connection string in Key Vault
az keyvault secret set \
    --vault-name "$KEYVAULT_NAME" \
    --name "eventhub-connection-string" \
    --value "$EVENTHUB_CONN" \
    --output none
echo "  ✓ Secret stored: eventhub-connection-string"

# Store storage key
az keyvault secret set \
    --vault-name "$KEYVAULT_NAME" \
    --name "storage-account-key" \
    --value "$STORAGE_KEY" \
    --output none
echo "  ✓ Secret stored: storage-account-key"

# ─── 5. Azure Databricks Workspace ───────────────────────────────────────────
echo "─── [5/5] Creating Databricks Workspace ───"
echo "  (this may take 3-5 minutes...)"
az databricks workspace create \
    --name "$DATABRICKS_NAME" \
    --resource-group "$RG_NAME" \
    --location "$LOCATION" \
    --sku premium \
    --tags project=CryptoPulse environment="${ENV}" \
    --output none
echo "✓ Databricks Workspace: ${DATABRICKS_NAME}"

# Get workspace URL
DATABRICKS_URL=$(az databricks workspace show \
    --name "$DATABRICKS_NAME" \
    --resource-group "$RG_NAME" \
    --query workspaceUrl -o tsv)

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║         Infrastructure Setup Complete!           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Resources created:"
echo "  • Resource Group:    ${RG_NAME}"
echo "  • Storage (ADLS):    ${STORAGE_NAME}"
echo "  • Event Hubs:        ${EVENTHUB_NS}"
echo "  • Key Vault:         ${KEYVAULT_NAME}"
echo "  • Databricks:        https://${DATABRICKS_URL}"
echo ""
echo "─── Next Steps ───"
echo ""
echo "1. Open Databricks workspace and generate a PAT token:"
echo "   https://${DATABRICKS_URL}/#setting/account"
echo ""
echo "2. Configure Databricks CLI:"
echo "   pip install databricks-cli"
echo "   databricks configure --token"
echo "   Host: https://${DATABRICKS_URL}"
echo "   Token: <paste your PAT token>"
echo ""
echo "3. Set secrets in your .env file:"
echo "   AZURE_STORAGE_ACCOUNT_NAME=${STORAGE_NAME}"
echo "   AZURE_STORAGE_ACCOUNT_KEY=${STORAGE_KEY}"
echo "   AZURE_EVENTHUB_CONNECTION_STRING=${EVENTHUB_CONN}"
echo "   DATABRICKS_HOST=https://${DATABRICKS_URL}"
echo ""
echo "4. Run the Databricks setup:"
echo "   bash infrastructure/databricks_setup.sh"
echo ""
