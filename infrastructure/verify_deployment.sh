#!/bin/bash
# =============================================================================
# CryptoPulse - Deployment Verification
# =============================================================================
# End-to-end smoke test of all deployed Azure services.
#
# Usage:
#   bash infrastructure/verify_deployment.sh
# =============================================================================

set -euo pipefail

ENV="${CRYPTOPULSE_ENV:-dev}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"

PASS=0
FAIL=0
WARN=0

check() {
    local NAME="$1"
    local CMD="$2"
    local RESULT
    RESULT=$(eval "$CMD" 2>/dev/null || echo "FAILED")
    if [ "$RESULT" != "FAILED" ] && [ -n "$RESULT" ]; then
        echo "  ✅ ${NAME}: ${RESULT}"
        ((PASS++))
    else
        echo "  ❌ ${NAME}: FAILED"
        ((FAIL++))
    fi
}

warn() {
    local NAME="$1"
    local MSG="$2"
    echo "  ⚠️  ${NAME}: ${MSG}"
    ((WARN++))
}

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║      CryptoPulse Deployment Verification         ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── 1. Resource Group ───────────────────────────────────────────────────────
echo "─── [1/8] Resource Group ───"
check "Resource Group" "az group show --name '$RG_NAME' --query name -o tsv"

# ─── 2. Storage Account (ADLS Gen2) ──────────────────────────────────────────
echo ""
echo "─── [2/8] ADLS Gen2 Storage ───"
STORAGE_NAME="st${PREFIX}${ENV}"
check "Storage Account" "az storage account show --name '$STORAGE_NAME' --query name -o tsv"
check "HNS Enabled" "az storage account show --name '$STORAGE_NAME' --query isHnsEnabled -o tsv"
check "Delta Container" "az storage container show --name cryptopulse-delta --account-name '$STORAGE_NAME' --query name -o tsv"

# ─── 3. Event Hubs ───────────────────────────────────────────────────────────
echo ""
echo "─── [3/8] Event Hubs ───"
EVENTHUB_NS="evhns-${PREFIX}-${ENV}"
check "Namespace" "az eventhubs namespace show --name '$EVENTHUB_NS' --resource-group '$RG_NAME' --query name -o tsv"
for EH in trades news features predictions; do
    check "Event Hub: $EH" "az eventhubs eventhub show --name '$EH' --namespace-name '$EVENTHUB_NS' --resource-group '$RG_NAME' --query name -o tsv"
done

# ─── 4. Key Vault ────────────────────────────────────────────────────────────
echo ""
echo "─── [4/8] Key Vault ───"
KEYVAULT_NAME="kv-${PREFIX}-${ENV}"
check "Key Vault" "az keyvault show --name '$KEYVAULT_NAME' --query name -o tsv"
for SECRET in eventhub-connection-string storage-account-key storage-account-name redis-connection-string; do
    check "Secret: $SECRET" "az keyvault secret show --vault-name '$KEYVAULT_NAME' --name '$SECRET' --query name -o tsv"
done

# ─── 5. Databricks ───────────────────────────────────────────────────────────
echo ""
echo "─── [5/8] Databricks ───"
DATABRICKS_NAME="dbw-${PREFIX}-${ENV}"
check "Workspace" "az databricks workspace show --name '$DATABRICKS_NAME' --resource-group '$RG_NAME' --query name -o tsv"
DBX_URL=$(az databricks workspace show --name "$DATABRICKS_NAME" --resource-group "$RG_NAME" \
    --query workspaceUrl -o tsv 2>/dev/null || echo "")
[ -n "$DBX_URL" ] && echo "  🔗 URL: https://${DBX_URL}"

# ─── 6. Container Registry ───────────────────────────────────────────────────
echo ""
echo "─── [6/8] Container Registry ───"
ACR_NAME="cr${PREFIX}${ENV}"
check "ACR" "az acr show --name '$ACR_NAME' --query name -o tsv"
ACR_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv 2>/dev/null || echo "")
[ -n "$ACR_SERVER" ] && echo "  🔗 Server: ${ACR_SERVER}"

# Check if images exist
for IMG in cryptopulse-api cryptopulse-frontend; do
    check "Image: $IMG" "az acr repository show --name '$ACR_NAME' --repository '$IMG' --query name -o tsv"
done

# ─── 7. Function App ─────────────────────────────────────────────────────────
echo ""
echo "─── [7/8] Function App ───"
FUNCAPP_NAME="func-${PREFIX}-${ENV}"
check "Function App" "az functionapp show --name '$FUNCAPP_NAME' --resource-group '$RG_NAME' --query state -o tsv"
FUNC_URL="https://${FUNCAPP_NAME}.azurewebsites.net"
echo "  🔗 URL: ${FUNC_URL}"

# ─── 8. Container Apps ───────────────────────────────────────────────────────
echo ""
echo "─── [8/8] Container Apps ───"
CAE_NAME="cae-${PREFIX}-${ENV}"
check "Environment" "az containerapp env show --name '$CAE_NAME' --resource-group '$RG_NAME' --query name -o tsv"

for APP in cryptopulse-api cryptopulse-frontend; do
    check "App: $APP" "az containerapp show --name '$APP' --resource-group '$RG_NAME' --query properties.runningStatus -o tsv"
    APP_URL=$(az containerapp show --name "$APP" --resource-group "$RG_NAME" \
        --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || echo "")
    [ -n "$APP_URL" ] && echo "  🔗 URL: https://${APP_URL}"
done

# ─── HTTP Health Checks ──────────────────────────────────────────────────────
echo ""
echo "─── HTTP Health Checks ───"

API_URL=$(az containerapp show --name "cryptopulse-api" --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || echo "")
if [ -n "$API_URL" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "https://${API_URL}/health" --max-time 10 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
        echo "  ✅ API Health: HTTP ${HTTP_CODE}"
        ((PASS++))
    else
        echo "  ❌ API Health: HTTP ${HTTP_CODE}"
        ((FAIL++))
    fi
fi

FRONTEND_URL=$(az containerapp show --name "cryptopulse-frontend" --resource-group "$RG_NAME" \
    --query properties.configuration.ingress.fqdn -o tsv 2>/dev/null || echo "")
if [ -n "$FRONTEND_URL" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "https://${FRONTEND_URL}/" --max-time 10 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
        echo "  ✅ Frontend: HTTP ${HTTP_CODE}"
        ((PASS++))
    else
        echo "  ❌ Frontend: HTTP ${HTTP_CODE}"
        ((FAIL++))
    fi
fi

# ─── Redis ────────────────────────────────────────────────────────────────────
echo ""
echo "─── Redis Cache ───"
REDIS_NAME="${PREFIX}-redis-${ENV}"
check "Redis" "az redis show --name '$REDIS_NAME' --resource-group '$RG_NAME' --query provisioningState -o tsv"

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║          Verification Summary                    ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║  ✅ Passed:  ${PASS}                              ║"
echo "║  ❌ Failed:  ${FAIL}                              ║"
echo "║  ⚠️  Warnings: ${WARN}                            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "🎉 All checks passed! CryptoPulse is fully deployed on Azure."
else
    echo "⚠ Some checks failed. Review the output above and re-run failed phases."
fi

echo ""
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│                    SERVICE ACCESS MAP                           │"
echo "├──────────────────────────────────────────────────────────────────┤"
echo "│ Service              │ Port  │ URL                              │"
echo "├──────────────────────────────────────────────────────────────────┤"
[ -n "$API_URL" ] &&     echo "│ FastAPI              │ 8000  │ https://${API_URL}"
[ -n "$API_URL" ] &&     echo "│   ├─ Swagger UI      │       │ https://${API_URL}/docs"
[ -n "$API_URL" ] &&     echo "│   └─ Health          │       │ https://${API_URL}/health"
[ -n "$FRONTEND_URL" ] && echo "│ Flask Dashboard      │ 5050  │ https://${FRONTEND_URL}"
                          echo "│ Function App         │  —    │ ${FUNC_URL}"
[ -n "$DBX_URL" ] &&     echo "│ Databricks           │  443  │ https://${DBX_URL}"
                          echo "│ Event Hubs           │ 9093  │ evhns-${PREFIX}-${ENV}.servicebus.windows.net"
                          echo "│ ADLS Gen2            │  443  │ abfss://cryptopulse-delta@st${PREFIX}${ENV}.dfs.core.windows.net"
                          echo "│ Redis Cache          │ 6380  │ ${PREFIX}-redis-${ENV}.redis.cache.windows.net"
                          echo "│ Key Vault            │  443  │ https://kv-${PREFIX}-${ENV}.vault.azure.net"
[ -n "$ACR_SERVER" ] &&  echo "│ Container Registry   │  443  │ ${ACR_SERVER}"
echo "└──────────────────────────────────────────────────────────────────┘"
echo ""
