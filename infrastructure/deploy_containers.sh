#!/bin/bash
# =============================================================================
# CryptoPulse - Build & Push Docker Images to ACR
# =============================================================================
# Prerequisites:
#   - azure_setup.sh completed (ACR created)
#   - Docker installed and running
#
# Usage:
#   bash infrastructure/deploy_containers.sh
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV="${CRYPTOPULSE_ENV:-dev}"
PREFIX="cryptopulse"
RG_NAME="rg-${PREFIX}-${ENV}"
ACR_NAME="cr${PREFIX}${ENV}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║      CryptoPulse Docker Image Deployment         ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── Prerequisites ───────────────────────────────────────────────────────────
if ! command -v docker &> /dev/null; then
    echo "✗ Docker not found. Install Docker Desktop."
    exit 1
fi
echo "✓ Docker found"

# Get ACR login server
ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --resource-group "$RG_NAME" \
    --query loginServer -o tsv 2>/dev/null)

if [ -z "$ACR_LOGIN_SERVER" ]; then
    echo "✗ ACR '${ACR_NAME}' not found. Run azure_setup.sh first."
    exit 1
fi
echo "✓ ACR: ${ACR_LOGIN_SERVER}"

# ─── Login to ACR ────────────────────────────────────────────────────────────
echo ""
echo "─── [1/5] Logging in to ACR ───"
az acr login --name "$ACR_NAME"
echo "✓ Logged in to ACR"

# ─── Build & Push API Image ──────────────────────────────────────────────────
echo ""
echo "─── [2/5] Building API image ───"
cd "$PROJECT_ROOT"

API_IMAGE="${ACR_LOGIN_SERVER}/cryptopulse-api"
docker build \
    -t "${API_IMAGE}:latest" \
    -t "${API_IMAGE}:$(git rev-parse --short HEAD 2>/dev/null || echo 'dev')" \
    -f Dockerfile \
    .
echo "✓ Built: ${API_IMAGE}:latest"

echo ""
echo "─── [3/5] Pushing API image ───"
docker push "${API_IMAGE}:latest"
docker push "${API_IMAGE}:$(git rev-parse --short HEAD 2>/dev/null || echo 'dev')" 2>/dev/null || true
echo "✓ Pushed: ${API_IMAGE}:latest"

# ─── Build & Push Frontend Image ─────────────────────────────────────────────
echo ""
echo "─── [4/5] Building Frontend image ───"

FRONTEND_IMAGE="${ACR_LOGIN_SERVER}/cryptopulse-frontend"
docker build \
    -t "${FRONTEND_IMAGE}:latest" \
    -t "${FRONTEND_IMAGE}:$(git rev-parse --short HEAD 2>/dev/null || echo 'dev')" \
    -f Dockerfile.frontend \
    .
echo "✓ Built: ${FRONTEND_IMAGE}:latest"

echo ""
echo "─── [5/5] Pushing Frontend image ───"
docker push "${FRONTEND_IMAGE}:latest"
docker push "${FRONTEND_IMAGE}:$(git rev-parse --short HEAD 2>/dev/null || echo 'dev')" 2>/dev/null || true
echo "✓ Pushed: ${FRONTEND_IMAGE}:latest"

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║         Docker Images Pushed to ACR!             ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Images:"
echo "  • API:      ${API_IMAGE}:latest"
echo "  • Frontend: ${FRONTEND_IMAGE}:latest"
echo ""
echo "Next: bash infrastructure/deploy_apps.sh"
echo ""
