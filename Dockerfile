# =============================================================================
# CryptoPulse API - Multi-Stage Dockerfile
# =============================================================================
# Stage 1: Build dependencies
# Stage 2: Production runtime
#
# Usage:
#   docker build -t cryptopulse-api .
#   docker run -p 8000:8000 --env-file .env cryptopulse-api
# =============================================================================

# ── Stage 1: Builder ────────────────────────────────────────────────────────
FROM python:3.11-slim as builder

WORKDIR /build

# System dependencies for building wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir --prefix=/install -e "." 2>/dev/null || \
    pip install --no-cache-dir --prefix=/install \
    pydantic pydantic-settings python-dotenv structlog httpx aiohttp \
    fastapi "uvicorn[standard]" python-multipart \
    langgraph langchain-core langchain-google-genai \
    redis tenacity numpy pandas scikit-learn


# ── Stage 2: Production ────────────────────────────────────────────────────
FROM python:3.11-slim as production

LABEL maintainer="CryptoPulse Team"
LABEL description="CryptoPulse API - Real-time crypto market intelligence"

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY cryptopulse/ ./cryptopulse/
COPY agents/ ./agents/
COPY api/ ./api/
COPY functions/ ./functions/
COPY pipelines/ ./pipelines/
COPY models/ ./models/

# Create non-root user
RUN groupadd -r cryptopulse && \
    useradd -r -g cryptopulse -d /app -s /sbin/nologin cryptopulse && \
    chown -R cryptopulse:cryptopulse /app

USER cryptopulse

# Environment
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    CRYPTOPULSE_ENV=production \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    API_RELOAD=false \
    API_WORKERS=4

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; r = httpx.get('http://localhost:8000/health'); r.raise_for_status()" || exit 1

CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
