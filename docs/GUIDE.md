# CryptoPulse — Complete User Guide

> **Comprehensive step-by-step guide** for running, deploying, and using CryptoPulse — both locally and on Azure Cloud.

---

## Table of Contents

1. [Prerequisites & Environment Setup](#1-prerequisites--environment-setup)
2. [Local Development (venv, manual)](#2-local-development-venv-manual)
3. [Docker Compose (full local stack)](#3-docker-compose-full-local-stack)
4. [Azure Cloud Deployment](#4-azure-cloud-deployment)
5. [Running Spark Pipelines](#5-running-spark-pipelines)
6. [ML Training & Inference](#6-ml-training--inference)
7. [Frontend Dashboard Usage](#7-frontend-dashboard-usage)
8. [API Reference](#8-api-reference)
9. [Databricks Notebooks](#9-databricks-notebooks)
10. [Monitoring & Troubleshooting](#10-monitoring--troubleshooting)

---

## 1. Prerequisites & Environment Setup

### System Requirements

| Tool | Version | Check Command |
|---|---|---|
| Python | ≥ 3.10 | `python3 --version` |
| Docker & Docker Compose | Latest | `docker --version && docker compose version` |
| Azure CLI | Latest | `az --version` |
| Databricks CLI | Latest | `databricks --version` |
| Git | Any | `git --version` |

### API Keys You'll Need

| Key | Where to Get It | Required? |
|---|---|---|
| **Groq API Key** | [console.groq.com/keys](https://console.groq.com/keys) | ✅ Yes — powers the LangGraph multi-agent system |
| **CryptoCompare API Key** | [cryptocompare.com/api-keys](https://www.cryptocompare.com/cryptopian/api-keys) | Optional — for premium news data |
| **CryptoPanic API Key** | [cryptopanic.com/developers/api](https://cryptopanic.com/developers/api/) | Optional — additional news source |
| **Binance API Key** | [binance.com/en/my/settings/api-management](https://www.binance.com/en/my/settings/api-management) | Optional — public endpoints work without a key |

### Clone & Configure .env

```bash
# Clone the repository
git clone <your-repo-url>
cd CryptoPulse

# Create .env from template
cp .env.example .env

# Edit .env — at minimum set your Groq key:
# GROQ_API_KEY=gsk_xxxxxxxxxxxx
```

> [!IMPORTANT]
> The `.env` file is gitignored. **Never commit API keys.** The `.env.example` file shows all available config options.

### .env File Sections Explained

| Section | Purpose |
|---|---|
| **Application Settings** | `CRYPTOPULSE_ENV`, `DEBUG`, `LOG_LEVEL` |
| **Azure Configuration** | Event Hubs, ADLS Gen2, Databricks, Key Vault connection strings |
| **Local Development** | Kafka, Spark, MinIO, Redis settings (used by Docker Compose) |
| **Binance Configuration** | WebSocket URL, REST URL, trading pairs (20 default pairs), rate limiting |
| **News API Configuration** | CoinDesk, CryptoCompare, CryptoPanic URLs and keys |
| **ML Configuration** | MLflow URI, model paths, prediction horizon/confidence thresholds |
| **API Configuration** | Host, port, workers, API keys, rate limiting |
| **Alerting** | Webhook URLs, email/SMTP settings |

---

## 2. Local Development (venv, manual)

### Step 1: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate    # On Mac/Linux
# venv\Scripts\activate     # On Windows
```

### Step 2: Install Dependencies

```bash
pip install -e ".[dev]"
```

This installs all packages defined in `pyproject.toml`:
- **Core**: pydantic, structlog, httpx, aiohttp, websockets
- **Streaming**: confluent-kafka, delta-spark, pyspark
- **ML**: scikit-learn, lightgbm, numpy, pandas
- **AI Agents**: langgraph, langchain-core, langchain-groq
- **API**: FastAPI, uvicorn
- **Frontend**: Flask, flask-cors
- **Dev**: pytest, ruff, mypy, black

### Step 3: Start the FastAPI Backend

```bash
# Using Makefile shortcut:
make dev

# Or manually:
python -m api.main
```

**FastAPI starts at:** `http://localhost:8000`
**Swagger docs at:** `http://localhost:8000/docs`

### Step 4: Start the Flask Frontend

```bash
python frontend/app.py
```

**Flask dashboard at:** `http://localhost:5050`

### Step 5: Run Tests

```bash
# All tests
make test
# Or: pytest tests/ -v --tb=short

# With coverage
make test-cov
# Or: pytest tests/ -v --cov=cryptopulse --cov=agents --cov=api --cov-report=term-missing
```

### Step 6: Run Ingestion (Manual)

```bash
# Binance trade ingestion
make ingest-trades
# Or: python -m functions.binance_ingestion

# News crawl ingestion
make ingest-news
# Or: python -m functions.news_ingestion
```

---

## 3. Docker Compose (Full Local Stack)

Docker Compose simulates the full Azure infrastructure locally using open-source alternatives.

### Architecture Mapping

| Azure Service | Docker Equivalent | Container Name | Port |
|---|---|---|---|
| Azure Event Hubs | Apache Kafka | `cryptopulse-kafka` | 9092 |
| ADLS Gen2 / Delta Lake | MinIO (S3-compatible) | `cryptopulse-minio` | 9000, 9001 |
| Azure Databricks | Apache Spark (Master + Worker) | `cryptopulse-spark-master/worker` | 7077, 8081 |
| Azure Cache for Redis | Redis | `cryptopulse-redis` | 6379 |
| Azure ML (tracking) | MLflow | `cryptopulse-mlflow` | 5000 |
| Azure Functions (ingestion) | Python ingestion loop | `cryptopulse-ingestion` | — |
| Azure App Service (API) | FastAPI | `cryptopulse-api` | 8000 |
| Azure App Service (Frontend) | Flask | `cryptopulse-frontend` | 5050 |
| — | Kafka UI (monitoring) | `cryptopulse-kafka-ui` | 8080 |
| — | Zookeeper (Kafka dependency) | `cryptopulse-zookeeper` | 2181 |

### Start Everything

```bash
# Start all 11 services
docker compose up -d

# Or using Makefile:
make docker-up
```

### Verify Services Are Running

```bash
docker compose ps
```

You should see all containers in `running` or `healthy` state:

```
NAME                          STATUS
cryptopulse-zookeeper         running (healthy)
cryptopulse-kafka             running (healthy)
cryptopulse-kafka-ui          running
cryptopulse-minio             running (healthy)
cryptopulse-minio-setup       exited (0)      ← expected, it's a one-shot init
cryptopulse-spark-master      running
cryptopulse-spark-worker      running
cryptopulse-redis             running (healthy)
cryptopulse-mlflow            running
cryptopulse-api               running (healthy)
cryptopulse-ingestion         running
cryptopulse-frontend          running (healthy)
```

### Service URLs (Docker)

| Service | URL | What to Check |
|---|---|---|
| **Flask Dashboard** | http://localhost:5050 | Live prices, charts, news, agent analysis |
| **FastAPI Docs** | http://localhost:8000/docs | Swagger UI with all endpoints |
| **FastAPI Health** | http://localhost:8000/health | JSON with service statuses |
| **Kafka UI** | http://localhost:8080 | Topics: `trades`, `news`, `features`, `predictions` |
| **MinIO Console** | http://localhost:9001 | Buckets: `cryptopulse-bronze/silver/gold/checkpoints/mlflow` |
| **Spark Master UI** | http://localhost:8081 | Workers, running jobs, executor status |
| **MLflow** | http://localhost:5000 | Experiments, model registry, run tracking |

> [!TIP]
> **MinIO Console login:** Username `minioadmin`, Password `minioadmin`. You should see 5 buckets created automatically by the `minio-setup` container.

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f api
docker compose logs -f ingestion
docker compose logs -f kafka

# Or using Makefile:
make docker-logs
```

### Stop Everything

```bash
docker compose down       # Stop containers (keep data)
docker compose down -v    # Stop and remove all volumes (clean slate)

# Or using Makefile:
make docker-down
```

---

## 4. Azure Cloud Deployment

### Step 1: Login to Azure

```bash
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID>"
```

### Step 2: Provision Infrastructure

The `infrastructure/azure_setup.sh` script creates all required Azure resources:

```bash
# Set environment
export CRYPTOPULSE_ENV=dev     # or staging / prod

# Run the provisioner
bash infrastructure/azure_setup.sh
```

**What it creates (5 resources):**

| Step | Resource | Azure Service | Purpose |
|---|---|---|---|
| 1/5 | Resource Group | `rg-cryptopulse-dev` | Container for all resources |
| 2/5 | Storage Account | ADLS Gen2 | Delta Lake (Bronze/Silver/Gold tables) |
| 3/5 | Event Hubs Namespace | 4 Event Hubs | Real-time streaming (trades, news, features, predictions) |
| 4/5 | Key Vault | Secrets | Store connection strings, API keys |
| 5/5 | Databricks Workspace | Spark compute | Pipeline execution, ML training |

**After the script completes, it prints connection strings.** Copy them into your `.env`:

```bash
# The script will output something like:
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
DATABRICKS_HOST=https://eastus.azuredatabricks.net
```

### Step 2b: Python Provisioner (Alternative)

There's also a Python-based provisioner with dry-run support:

```bash
# Dry run (preview only):
python infrastructure/azure_provisioner.py --env dev --dry-run

# Actual provisioning:
python infrastructure/azure_provisioner.py --env dev

# Generate .env from provisioned resources:
python infrastructure/azure_provisioner.py --env dev --generate-env
```

### Step 3: Configure Databricks

```bash
# Install and configure Databricks CLI
pip install databricks-cli
databricks configure --token
# Enter:
#   Host: https://<region>.azuredatabricks.net
#   Token: <personal access token from Databricks UI>

# Run the Databricks setup
bash infrastructure/databricks_setup.sh
```

**What this does:**
1. Creates DBFS directories: `/cryptopulse/checkpoints`, `/cryptopulse/models`, `/cryptopulse/libs`
2. Builds the CryptoPulse wheel and uploads it to DBFS
3. Imports all 10 notebooks into `/CryptoPulse/notebooks/`
4. Creates a compute cluster `cryptopulse-dev` with Delta Lake and Spark 3.5

### Step 4: Quick Deploy (Re-deploy Code Changes)

```bash
# Rebuild wheel + re-import notebooks only:
bash infrastructure/deploy_to_databricks.sh

# Or via Makefile:
make deploy
```

### Where to Navigate on Azure Portal

After provisioning, navigate to [portal.azure.com](https://portal.azure.com) and check:

| Azure Service | What to Look For |
|---|---|
| **Resource Group** (`rg-cryptopulse-dev`) | All 5+ resources should exist |
| **Event Hubs** → Overview | 4 Event Hubs: `trades`, `news`, `features`, `predictions` |
| **Event Hubs** → Process Data | Incoming/outgoing message metrics |
| **Storage Account** → Containers | `cryptopulse-delta` container with `bronze/`, `silver/`, `gold/` folders |
| **Storage Account** → Monitoring | Read/write throughput, transaction counts |
| **Key Vault** → Secrets | `eventhub-connection-string` should be stored |
| **Databricks** → Workspace | `/CryptoPulse/notebooks/` with 10 notebooks |
| **Databricks** → Compute | `cryptopulse-dev` cluster |
| **Databricks** → Clusters → Spark UI | Job progress, DAG visualization, storage metrics |

> [!IMPORTANT]
> **Databricks Personal Access Token**: Go to Databricks → User Settings → Developer → Access Tokens → Generate New Token. Copy it when prompted by `databricks configure --token`.

---

## 5. Running Spark Pipelines

### Pipeline Architecture (Medallion / Lakehouse)

```
Binance WebSocket   ──→  Kafka/Event Hubs  ──→  Bronze (raw)
News Crawlers       ──→  Kafka/Event Hubs  ──→  Bronze (raw)
                                                    │
                                                    ▼
                                            Silver (cleaned)
                                                    │
                                                    ▼
                                            Gold (features, sentiment)
                                                    │
                                                    ▼
                                            ML Models (GBT, IF)
```

### Pipeline Files

| Layer | Pipeline | File | What It Does |
|---|---|---|---|
| **Bronze** | Trades | `pipelines/bronze/trades_bronze.py` | Kafka → Delta: raw trade events with minimal transformation |
| **Bronze** | News | `pipelines/bronze/news_bronze.py` | Kafka → Delta: raw news articles with metadata |
| **Silver** | Trades | `pipelines/silver/trades_silver.py` | Dedup, validation, type casting, outlier filtering |
| **Silver** | News | `pipelines/silver/news_silver.py` | Text cleaning, language filtering, schema normalization |
| **Gold** | Features | `pipelines/gold/feature_engineering.py` | OHLC (1m/5m/15m), VWAP, volatility, trade intensity, buy/sell ratio |
| **Gold** | Sentiment | `pipelines/gold/sentiment_analysis.py` | FinBERT NLP, per-symbol aggregation, confidence scoring |
| **Utils** | Windowing | `pipelines/utils/windowing.py` | Reusable Spark window functions for aggregation |

### Running Pipelines Locally (via Docker Compose)

1. Make sure Docker Compose is running (`docker compose up -d`)
2. Open the Spark Master UI at `http://localhost:8081`
3. Submit a pipeline job:

```bash
# From the spark-master container:
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/bitnami/spark/pipelines/bronze/trades_bronze.py
```

### Running Pipelines on Databricks

1. Open Databricks Workspace → `/CryptoPulse/notebooks/`
2. Attach the `cryptopulse-dev` cluster
3. Run notebooks **in order**: `01_data_ingestion` → `02_bronze_layer` → ... → `10_langgraph_agents`

### Backfill Mode (Batch Reprocessing)

Each pipeline supports both streaming and batch:

```python
from pipelines.bronze.trades_bronze import backfill_trades
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
backfill_trades(
    spark=spark,
    source_path="/data/raw/trades/",
    target_path="/data/delta/bronze/trades",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

---

## 6. ML Training & Inference

### Models Overview

| Model | Algorithm | File | Purpose |
|---|---|---|---|
| **Price Direction** | Gradient Boosted Trees (LightGBM) | `models/training/price_direction.py` | Predicts UP / DOWN / NEUTRAL for the next N minutes |
| **Anomaly Detection** | Isolation Forest + Z-Score | `models/training/anomaly_detection.py` | Detects pump/dump, wash trading, volume spikes, price manipulation |

### Training the Price Direction Model

```python
from models.training.price_direction import PriceDirectionModel, ModelConfig, train_from_delta

# Option 1: Train from Delta Lake features (production)
model = train_from_delta(
    spark=spark,
    features_path="/data/delta/gold/features",
    sentiment_path="/data/delta/gold/sentiment",
    model_output_path="models/artifacts/price_direction",
)

# Option 2: Train with custom config
config = ModelConfig(
    prediction_horizon_minutes=5,   # Predict 5 minutes ahead
    n_estimators=200,               # Number of GBT trees
    max_depth=6,
    learning_rate=0.05,
    test_size=0.2,
)
model = PriceDirectionModel(config)
metrics = model.fit(X_train, y_train, validation_data=(X_val, y_val))
model.save("models/artifacts/price_direction")
```

**Features used (15+):**
- OHLC-derived: `price_change_pct`, `high_low_range`
- Volume: `volume`, `quote_volume`, `buy_sell_ratio`, `trade_intensity`
- VWAP: `vwap_deviation`
- Momentum: `price_change_pct_lag1/lag2/lag3`, `volume_change_pct`, `volatility_change`
- Sentiment: `sentiment_score`, `sentiment_confidence`, `article_count`, `sentiment_momentum`

### Training the Anomaly Detection Model

```python
from models.training.anomaly_detection import AnomalyDetectionModel, AnomalyConfig

config = AnomalyConfig(
    contamination=0.01,    # Expected anomaly fraction
    n_estimators=100,
    zscore_threshold=3.0,
)
model = AnomalyDetectionModel(config)
model.fit(X_train)

# Detect and classify anomalies
results = model.detect_and_classify(df, feature_cols=["volume_spike", "price_velocity", ...])

# Results include:
# is_anomaly, anomaly_severity, anomaly_type (pump/dump/wash_trade/volume_spike), anomaly_description
```

### Making Single Predictions (API)

```bash
# Via FastAPI
curl http://localhost:8000/predict/BTCUSDT?horizon_minutes=5

# Via multi-agent analysis (full pipeline)
curl -X POST http://localhost:8000/analyze/BTCUSDT?horizon_minutes=5
```

### MLflow Tracking

- **Local:** http://localhost:5000 (via Docker Compose)
- **Azure:** Configured via `MLFLOW_TRACKING_URI` in `.env`

MLflow stores:
- Experiment runs with hyperparameters
- Training metrics (accuracy, F1, precision, recall)
- Model artifacts (serialized models)
- Feature importance rankings

---

## 7. Frontend Dashboard Usage

The Flask dashboard is a presentation-ready interface at **http://localhost:5050**.

### Pages & What to Look For

#### 📊 Dashboard (`/`)

**What you see:**
- **Live Price Cards** — Real-time BTC, ETH, BNB, SOL, XRP, ADA, DOGE, AVAX prices from Binance
- **24h Performance Chart** — Line chart showing price changes over the last 24 hours
- **Market Stats** — Total market volume, top gainer/loser, average change
- **Latest News** — Recent crypto headlines with sentiment badges

**What to verify:**
- Prices update every 30 seconds (auto-refresh)
- Price change percentages show green (▲) for positive, red (▼) for negative
- Chart has smooth animations on load

#### 📰 Live News (`/news`)

**What you see:**
- **30 Latest Articles** — From 6+ sources (CoinDesk, CoinTelegraph, CryptoCompare, etc.)
- **Sentiment Tags** — Each article tagged as `positive` (green), `negative` (red), or `neutral` (gray)
- **Source & Timestamp** — Publisher name and publication time
- **SSE Streaming** — New articles appear in real-time via Server-Sent Events (top banner)

**What to verify:**
- Articles load on page open (30 articles)
- Sentiment colors match content sentiment
- SSE banner shows new articles every ~30 seconds

#### 🤖 Agent Analysis (`/analyze` or `/analyze/BTCUSDT`)

**What you see:**
- **Trading Pair Selector** — Dropdown to choose any coin (BTC, ETH, BNB, SOL, XRP, ADA, DOGE, AVAX)
- **Run Analysis Button** — Triggers the 4-agent LangGraph pipeline
- **Agent Pipeline Visualization** — Shows: MarketAnalyst → SentimentAnalyst → RiskManager → PortfolioStrategist
- **Results** — Trading recommendation, confidence score, agent trace

**What to verify:**
- Select different trading pairs from the dropdown
- Click "Run Analysis" and verify the 4 agents execute in sequence
- Each agent card shows its individual analysis
- **Requires**: `GROQ_API_KEY` in `.env` and FastAPI running at port 8000

#### 🧠 ML Pipeline (`/ml`)

**What you see:**
- **Medallion Architecture Diagram** — Bronze → Silver → Gold visual flow
- **Spark MLlib Code Viewer** — Real code from `price_direction.py` and `anomaly_detection.py`
- **Feature Importance Chart** — Top 15 features ranked by importance
- **Tech Stack Table** — All technologies with descriptions and usage

**What to verify:**
- Architecture diagram shows 3 layers with correct arrow flow
- Code blocks have syntax highlighting
- Feature importance chart renders with bar widths

### Screenshots to Add

Here are the recommended screenshots for your README/portfolio:

| # | Screenshot | Page | What to Capture |
|---|---|---|---|
| 1 | `screenshots/dashboard.png` | Dashboard `/` | Full page showing live prices + chart + news |
| 2 | `screenshots/news.png` | News `/news` | Article cards with sentiment badges visible |
| 3 | `screenshots/agent-analysis.png` | Analyze `/analyze/BTCUSDT` | 4-agent pipeline with results |
| 4 | `screenshots/ml-pipeline.png` | ML `/ml` | Medallion architecture diagram |
| 5 | `screenshots/fastapi-docs.png` | FastAPI `localhost:8000/docs` | Swagger UI with all endpoints |
| 6 | `screenshots/kafka-ui.png` | Kafka UI `localhost:8080` | Topics with message counts |
| 7 | `screenshots/minio-console.png` | MinIO `localhost:9001` | 5 Delta Lake buckets |
| 8 | `screenshots/spark-ui.png` | Spark `localhost:8081` | Master with worker + running job |
| 9 | `screenshots/mlflow.png` | MLflow `localhost:5000` | Experiment runs with metrics |

---

## 8. API Reference

### Base URL

- **Local:** `http://localhost:8000`
- **Docker:** `http://localhost:8000` (exposed port)
- **Swagger UI:** `http://localhost:8000/docs`

### Endpoints

#### Health & Status

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | API root — returns version info |
| `GET` | `/health` | Health check — returns service statuses for prediction, features, anomaly services |

#### Predictions

| Method | Path | Description |
|---|---|---|
| `GET` | `/predict/{symbol}` | Get price direction prediction (UP/DOWN/NEUTRAL) |
| `GET` | `/predict/all` | Get predictions for multiple symbols |

**Parameters:**
- `symbol` — Trading pair, e.g., `BTCUSDT`
- `horizon_minutes` — Prediction horizon, 1–15 (default: 5)
- `include_features` — Include current market features (default: false)
- `include_sentiment` — Include sentiment data (default: false)

**Example:**
```bash
curl "http://localhost:8000/predict/BTCUSDT?horizon_minutes=5&include_features=true"
```

**Response:**
```json
{
    "symbol": "BTCUSDT",
    "direction": "UP",
    "confidence": 0.73,
    "probabilities": {"UP": 0.73, "DOWN": 0.15, "NEUTRAL": 0.12},
    "features": { ... },
    "timestamp": "2026-02-12T10:00:00"
}
```

#### Features

| Method | Path | Description |
|---|---|---|
| `GET` | `/features/{symbol}` | Get current market features (OHLC, VWAP, volatility) |
| `GET` | `/features/{symbol}/history` | Get historical features |

**Parameters:**
- `interval` — `1m`, `5m`, or `15m` (default: `1m`)
- `limit` — Number of data points, 1–1000 (default: 60)

#### Anomalies

| Method | Path | Description |
|---|---|---|
| `GET` | `/anomalies` | Get recent anomaly alerts (all symbols) |
| `GET` | `/anomalies/{symbol}` | Get anomalies for a specific symbol |

**Parameters:**
- `severity_min` — Minimum severity, 0–1 (default: 0.5)
- `minutes_back` — Lookback period, 1–1440 (default: 60)
- `limit` — Max alerts, 1–200 (default: 50)

#### Market Overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/market/overview` | Overall market summary: top movers, alerts, aggregated stats |

#### Agent Analysis

| Method | Path | Description |
|---|---|---|
| `POST` | `/analyze/{symbol}` | **Full multi-agent LangGraph analysis** |

**Parameters:**
- `horizon_minutes` — 1–60 (default: 5)

**Response:**
```json
{
    "symbol": "BTCUSDT",
    "recommendation": "BUY with moderate confidence...",
    "confidence": 0.68,
    "prediction": { "direction": "UP", "probability": 0.73 },
    "agent_trace": [
        "MarketAnalyst: Bullish divergence detected...",
        "SentimentAnalyst: Positive sentiment (0.72)...",
        "RiskManager: Low anomaly risk...",
        "PortfolioStrategist: BUY signal..."
    ],
    "latency_ms": 2340.5,
    "timestamp": "2026-02-12T10:00:00"
}
```

---

## 9. Databricks Notebooks

Run these on your Databricks cluster **in order**:

| # | Notebook | Purpose | Dependencies |
|---|---|---|---|
| 01 | `01_data_ingestion` | Configure Event Hubs, ingest live Binance data | Event Hubs connection string |
| 02 | `02_bronze_layer` | Raw → Bronze Delta tables (trades + news) | Notebook 01 |
| 03 | `03_silver_layer` | Bronze → Silver: cleaning, dedup, validation | Notebook 02 |
| 04 | `04_gold_features` | Silver → Gold: OHLC, VWAP, volatility, indicators | Notebook 03 |
| 05 | `05_sentiment_analysis` | Silver news → Gold: FinBERT NLP scoring | Notebook 03 |
| 06 | `06_ml_training` | Train price direction GBT model on Gold features | Notebooks 04 + 05 |
| 07 | `07_anomaly_detection` | Train Isolation Forest on Gold trade features | Notebook 04 |
| 08 | `08_model_serving` | Register models in MLflow, load for inference | Notebooks 06 + 07 |
| 09 | `09_orchestration` | End-to-end pipeline orchestration and scheduling | All above |
| 10 | `10_langgraph_agents` | Multi-agent experimentation in Databricks | Groq API key |

### How to Run

1. Open Databricks Workspace → Navigate to `/CryptoPulse/notebooks/`
2. Click on `01_data_ingestion`
3. In the top-right, click **Attach** → Choose `cryptopulse-dev` cluster
4. Click **Run All** to execute all cells
5. Check the output of each cell for success/error messages
6. Move to the next notebook in sequence

> [!TIP]
> Each notebook is **idempotent** — you can re-run it safely. Streaming jobs run until cancelled or terminated.

---

## 10. Monitoring & Troubleshooting

### Health Check Dashboard

```bash
# Quick health check
curl http://localhost:8000/health | python -m json.tool
```

Expected output:
```json
{
    "status": "healthy",
    "services": {
        "prediction_service": true,
        "feature_service": true,
        "anomaly_service": true
    },
    "timestamp": "2026-02-12T10:00:00"
}
```

### Common Issues & Fixes

#### Docker Issues

| Problem | Cause | Fix |
|---|---|---|
| Kafka not starting | Zookeeper not ready | Wait 30s, or `docker compose restart kafka` |
| MinIO setup failed | MinIO not healthy yet | `docker compose restart minio-setup` |
| API health check fails | Dependencies not ready | Wait for Kafka + Redis to be healthy first |
| Port 5000 in use (macOS) | AirPlay Receiver on macOS | System Preferences → General → AirDrop & Handoff → Disable AirPlay Receiver |
| Port 8080 in use | Another service on port | Change port in docker-compose.yml or stop conflicting service |

#### Python / venv Issues

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: cryptopulse` | Run `pip install -e .` from project root |
| `GROQ_API_KEY not set` | Add `GROQ_API_KEY=gsk_xxx` to `.env` file |
| `pyspark version conflict` | Ensure Python 3.10–3.12 (PySpark doesn't support 3.13) |
| Import errors in agents | Ensure `PYTHONPATH` includes project root: `export PYTHONPATH=$(pwd)` |

#### Azure Issues

| Problem | Fix |
|---|---|
| `az login` expired | Run `az login` again |
| Databricks cluster won't start | Check subscription quota in Azure Portal → Quotas |
| Event Hubs 401 | Regenerate SharedAccessKey in Azure Portal |
| ADLS 403 Forbidden | Check Storage Account access keys / RBAC permissions |

### Makefile Commands Reference

```bash
make help          # Show all available commands
make install       # Install all dependencies
make dev           # Start FastAPI dev server
make dev-docker    # Start full Docker stack
make test          # Run all tests
make test-cov      # Tests with coverage report
make lint          # Run ruff + mypy linting
make format        # Format code with black + ruff
make docker-build  # Build API Docker image
make docker-up     # Start Docker services
make docker-down   # Stop Docker and remove volumes
make docker-logs   # Tail API container logs
make deploy        # Deploy to Databricks (notebooks + wheel)
make deploy-infra  # Provision Azure infrastructure
make deploy-dbx    # Configure Databricks workspace
make ingest-trades # Run Binance ingestion locally
make ingest-news   # Run news crawl locally
make clean         # Remove build artifacts, caches
```

---

## Quick Start Cheatsheet

### For a Demo (Fastest)

```bash
# 1. Clone + configure
cp .env.example .env
# Edit .env with GROQ_API_KEY

# 2. Start frontend only (no Docker needed)
python3 -m venv venv && source venv/bin/activate
pip install flask flask-cors httpx python-dotenv
python frontend/app.py

# 3. Open browser
open http://localhost:5050
```

### For Development

```bash
# 1. Full install
make install

# 2. Start API + Frontend
make dev &              # FastAPI on :8000
python frontend/app.py  # Flask on :5050
```

### For Production-like Testing

```bash
# 1. Everything in Docker
docker compose up -d

# 2. Verify all services
docker compose ps
curl http://localhost:8000/health
open http://localhost:5050
```

### For Azure Deployment

```bash
# 1. Provision infrastructure
make deploy-infra

# 2. Configure Databricks
make deploy-dbx

# 3. Deploy code
make deploy

# 4. Run notebooks 01–10 in Databricks
```
