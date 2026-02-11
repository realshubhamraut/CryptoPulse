# CryptoPulse 🚀

**Real-time cryptocurrency analytics platform powered by Azure Databricks & LangGraph AI agents.**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Azure Databricks                         │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │  Ingestion  │→│  Bronze   │→│  Silver   │→│   Gold     │ │
│  │  (Binance)  │ │  (Delta)  │ │  (Delta)  │ │  Features  │ │
│  └────────────┘  └──────────┘  └──────────┘  └───────────┘ │
│                                                              │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────────┐ │
│  │  FinBERT    │  │  MLlib   │  │  Isolation Forest        │ │
│  │  Sentiment  │  │  GBT     │  │  Anomaly Detection       │ │
│  └────────────┘  └──────────┘  └──────────────────────────┘ │
│                                                              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │  LangGraph Multi-Agent System (Type 5)                   ││
│  │  MarketAnalyst → SentimentAnalyst → RiskManager          ││
│  │                    ↓                                     ││
│  │              PortfolioStrategist → Recommendation         ││
│  └──────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
         │
    ┌────┴────┐
    │ FastAPI │  ← /predict, /features, /anomalies, /analyze
    │   REST  │
    └─────────┘
```

## Quick Start

### 1. Prerequisites

- Azure CLI (`az`) and Databricks CLI (`databricks`)
- Python 3.11+
- Google Gemini API key (for LangGraph agents)

### 2. Azure Infrastructure

```bash
# Login to Azure
az login

# Provision all resources (Resource Group, Storage, Event Hubs, Key Vault, Databricks)
bash infrastructure/azure_setup.sh

# Configure Databricks CLI with workspace URL + token
databricks configure

# Setup workspace (DBFS dirs, wheel upload, notebooks, cluster)
bash infrastructure/databricks_setup.sh
```

### 3. Environment Setup

```bash
cp .env.example .env
# Set GOOGLE_API_KEY, EVENTHUB_CONNECTION_STRING, STORAGE_ACCOUNT_KEY
```

### 4. Local Development

```bash
pip install -e ".[dev]"
python -m api.main        # FastAPI on http://localhost:8000
```

### 5. Deploy to Databricks

```bash
bash infrastructure/deploy_to_databricks.sh
```

## Project Structure

```
CryptoPulse/
├── agents/                        # LangGraph multi-agent system
│   ├── state.py                   # TypedDict agent state schema
│   ├── graph.py                   # StateGraph with conditional routing
│   ├── tools.py                   # LangChain tools (Binance, Delta queries)
│   ├── market_analyst.py          # Technical analysis agent
│   ├── sentiment_analyst.py       # NLP sentiment reasoning agent
│   ├── risk_manager.py            # Risk assessment + anomaly agent
│   ├── portfolio_strategist.py    # Signal fusion + recommendation agent
│   └── orchestrator.py            # Async wrapper for API integration
│
├── notebooks/                     # Databricks notebooks
│   ├── 01_data_ingestion.py       # Binance + News → Event Hubs
│   ├── 02_bronze_layer.py         # Structured Streaming → Bronze Delta
│   ├── 03_silver_layer.py         # Cleansing, dedup → Silver Delta
│   ├── 04_gold_features.py        # OHLCV, VWAP, RSI, EMA features
│   ├── 05_sentiment_analysis.py   # FinBERT sentiment pipeline
│   ├── 06_ml_training.py          # GBT + CrossValidator + MLflow
│   ├── 07_anomaly_detection.py    # Isolation Forest anomaly detection
│   ├── 08_model_serving.py        # MLflow Model Registry + batch predict
│   ├── 09_orchestration.py        # Pipeline orchestration DAG
│   └── 10_langgraph_agents.py     # Agent experimentation notebook
│
├── infrastructure/                # Azure deployment scripts
│   ├── azure_setup.sh             # Provision Azure resources via CLI
│   ├── databricks_setup.sh        # Configure Databricks workspace
│   └── deploy_to_databricks.sh    # Quick redeploy notebooks + wheel
│
├── api/                           # FastAPI REST service
│   ├── main.py                    # Endpoints: /predict, /analyze, /features
│   └── services.py                # Service layer (Prediction, Feature, Anomaly)
│
├── cryptopulse/                   # Core library
│   ├── config.py                  # Pydantic settings
│   ├── models.py                  # Data models
│   └── logging.py                 # Structured logging
│
├── tests/                         # Test suite
├── pyproject.toml                 # Dependencies & build config
└── docker-compose.yml             # Local development services
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API info |
| `GET` | `/health` | Health check |
| `GET` | `/predict/{symbol}` | ML price prediction |
| `POST` | `/analyze/{symbol}` | **LangGraph multi-agent analysis** |
| `GET` | `/features/{symbol}` | Market features |
| `GET` | `/anomalies` | Anomaly alerts |
| `GET` | `/market/overview` | Market overview |

## Key Technologies

| Layer | Technology |
|-------|-----------|
| Compute | Azure Databricks (Spark 14.3 LTS) |
| Storage | ADLS Gen2 + Delta Lake |
| Streaming | Azure Event Hubs + Spark Structured Streaming |
| ML | Spark MLlib, XGBoost, scikit-learn |
| NLP | FinBERT (HuggingFace Transformers) |
| AI Agents | LangGraph + LangChain + Google Gemini |
| Tracking | MLflow (experiments + model registry) |
| API | FastAPI + Uvicorn |
| Anomaly | Isolation Forest + Z-score detection |

## Testing

```bash
pytest tests/ -v                  # Unit tests
pytest tests/ -v -m integration   # Integration tests
```

## License

MIT
