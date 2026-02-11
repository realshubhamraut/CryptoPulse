# =============================================================================
# CryptoPulse - Development Makefile
# =============================================================================

.PHONY: help install dev test lint format docker-up docker-down deploy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ───────────────────────────────────────────────────────────────────
install: ## Install all dependencies
	python -m venv venv
	. venv/bin/activate && pip install -e ".[dev]"

# ── Development ─────────────────────────────────────────────────────────────
dev: ## Start FastAPI dev server
	. venv/bin/activate && python -m api.main

dev-docker: ## Start full local stack (Kafka, Spark, Redis, MLflow, API)
	docker compose up -d

# ── Testing & Quality ───────────────────────────────────────────────────────
test: ## Run all tests
	. venv/bin/activate && pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage
	. venv/bin/activate && pytest tests/ -v --cov=cryptopulse --cov=agents --cov=api --cov-report=term-missing

lint: ## Run linting (ruff + mypy)
	. venv/bin/activate && ruff check . && mypy cryptopulse/ agents/ api/

format: ## Format code with black + ruff
	. venv/bin/activate && black . && ruff check --fix .

# ── Docker ──────────────────────────────────────────────────────────────────
docker-build: ## Build the API Docker image
	docker build -t cryptopulse-api .

docker-up: ## Start all Docker services
	docker compose up -d

docker-down: ## Stop all Docker services
	docker compose down -v

docker-logs: ## Tail API logs
	docker compose logs -f api

# ── Databricks Deployment ───────────────────────────────────────────────────
deploy: ## Deploy to Databricks (notebooks + wheel)
	bash infrastructure/deploy_to_databricks.sh

deploy-infra: ## Provision Azure infrastructure
	bash infrastructure/azure_setup.sh

deploy-dbx: ## Configure Databricks workspace
	bash infrastructure/databricks_setup.sh

# ── Ingestion (local testing) ───────────────────────────────────────────────
ingest-trades: ## Run Binance trade ingestion locally
	. venv/bin/activate && python -m functions.binance_ingestion

ingest-news: ## Run news crawl ingestion locally
	. venv/bin/activate && python -m functions.news_ingestion

# ── Cleanup ─────────────────────────────────────────────────────────────────
clean: ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
