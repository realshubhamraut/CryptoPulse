# Databricks notebook source
# MAGIC %md
# MAGIC # 🔗 09 — Pipeline Orchestration
# MAGIC
# MAGIC **CryptoPulse** | End-to-end pipeline orchestration
# MAGIC
# MAGIC Chains all pipeline stages in a single DAG, suitable for scheduling
# MAGIC as a Databricks Workflow or triggered via the Jobs API.
# MAGIC
# MAGIC ```
# MAGIC 01 Ingest → 02 Bronze → 03 Silver → 04 Features ─┐
# MAGIC                                    05 Sentiment ──┤
# MAGIC                                                   ├→ 06 Train → 08 Serve
# MAGIC                                                   └→ 07 Anomaly
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.dropdown("run_mode", "batch", ["batch", "full", "inference_only"], "Run Mode")

ENV = dbutils.widgets.get("environment")
RUN_MODE = dbutils.widgets.get("run_mode")

print(f"🔧 Orchestration Mode: {RUN_MODE} | Environment: {ENV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Pipeline Runner

# COMMAND ----------

import time
from datetime import datetime, timezone

class PipelineOrchestrator:
    """Orchestrates the CryptoPulse data pipeline stages."""

    def __init__(self, env: str):
        self.env = env
        self.results = {}
        self.start_time = time.time()

    def run_stage(self, stage_name: str, notebook_path: str, params: dict = None):
        """Run a pipeline stage via notebook execution."""
        print(f"\n{'━' * 60}")
        print(f"  Stage: {stage_name}")
        print(f"{'━' * 60}")

        stage_start = time.time()
        params = params or {}
        params["environment"] = self.env

        try:
            result = dbutils.notebook.run(
                notebook_path,
                timeout_seconds=1800,  # 30 min timeout
                arguments=params,
            )
            elapsed = time.time() - stage_start
            self.results[stage_name] = {
                "status": "SUCCESS",
                "elapsed_s": round(elapsed, 1),
                "result": result,
            }
            print(f"  ✓ {stage_name} completed in {elapsed:.1f}s")

        except Exception as e:
            elapsed = time.time() - stage_start
            self.results[stage_name] = {
                "status": "FAILED",
                "elapsed_s": round(elapsed, 1),
                "error": str(e),
            }
            print(f"  ✗ {stage_name} failed after {elapsed:.1f}s: {e}")

    def summary(self):
        """Print pipeline execution summary."""
        total_elapsed = time.time() - self.start_time
        print(f"\n{'═' * 60}")
        print(f"  PIPELINE EXECUTION SUMMARY")
        print(f"{'═' * 60}")
        print(f"  Total Time: {total_elapsed:.1f}s")
        print(f"  Stages Run: {len(self.results)}")
        print()

        for stage, info in self.results.items():
            status_icon = "✓" if info["status"] == "SUCCESS" else "✗"
            print(f"  {status_icon} {stage:<30} {info['status']:<10} {info['elapsed_s']:>8.1f}s")

        failures = [s for s, i in self.results.items() if i["status"] == "FAILED"]
        if failures:
            print(f"\n  ⚠ Failed stages: {', '.join(failures)}")
        else:
            print(f"\n  ✓ All stages completed successfully!")

        return len(failures) == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Pipeline

# COMMAND ----------

orchestrator = PipelineOrchestrator(ENV)

if RUN_MODE == "full":
    # Full pipeline: ingest → process → train → serve
    orchestrator.run_stage("Data Ingestion", "/CryptoPulse/01_data_ingestion", {
        "trading_pairs": "BTCUSDT,ETHUSDT,BNBUSDT",
        "trades_per_pair": "100",
    })
    orchestrator.run_stage("Bronze Layer", "/CryptoPulse/02_bronze_layer", {
        "stream_type": "both",
    })
    orchestrator.run_stage("Silver Layer", "/CryptoPulse/03_silver_layer")
    orchestrator.run_stage("Gold Features", "/CryptoPulse/04_gold_features", {
        "mode": "batch",
        "intervals": "1m,5m,15m",
    })
    orchestrator.run_stage("Sentiment Analysis", "/CryptoPulse/05_sentiment_analysis")
    orchestrator.run_stage("ML Training", "/CryptoPulse/06_ml_training", {
        "target_symbol": "BTCUSDT",
        "horizon_minutes": "5",
    })
    orchestrator.run_stage("Anomaly Detection", "/CryptoPulse/07_anomaly_detection")
    orchestrator.run_stage("Model Serving", "/CryptoPulse/08_model_serving", {
        "target_symbol": "BTCUSDT",
    })

elif RUN_MODE == "batch":
    # Batch processing only (no ingestion or training)
    orchestrator.run_stage("Gold Features", "/CryptoPulse/04_gold_features", {
        "mode": "batch",
    })
    orchestrator.run_stage("Sentiment Analysis", "/CryptoPulse/05_sentiment_analysis")
    orchestrator.run_stage("Anomaly Detection", "/CryptoPulse/07_anomaly_detection")
    orchestrator.run_stage("Model Serving", "/CryptoPulse/08_model_serving", {
        "target_symbol": "BTCUSDT",
    })

elif RUN_MODE == "inference_only":
    # Just run predictions with existing model
    orchestrator.run_stage("Model Serving", "/CryptoPulse/08_model_serving", {
        "target_symbol": "BTCUSDT",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Pipeline Summary

# COMMAND ----------

success = orchestrator.summary()

if not success:
    dbutils.notebook.exit("PIPELINE_FAILED")
else:
    dbutils.notebook.exit("PIPELINE_SUCCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Scheduling (Databricks Workflow)
# MAGIC
# MAGIC To schedule this pipeline as a recurring job:
# MAGIC
# MAGIC ```python
# MAGIC # Via Databricks Jobs API
# MAGIC import requests
# MAGIC
# MAGIC job_config = {
# MAGIC     "name": "CryptoPulse Daily Pipeline",
# MAGIC     "tasks": [{
# MAGIC         "task_key": "orchestration",
# MAGIC         "notebook_task": {
# MAGIC             "notebook_path": "/CryptoPulse/09_orchestration",
# MAGIC             "base_parameters": {
# MAGIC                 "environment": "prod",
# MAGIC                 "run_mode": "full",
# MAGIC             },
# MAGIC         },
# MAGIC         "existing_cluster_id": "<your-cluster-id>",
# MAGIC     }],
# MAGIC     "schedule": {
# MAGIC         "quartz_cron_expression": "0 0 */4 * * ?",  # Every 4 hours
# MAGIC         "timezone_id": "UTC",
# MAGIC     },
# MAGIC }
# MAGIC ```
