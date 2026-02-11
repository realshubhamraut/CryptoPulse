# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 08 — Model Serving & Registry
# MAGIC
# MAGIC **CryptoPulse** | MLflow model registration, versioning, and batch inference
# MAGIC
# MAGIC Loads trained models from MLflow, registers them in the Model Registry,
# MAGIC transitions to staging/production, and generates batch predictions.
# MAGIC
# MAGIC | Step | Description |
# MAGIC |------|-------------|
# MAGIC | Register | Push best run to MLflow Model Registry |
# MAGIC | Transition | Stage → Production lifecycle |
# MAGIC | Batch Predict | Generate predictions on latest Gold features |
# MAGIC | Write | Predictions → Gold Delta table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("model_name", "cryptopulse_price_direction", "Model Name")
dbutils.widgets.text("target_symbol", "BTCUSDT", "Target Symbol")

ENV = dbutils.widgets.get("environment")
MODEL_NAME = dbutils.widgets.get("model_name")
TARGET_SYMBOL = dbutils.widgets.get("target_symbol")

STORAGE = f"stcryptopulse{ENV}"
GOLD_FEATURES_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/features/5m"
GOLD_PREDICTIONS_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/predictions"

print(f"🔧 Model: {MODEL_NAME} | Symbol: {TARGET_SYMBOL}")

# COMMAND ----------

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Find Best Model Run

# COMMAND ----------

experiment = mlflow.get_experiment_by_name(f"/CryptoPulse/price_direction_{TARGET_SYMBOL}")

if experiment:
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.f1_score DESC"],
        max_results=5,
    )

    print(f"Found {len(runs)} runs for {TARGET_SYMBOL}:")
    print(f"{'Run ID':<36} {'F1':>8} {'Accuracy':>10} {'Status':<10}")
    print("─" * 70)
    for run in runs:
        f1 = run.data.metrics.get("f1_score", 0)
        acc = run.data.metrics.get("accuracy", 0)
        print(f"{run.info.run_id:<36} {f1:>8.4f} {acc:>10.4f} {run.info.status:<10}")

    best_run = runs[0] if runs else None
    if best_run:
        BEST_RUN_ID = best_run.info.run_id
        print(f"\n✓ Best run: {BEST_RUN_ID} (F1: {best_run.data.metrics.get('f1_score', 0):.4f})")
else:
    print("⚠ No experiment found. Run notebook 06_ml_training first.")
    BEST_RUN_ID = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Register Model

# COMMAND ----------

if BEST_RUN_ID:
    model_uri = f"runs:/{BEST_RUN_ID}/price_direction_model"

    # Register in MLflow Model Registry
    result = mlflow.register_model(
        model_uri=model_uri,
        name=MODEL_NAME,
    )

    print(f"✓ Registered: {MODEL_NAME} v{result.version}")
    print(f"  Source: {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Transition Model Stage

# COMMAND ----------

if BEST_RUN_ID:
    # Get latest version
    latest = client.get_latest_versions(MODEL_NAME, stages=["None"])
    if latest:
        version = latest[0].version

        # Transition to Staging
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=version,
            stage="Staging",
        )
        print(f"✓ {MODEL_NAME} v{version} → Staging")

        # For production: transition to Production
        if ENV == "prod":
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=version,
                stage="Production",
            )
            print(f"✓ {MODEL_NAME} v{version} → Production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔮 Batch Predictions

# COMMAND ----------

if BEST_RUN_ID:
    print("─── Generating Batch Predictions ───")

    # Load the latest model from registry
    stage = "Production" if ENV == "prod" else "Staging"
    model = mlflow.spark.load_model(f"models:/{MODEL_NAME}/{stage}")

    # Load latest features
    features = (
        spark.read.format("delta").load(GOLD_FEATURES_PATH)
        .filter(F.col("symbol") == TARGET_SYMBOL)
        .orderBy(F.desc("timestamp"))
        .limit(100)  # Latest 100 intervals
    )

    # Add placeholder sentiment columns if missing
    for col_name in ["mean_score", "weighted_score", "article_count",
                      "positive_count", "negative_count"]:
        if col_name not in features.columns:
            features = features.withColumn(col_name, F.lit(0.0))

    # Cast feature cols to double
    feature_cols = ["close", "volume", "vwap", "volatility", "trade_count",
                    "buy_sell_ratio", "price_change_pct", "high_low_range",
                    "trade_intensity", "rsi", "ema_20",
                    "mean_score", "weighted_score", "article_count"]
    for c in feature_cols:
        features = features.withColumn(c, F.col(c).cast("double"))
    features = features.fillna(0.0, subset=feature_cols)

    # Predict
    predictions = model.transform(features)

    # Map prediction back to labels
    direction_map = {0.0: "UP", 1.0: "DOWN", 2.0: "NEUTRAL"}
    map_udf = F.udf(lambda x: direction_map.get(float(x), "NEUTRAL"))

    results = (
        predictions
        .withColumn("predicted_direction", map_udf(F.col("prediction")))
        .withColumn("predicted_at", F.current_timestamp())
        .withColumn("model_version", F.lit(f"{MODEL_NAME}/staging"))
        .select(
            "symbol", "timestamp", "close", "predicted_direction",
            "prediction", "predicted_at", "model_version",
        )
    )

    # Write to Gold Delta
    (results.write
     .format("delta")
     .mode("overwrite")
     .save(GOLD_PREDICTIONS_PATH))

    print(f"✓ Predictions written → {GOLD_PREDICTIONS_PATH}")
    display(results.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Model Registry Summary

# COMMAND ----------

try:
    for mv in client.search_model_versions(f"name='{MODEL_NAME}'"):
        print(f"  v{mv.version} | Stage: {mv.current_stage} | Run: {mv.run_id[:12]}...")
except:
    print("No registered models yet.")
