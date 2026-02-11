# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 07 — Anomaly Detection
# MAGIC
# MAGIC **CryptoPulse** | Isolation Forest anomaly detection for market manipulation
# MAGIC
# MAGIC Detects unusual trading patterns including pump & dumps, wash trading,
# MAGIC and volume spikes using statistical methods and Isolation Forest.
# MAGIC
# MAGIC | Anomaly Type | Detection Method |
# MAGIC |-------------|------------------|
# MAGIC | Volume Spike | Z-score > 3σ |
# MAGIC | Pump/Dump | Price change + volume correlation |
# MAGIC | Wash Trade | Buy/sell ratio anomaly |
# MAGIC | Price Manipulation | Volatility + trade intensity |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("z_threshold", "3.0", "Z-Score Threshold")

ENV = dbutils.widgets.get("environment")
Z_THRESHOLD = float(dbutils.widgets.get("z_threshold"))

STORAGE = f"stcryptopulse{ENV}"
GOLD_FEATURES_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/features/5m"
GOLD_ANOMALIES_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/anomalies"

print(f"🔧 Z-Score Threshold: {Z_THRESHOLD}")

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

mlflow.set_experiment("/CryptoPulse/anomaly_detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load Features & Compute Z-Scores

# COMMAND ----------

features_df = spark.read.format("delta").load(GOLD_FEATURES_PATH)
print(f"✓ Loaded {features_df.count()} feature rows")

# COMMAND ----------

# Compute rolling statistics per symbol
w = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-30, -1)

enriched = (
    features_df
    .withColumn("volume_f", F.col("volume").cast("double"))
    .withColumn("close_f", F.col("close").cast("double"))
    .withColumn("volatility_f", F.col("volatility").cast("double"))

    # Rolling means and stddevs
    .withColumn("vol_mean", F.avg("volume_f").over(w))
    .withColumn("vol_std", F.stddev("volume_f").over(w))
    .withColumn("price_mean", F.avg("close_f").over(w))
    .withColumn("price_std", F.stddev("close_f").over(w))
    .withColumn("volatility_mean", F.avg("volatility_f").over(w))
    .withColumn("volatility_std", F.stddev("volatility_f").over(w))

    # Z-Scores
    .withColumn("volume_zscore",
        F.when(F.col("vol_std") > 0,
               (F.col("volume_f") - F.col("vol_mean")) / F.col("vol_std")
        ).otherwise(0))
    .withColumn("price_change_zscore",
        F.when(F.col("price_std") > 0,
               (F.col("close_f") - F.col("price_mean")) / F.col("price_std")
        ).otherwise(0))
    .withColumn("volatility_zscore",
        F.when(F.col("volatility_std") > 0,
               (F.col("volatility_f") - F.col("volatility_mean")) / F.col("volatility_std")
        ).otherwise(0))

    # Filter rows with enough history
    .filter(F.col("vol_mean").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Isolation Forest (sklearn on driver)

# COMMAND ----------

# Collect features to driver for sklearn Isolation Forest
anomaly_features = ["volume_zscore", "price_change_zscore", "volatility_zscore",
                    "buy_sell_ratio", "trade_intensity"]

# Sample for efficiency
sample_df = enriched.select(
    "symbol", "timestamp", *anomaly_features
).fillna(0.0, subset=anomaly_features)

pandas_df = sample_df.toPandas()
print(f"✓ Collected {len(pandas_df)} rows to driver")

# COMMAND ----------

from sklearn.ensemble import IsolationForest
import numpy as np
import pandas as pd

with mlflow.start_run(run_name="isolation_forest"):
    # Train Isolation Forest
    iso_forest = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        max_features=0.8,
        random_state=42,
        n_jobs=-1,
    )

    X = pandas_df[anomaly_features].values
    pandas_df["anomaly_score"] = iso_forest.decision_function(X)
    pandas_df["is_anomaly"] = iso_forest.predict(X) == -1

    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("contamination", 0.05)
    mlflow.log_param("features", anomaly_features)
    mlflow.sklearn.log_model(iso_forest, "isolation_forest")

    anomaly_count = pandas_df["is_anomaly"].sum()
    anomaly_rate = anomaly_count / len(pandas_df) * 100
    mlflow.log_metric("anomaly_count", int(anomaly_count))
    mlflow.log_metric("anomaly_rate", anomaly_rate)

    print(f"✓ Anomalies detected: {anomaly_count} ({anomaly_rate:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏷️ Classify Anomaly Types

# COMMAND ----------

def classify_anomaly(row):
    """Classify anomaly type based on feature patterns."""
    if not row["is_anomaly"]:
        return "normal"

    vol_z = abs(row["volume_zscore"])
    price_z = row["price_change_zscore"]
    bs_ratio = row["buy_sell_ratio"]

    if vol_z > Z_THRESHOLD and price_z > 2:
        return "pump"
    elif vol_z > Z_THRESHOLD and price_z < -2:
        return "dump"
    elif abs(bs_ratio - 0.5) < 0.05 and vol_z > 2:
        return "wash_trade"
    elif vol_z > Z_THRESHOLD:
        return "volume_spike"
    else:
        return "price_manipulation"

pandas_df["anomaly_type"] = pandas_df.apply(classify_anomaly, axis=1)
pandas_df["severity"] = np.clip(np.abs(pandas_df["anomaly_score"]) * 2, 0, 1)

# Show anomaly distribution
anomalies_only = pandas_df[pandas_df["is_anomaly"]]
if len(anomalies_only) > 0:
    print("\nAnomaly type distribution:")
    print(anomalies_only["anomaly_type"].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write Anomalies to Gold Delta

# COMMAND ----------

# Convert back to Spark and save
anomalies_spark = spark.createDataFrame(
    pandas_df[pandas_df["is_anomaly"]][[
        "symbol", "timestamp", "anomaly_type", "severity",
        "volume_zscore", "price_change_zscore", "volatility_zscore",
        "anomaly_score",
    ]]
).withColumn("detected_at", F.current_timestamp())

(anomalies_spark.write
 .format("delta")
 .mode("overwrite")
 .save(GOLD_ANOMALIES_PATH))

print(f"✓ Anomalies written → {GOLD_ANOMALIES_PATH}")
display(anomalies_spark.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Anomaly Summary Dashboard

# COMMAND ----------

if len(anomalies_only) > 0:
    display(
        anomalies_spark
        .groupBy("symbol", "anomaly_type")
        .agg(
            F.count("*").alias("count"),
            F.avg("severity").alias("avg_severity"),
        )
        .orderBy(F.desc("count"))
    )
