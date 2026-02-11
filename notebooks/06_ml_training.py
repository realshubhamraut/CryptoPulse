# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 06 — ML Model Training: Price Direction
# MAGIC
# MAGIC **CryptoPulse** | Gradient Boosted Tree for price direction prediction
# MAGIC
# MAGIC Trains a classification model predicting short-term price direction
# MAGIC (UP / DOWN / NEUTRAL) using fused market features + sentiment signals.
# MAGIC
# MAGIC | Component | Technology |
# MAGIC |-----------|-----------|
# MAGIC | Algorithm | XGBoost + Spark MLlib CrossValidator |
# MAGIC | Features | Gold market features + sentiment |
# MAGIC | Tracking | MLflow experiment tracking |
# MAGIC | Registry | MLflow Model Registry |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("target_symbol", "BTCUSDT", "Target Symbol")
dbutils.widgets.text("horizon_minutes", "5", "Prediction Horizon (min)")
dbutils.widgets.text("test_split", "0.2", "Test Split Ratio")

ENV = dbutils.widgets.get("environment")
TARGET_SYMBOL = dbutils.widgets.get("target_symbol")
HORIZON = int(dbutils.widgets.get("horizon_minutes"))
TEST_SPLIT = float(dbutils.widgets.get("test_split"))

STORAGE = f"stcryptopulse{ENV}"
GOLD_FEATURES_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/features/5m"
GOLD_SENTIMENT_PATH = f"abfss://gold@{STORAGE}.dfs.core.windows.net/sentiment/aggregated"
MODEL_PATH = f"abfss://models@{STORAGE}.dfs.core.windows.net/price_direction"

print(f"🔧 Symbol: {TARGET_SYMBOL} | Horizon: {HORIZON}min | Test: {TEST_SPLIT}")

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql import functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

mlflow.set_experiment(f"/CryptoPulse/price_direction_{TARGET_SYMBOL}")
print("✓ MLflow experiment configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load & Prepare Training Data

# COMMAND ----------

# Load Gold features
features_df = (
    spark.read.format("delta").load(GOLD_FEATURES_PATH)
    .filter(F.col("symbol") == TARGET_SYMBOL)
    .orderBy("timestamp")
)

# Load sentiment (if available)
try:
    sentiment_df = (
        spark.read.format("delta").load(GOLD_SENTIMENT_PATH)
        .filter(F.col("symbol") == TARGET_SYMBOL)
        .select("symbol", "mean_score", "weighted_score", "article_count",
                "positive_count", "negative_count")
    )
    # Join sentiment to features
    features_df = features_df.join(sentiment_df, on="symbol", how="left")
    features_df = features_df.fillna({"mean_score": 0, "weighted_score": 0,
                                       "article_count": 0, "positive_count": 0,
                                       "negative_count": 0})
    print("✓ Sentiment features joined")
except Exception as e:
    print(f"⚠ Sentiment data not available: {e}")
    features_df = features_df.withColumn("mean_score", F.lit(0.0))
    features_df = features_df.withColumn("weighted_score", F.lit(0.0))
    features_df = features_df.withColumn("article_count", F.lit(0))
    features_df = features_df.withColumn("positive_count", F.lit(0))
    features_df = features_df.withColumn("negative_count", F.lit(0))

total_rows = features_df.count()
print(f"✓ Training dataset: {total_rows} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Labels

# COMMAND ----------

# Create price direction label based on future price
w = Window.partitionBy("symbol").orderBy("timestamp")

labeled_df = (
    features_df
    .withColumn("future_close", F.lead("close", HORIZON).over(w))
    .filter(F.col("future_close").isNotNull())
    .withColumn("price_change",
        (F.col("future_close") - F.col("close")) / F.col("close") * 100)
    .withColumn("direction",
        F.when(F.col("price_change") > 0.1, "UP")
         .when(F.col("price_change") < -0.1, "DOWN")
         .otherwise("NEUTRAL"))
)

# Show label distribution
print("Label distribution:")
display(labeled_df.groupBy("direction").count().orderBy("direction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Feature Engineering Pipeline

# COMMAND ----------

# Feature columns for the model
FEATURE_COLS = [
    "close", "volume", "vwap", "volatility", "trade_count",
    "buy_sell_ratio", "price_change_pct", "high_low_range",
    "trade_intensity", "rsi", "ema_20",
    "mean_score", "weighted_score", "article_count",
]

# Index the label
label_indexer = StringIndexer(inputCol="direction", outputCol="label")

# Assemble feature vector
assembler = VectorAssembler(
    inputCols=FEATURE_COLS,
    outputCol="raw_features",
    handleInvalid="skip",
)

# Scale features
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True,
)

# Classifier
gbt = GBTClassifier(
    labelCol="label",
    featuresCol="features",
    maxDepth=5,
    maxIter=50,
    seed=42,
)

# Full pipeline
pipeline = Pipeline(stages=[label_indexer, assembler, scaler, gbt])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Train / Test Split & Cross-Validation

# COMMAND ----------

# Cast feature columns to double
for col_name in FEATURE_COLS:
    labeled_df = labeled_df.withColumn(col_name, F.col(col_name).cast("double"))

# Fill nulls
labeled_df = labeled_df.fillna(0.0, subset=FEATURE_COLS)

# Split
train_df, test_df = labeled_df.randomSplit([1 - TEST_SPLIT, TEST_SPLIT], seed=42)
print(f"✓ Train: {train_df.count()} | Test: {test_df.count()}")

# COMMAND ----------

# Hyperparameter grid
param_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .addGrid(gbt.maxIter, [30, 50, 100])
    .build()
)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1",
)

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Model with MLflow Tracking

# COMMAND ----------

with mlflow.start_run(run_name=f"gbt_{TARGET_SYMBOL}_{HORIZON}min") as run:
    mlflow.log_param("symbol", TARGET_SYMBOL)
    mlflow.log_param("horizon_minutes", HORIZON)
    mlflow.log_param("feature_count", len(FEATURE_COLS))
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())

    print("🏋️ Training GBT model with 3-fold cross-validation...")
    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel

    # Evaluate on test set
    predictions = best_model.transform(test_df)

    f1 = evaluator.evaluate(predictions)
    accuracy = evaluator.evaluate(predictions,
        {evaluator.metricName: "accuracy"})
    precision = evaluator.evaluate(predictions,
        {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(predictions,
        {evaluator.metricName: "weightedRecall"})

    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)

    # Log model
    mlflow.spark.log_model(best_model, "price_direction_model")

    print(f"\n{'═' * 50}")
    print(f"  MODEL RESULTS — {TARGET_SYMBOL}")
    print(f"{'═' * 50}")
    print(f"  F1 Score:   {f1:.4f}")
    print(f"  Accuracy:   {accuracy:.4f}")
    print(f"  Precision:  {precision:.4f}")
    print(f"  Recall:     {recall:.4f}")
    print(f"  MLflow Run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Confusion Matrix

# COMMAND ----------

display(
    predictions
    .groupBy("direction", "prediction")
    .count()
    .orderBy("direction", "prediction")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Best Model

# COMMAND ----------

best_model.write().overwrite().save(MODEL_PATH)
print(f"✓ Model saved to: {MODEL_PATH}")
