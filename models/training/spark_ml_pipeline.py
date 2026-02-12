"""
CryptoPulse - Spark MLlib Training Pipeline

Distributed model training using PySpark ML for:
- Price direction classification via GBTClassifier
- Full ML Pipeline with VectorAssembler → Scaler → Classifier
- CrossValidator with ParamGridBuilder for hyperparameter tuning
- MulticlassClassificationEvaluator for evaluation
- MLflow experiment tracking integration

This replaces the single-node scikit-learn approach with a
distributed Spark-native pipeline that runs on Databricks clusters.

Resume claim: "Applied Spark MLlib to train models using weighted fusion
of market signals and NLP-derived news sentiment for short-horizon
directional prediction"
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    lag,
    avg as spark_avg,
    stddev as spark_stddev,
    count as spark_count,
    expr,
    window,
    unix_timestamp,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    StringIndexer,
    IndexToString,
)
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="spark_ml_pipeline")


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class SparkMLConfig:
    """Configuration for the Spark MLlib training pipeline."""

    # Feature columns — market microstructure
    market_features: list[str] = field(default_factory=lambda: [
        "price_change_pct",
        "high_low_range",
        "volume",
        "quote_volume",
        "buy_sell_ratio",
        "trade_intensity",
        "vwap_deviation",
        "volatility",
    ])

    # Feature columns — time-series derived
    timeseries_features: list[str] = field(default_factory=lambda: [
        "price_change_pct_lag_1",
        "price_change_pct_lag_3",
        "price_change_pct_lag_5",
        "volume_change_pct",
        "volatility_change",
        "ema_short",
        "ema_long",
        "ema_crossover",
        "rolling_mean_price",
        "rolling_std_price",
        "momentum",
    ])

    # Feature columns — NLP sentiment
    sentiment_features: list[str] = field(default_factory=lambda: [
        "sentiment_score",
        "sentiment_confidence",
        "article_count",
        "sentiment_momentum",
    ])

    # Fusion weights
    market_weight: float = 0.5
    sentiment_weight: float = 0.3
    timeseries_weight: float = 0.2

    # Model hyperparameters
    max_depth: int = 5
    max_iter: int = 100
    step_size: float = 0.1
    subsample_rate: float = 0.8

    # Cross-validation
    num_folds: int = 3

    # Prediction
    prediction_horizon_minutes: int = 5
    label_column: str = "direction"
    prediction_column: str = "prediction"

    # Paths
    model_output_path: str = "models/artifacts/spark_ml"
    features_path: str = ""
    sentiment_path: str = ""

    def __post_init__(self):
        if not self.features_path:
            self.features_path = f"{settings.storage.adls_delta_path}/gold/features"
        if not self.sentiment_path:
            self.sentiment_path = f"{settings.storage.adls_delta_path}/gold/sentiment"

    @property
    def all_feature_columns(self) -> list[str]:
        """All feature columns used for training."""
        return self.market_features + self.timeseries_features + self.sentiment_features


# =============================================================================
# Spark MLlib Pipeline
# =============================================================================

class SparkMLPipeline:
    """
    Distributed ML training pipeline using PySpark MLlib.

    Implements the full workflow:
    1. Read Gold features and sentiment from Delta Lake
    2. Compute time-series features (lags, EMA, momentum)
    3. Apply weighted signal fusion (market × 0.5 + sentiment × 0.3 + timeseries × 0.2)
    4. Build Spark ML Pipeline: StringIndexer → VectorAssembler → Scaler → GBTClassifier
    5. Hyperparameter tuning via CrossValidator + ParamGridBuilder
    6. Evaluate with MulticlassClassificationEvaluator
    7. Save model artifacts + log to MLflow
    """

    def __init__(
        self,
        spark: SparkSession | None = None,
        config: SparkMLConfig | None = None,
    ):
        self.spark = spark or self._create_spark_session()
        self.config = config or SparkMLConfig()
        self.pipeline_model: PipelineModel | None = None
        self.metrics: dict[str, float] = {}

    def _create_spark_session(self) -> SparkSession:
        """Create Delta-enabled Spark session."""
        return (
            SparkSession.builder
            .appName("CryptoPulse-SparkML-Training")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .getOrCreate()
        )

    # ─── Data Loading ─────────────────────────────────────────────────────────

    def load_features(self) -> DataFrame:
        """Read Gold market features from Delta Lake."""
        logger.info("loading_features", path=self.config.features_path)
        return (
            self.spark.read
            .format("delta")
            .load(self.config.features_path)
            .filter(col("interval") == "1m")
        )

    def load_sentiment(self) -> DataFrame:
        """Read Gold sentiment scores from Delta Lake."""
        logger.info("loading_sentiment", path=self.config.sentiment_path)
        return (
            self.spark.read
            .format("delta")
            .load(self.config.sentiment_path)
            .select(
                col("symbol"),
                col("window_start"),
                col("avg_sentiment").alias("sentiment_score"),
                col("max_confidence").alias("sentiment_confidence"),
                col("article_count"),
                col("sentiment_category"),
            )
        )

    # ─── Time-Series Feature Engineering ──────────────────────────────────────

    def compute_timeseries_features(self, df: DataFrame) -> DataFrame:
        """
        Compute time-series features for directional prediction.

        Resume claim: Platform includes "Time-Series Modeling"
        """
        # Window for lag features (ordered by time, partitioned by symbol)
        ts_window = Window.partitionBy("symbol").orderBy("window_start")

        # Window for rolling stats (last 10 rows)
        rolling_window = (
            Window.partitionBy("symbol")
            .orderBy("window_start")
            .rowsBetween(-9, 0)
        )

        # Window for EMA (short = 5 periods, long = 20 periods)
        ema_short_window = (
            Window.partitionBy("symbol")
            .orderBy("window_start")
            .rowsBetween(-4, 0)
        )
        ema_long_window = (
            Window.partitionBy("symbol")
            .orderBy("window_start")
            .rowsBetween(-19, 0)
        )

        return (
            df
            # ── Lag features ──
            .withColumn(
                "price_change_pct_lag_1",
                lag("price_change_pct", 1).over(ts_window)
            )
            .withColumn(
                "price_change_pct_lag_3",
                lag("price_change_pct", 3).over(ts_window)
            )
            .withColumn(
                "price_change_pct_lag_5",
                lag("price_change_pct", 5).over(ts_window)
            )
            # ── Volume momentum ──
            .withColumn(
                "prev_volume",
                lag("volume", 1).over(ts_window),
            )
            .withColumn(
                "volume_change_pct",
                when(col("prev_volume") > 0,
                     (col("volume") - col("prev_volume")) / col("prev_volume") * 100)
                .otherwise(lit(0.0)),
            )
            .drop("prev_volume")
            # ── Volatility change ──
            .withColumn(
                "prev_volatility",
                lag("volatility", 1).over(ts_window),
            )
            .withColumn(
                "volatility_change",
                when(col("prev_volatility") > 0,
                     (col("volatility") - col("prev_volatility")) / col("prev_volatility") * 100)
                .otherwise(lit(0.0)),
            )
            .drop("prev_volatility")
            # ── EMA crossovers (short = 5-period, long = 20-period) ──
            .withColumn("ema_short", spark_avg("close").over(ema_short_window))
            .withColumn("ema_long", spark_avg("close").over(ema_long_window))
            .withColumn(
                "ema_crossover",
                when(col("ema_short") > col("ema_long"), lit(1.0)).otherwise(lit(-1.0)),
            )
            # ── Rolling statistics ──
            .withColumn("rolling_mean_price", spark_avg("close").over(rolling_window))
            .withColumn("rolling_std_price", spark_stddev("close").over(rolling_window))
            # ── Momentum (current close vs close 5 periods ago) ──
            .withColumn(
                "close_lag_5",
                lag("close", 5).over(ts_window),
            )
            .withColumn(
                "momentum",
                when(col("close_lag_5") > 0,
                     (col("close") - col("close_lag_5")) / col("close_lag_5") * 100)
                .otherwise(lit(0.0)),
            )
            .drop("close_lag_5")
            # ── VWAP deviation (how far close is from VWAP) ──
            .withColumn(
                "vwap_deviation",
                when(col("vwap") > 0,
                     (col("close") - col("vwap")) / col("vwap") * 100)
                .otherwise(lit(0.0)),
            )
            # ── Drop rows with nulls from lag operations ──
            .na.fill(0.0)
        )

    # ─── Label Generation ─────────────────────────────────────────────────────

    def create_labels(self, df: DataFrame) -> DataFrame:
        """
        Create direction labels for supervised training.

        Labels: UP / DOWN / NEUTRAL based on future price change.
        """
        ts_window = Window.partitionBy("symbol").orderBy("window_start")

        horizon = self.config.prediction_horizon_minutes

        return (
            df
            .withColumn(
                "future_close",
                lag("close", -horizon).over(ts_window),
            )
            .withColumn(
                "future_return",
                when(col("close") > 0,
                     (col("future_close") - col("close")) / col("close") * 100)
                .otherwise(lit(0.0)),
            )
            .withColumn(
                self.config.label_column,
                when(col("future_return") > 0.1, lit("UP"))
                .when(col("future_return") < -0.1, lit("DOWN"))
                .otherwise(lit("NEUTRAL")),
            )
            # Drop rows without future target
            .filter(col("future_close").isNotNull())
            .drop("future_close", "future_return")
        )

    # ─── Weighted Signal Fusion ───────────────────────────────────────────────

    def fuse_signals(
        self,
        features_df: DataFrame,
        sentiment_df: DataFrame,
    ) -> DataFrame:
        """
        Weighted fusion of market signals and NLP-derived news sentiment.

        Resume claim: "weighted fusion of market signals and NLP-derived 
        news sentiment for short-horizon directional prediction"

        Fusion formula:
            composite_signal = (market_weight × normalized_market_score)
                             + (sentiment_weight × sentiment_score)
                             + (timeseries_weight × momentum_score)
        """
        logger.info(
            "fusing_signals",
            market_weight=self.config.market_weight,
            sentiment_weight=self.config.sentiment_weight,
            timeseries_weight=self.config.timeseries_weight,
        )

        # Join features with sentiment on symbol + time window
        fused = features_df.join(
            sentiment_df,
            on=["symbol", "window_start"],
            how="left",
        )

        # Fill missing sentiment with neutral defaults
        fused = (
            fused
            .na.fill({"sentiment_score": 0.0, "sentiment_confidence": 0.5, "article_count": 0})
            # Compute sentiment momentum (change from previous period)
            .withColumn(
                "sentiment_momentum",
                lag("sentiment_score", 1).over(
                    Window.partitionBy("symbol").orderBy("window_start")
                ),
            )
            .withColumn(
                "sentiment_momentum",
                when(col("sentiment_momentum").isNotNull(),
                     col("sentiment_score") - col("sentiment_momentum"))
                .otherwise(lit(0.0)),
            )
        )

        # ── Weighted composite signal ──
        wm = self.config.market_weight
        ws = self.config.sentiment_weight
        wt = self.config.timeseries_weight

        fused = fused.withColumn(
            "weighted_composite_signal",
            (
                lit(wm) * col("price_change_pct")
                + lit(ws) * col("sentiment_score")
                + lit(wt) * col("momentum")
            ),
        )

        logger.info("signal_fusion_complete", total_rows=fused.count())
        return fused

    # ─── Spark ML Pipeline Construction ───────────────────────────────────────

    def build_pipeline(self) -> Pipeline:
        """
        Build a Spark ML Pipeline with:
        - StringIndexer: Encode direction labels → numeric index
        - VectorAssembler: Merge all features into a single vector column
        - StandardScaler: Normalize the feature vector
        - GBTClassifier: Gradient Boosted Trees for classification
        """
        # Stage 1: Index the target label
        label_indexer = StringIndexer(
            inputCol=self.config.label_column,
            outputCol="label",
            handleInvalid="keep",
        )

        # Stage 2: Assemble feature columns into a vector
        feature_cols = self.config.all_feature_columns + ["weighted_composite_signal"]
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="raw_features",
            handleInvalid="skip",
        )

        # Stage 3: Scale features
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=True,
        )

        # Stage 4: GBT Classifier (Spark MLlib native)
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol=self.config.prediction_column,
            maxDepth=self.config.max_depth,
            maxIter=self.config.max_iter,
            stepSize=self.config.step_size,
            subsamplingRate=self.config.subsample_rate,
            seed=42,
        )

        # Stage 5: Convert prediction index back to label
        label_converter = IndexToString(
            inputCol=self.config.prediction_column,
            outputCol="predicted_direction",
            labels=["UP", "DOWN", "NEUTRAL"],
        )

        return Pipeline(stages=[
            label_indexer,
            assembler,
            scaler,
            gbt,
            label_converter,
        ])

    # ─── Training ─────────────────────────────────────────────────────────────

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame | None = None,
    ) -> dict[str, Any]:
        """
        Train the Spark MLlib pipeline.

        Uses CrossValidator + ParamGridBuilder for hyperparameter tuning
        and MulticlassClassificationEvaluator for metric computation.
        """
        logger.info("starting_spark_ml_training", train_rows=train_df.count())

        pipeline = self.build_pipeline()

        # ── Build parameter grid for hyperparameter tuning ──
        gbt_stage = pipeline.getStages()[-2]  # GBTClassifier stage
        param_grid = (
            ParamGridBuilder()
            .addGrid(gbt_stage.maxDepth, [3, 5, 7])
            .addGrid(gbt_stage.maxIter, [50, 100])
            .addGrid(gbt_stage.stepSize, [0.05, 0.1])
            .build()
        )

        # ── Evaluator ──
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol=self.config.prediction_column,
            metricName="f1",
        )

        # ── Cross-validator ──
        cross_validator = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=self.config.num_folds,
            parallelism=2,
            seed=42,
        )

        # ── Fit ──
        logger.info("running_cross_validation", folds=self.config.num_folds)
        cv_model = cross_validator.fit(train_df)
        self.pipeline_model = cv_model.bestModel

        # ── Evaluate ──
        eval_df = val_df if val_df is not None else train_df
        predictions = self.pipeline_model.transform(eval_df)

        self.metrics = {
            "f1": evaluator.evaluate(predictions, {evaluator.metricName: "f1"}),
            "accuracy": evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"}),
            "precision": evaluator.evaluate(
                predictions, {evaluator.metricName: "weightedPrecision"}
            ),
            "recall": evaluator.evaluate(
                predictions, {evaluator.metricName: "weightedRecall"}
            ),
            "best_depth": self.pipeline_model.stages[-2].getOrDefault("maxDepth"),
            "best_iter": self.pipeline_model.stages[-2].getOrDefault("maxIter"),
            "train_rows": train_df.count(),
            "eval_rows": eval_df.count(),
            "timestamp": datetime.utcnow().isoformat(),
        }

        logger.info("spark_ml_training_complete", **self.metrics)
        return self.metrics

    # ─── Prediction ───────────────────────────────────────────────────────────

    def predict(self, df: DataFrame) -> DataFrame:
        """Run inference using the fitted pipeline model."""
        if self.pipeline_model is None:
            raise RuntimeError("Model not trained — call train() first or load()")
        return self.pipeline_model.transform(df)

    # ─── Persistence ──────────────────────────────────────────────────────────

    def save(self, path: str | None = None) -> str:
        """Save the trained Spark ML pipeline model to disk or DBFS."""
        save_path = path or self.config.model_output_path
        if self.pipeline_model is None:
            raise RuntimeError("No model to save — call train() first")

        self.pipeline_model.write().overwrite().save(save_path)
        logger.info("model_saved", path=save_path)
        return save_path

    def load(self, path: str | None = None) -> "SparkMLPipeline":
        """Load a previously saved Spark ML pipeline model."""
        load_path = path or self.config.model_output_path
        self.pipeline_model = PipelineModel.load(load_path)
        logger.info("model_loaded", path=load_path)
        return self

    # ─── Feature Importance ───────────────────────────────────────────────────

    def feature_importance(self) -> list[dict[str, Any]]:
        """Extract feature importances from the GBT model."""
        if self.pipeline_model is None:
            raise RuntimeError("Model not trained")

        gbt_model = self.pipeline_model.stages[-2]  # GBTClassificationModel
        importances = gbt_model.featureImportances.toArray()
        feature_names = self.config.all_feature_columns + ["weighted_composite_signal"]

        ranked = sorted(
            zip(feature_names, importances),
            key=lambda x: x[1],
            reverse=True,
        )

        return [
            {"feature": name, "importance": float(imp), "rank": i + 1}
            for i, (name, imp) in enumerate(ranked)
        ]

    # ─── MLflow Integration ───────────────────────────────────────────────────

    def log_to_mlflow(self, experiment_name: str = "cryptopulse-spark-ml") -> None:
        """Log model, params, and metrics to MLflow."""
        try:
            import mlflow
            import mlflow.spark

            mlflow.set_experiment(experiment_name)

            with mlflow.start_run(run_name=f"spark_gbt_{datetime.utcnow():%Y%m%d_%H%M}"):
                # Log config
                mlflow.log_params({
                    "max_depth": self.config.max_depth,
                    "max_iter": self.config.max_iter,
                    "step_size": self.config.step_size,
                    "market_weight": self.config.market_weight,
                    "sentiment_weight": self.config.sentiment_weight,
                    "timeseries_weight": self.config.timeseries_weight,
                    "prediction_horizon_minutes": self.config.prediction_horizon_minutes,
                    "num_folds": self.config.num_folds,
                })

                # Log metrics
                mlflow.log_metrics(
                    {k: v for k, v in self.metrics.items() if isinstance(v, (int, float))}
                )

                # Log Spark ML model
                if self.pipeline_model:
                    mlflow.spark.log_model(self.pipeline_model, "spark_ml_model")

                # Log feature importances
                importance = self.feature_importance()
                mlflow.log_dict(importance, "feature_importances.json")

                logger.info("mlflow_logged", experiment=experiment_name)

        except ImportError:
            logger.warning("mlflow_not_available")


# =============================================================================
# End-to-End Training Entrypoint
# =============================================================================

def train_from_delta(
    spark: SparkSession | None = None,
    config: SparkMLConfig | None = None,
    test_split: float = 0.2,
) -> SparkMLPipeline:
    """
    Full training workflow: load → engineer → fuse → train → save.

    Usage:
        # On Databricks:
        pipeline = train_from_delta(spark=spark)
        print(pipeline.metrics)
        print(pipeline.feature_importance())
    """
    pipeline = SparkMLPipeline(spark=spark, config=config)

    # 1. Load raw features and sentiment from Delta Lake
    features_df = pipeline.load_features()
    sentiment_df = pipeline.load_sentiment()

    # 2. Compute time-series features
    features_df = pipeline.compute_timeseries_features(features_df)

    # 3. Weighted signal fusion
    fused_df = pipeline.fuse_signals(features_df, sentiment_df)

    # 4. Create labels
    labeled_df = pipeline.create_labels(fused_df)

    # 5. Train/test split
    train_df, val_df = labeled_df.randomSplit([1 - test_split, test_split], seed=42)

    # 6. Train with cross-validation
    metrics = pipeline.train(train_df, val_df)

    # 7. Save model
    pipeline.save()

    # 8. Log to MLflow
    pipeline.log_to_mlflow()

    logger.info("end_to_end_training_complete", **metrics)
    return pipeline


if __name__ == "__main__":
    pipeline = train_from_delta()
    print("\n✅ Training complete!")
    print(f"   F1 Score: {pipeline.metrics.get('f1', 'N/A'):.4f}")
    print(f"   Accuracy: {pipeline.metrics.get('accuracy', 'N/A'):.4f}")
    print("\n📊 Top 10 Feature Importances:")
    for feat in pipeline.feature_importance()[:10]:
        print(f"   {feat['rank']:2d}. {feat['feature']:<35s} {feat['importance']:.4f}")
