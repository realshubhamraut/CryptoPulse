"""
CryptoPulse - Weighted Signal Fusion Pipeline

Combines market microstructure features with NLP-derived sentiment
scores using configurable weighted fusion for predictive analytics.

Resume claim: "weighted fusion of market signals and NLP-derived news
sentiment for short-horizon directional prediction"

Architecture:
    Gold Features (OHLC, VWAP, volatility)  ─┐
                                              ├──▶  Weighted Fusion  ──▶  Training
    Gold Sentiment (FinBERT scores)          ─┘

Fusion Formula:
    composite = w_market × market_signal_normalized
              + w_sentiment × sentiment_signal
              + w_timeseries × momentum_signal
"""

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    lag,
    avg as spark_avg,
    stddev as spark_stddev,
    abs as spark_abs,
    coalesce,
    struct,
    greatest,
    least,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="signal_fusion")


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class FusionConfig:
    """Configuration for the weighted signal fusion pipeline."""

    # Weights must sum to 1.0
    market_weight: float = 0.5
    sentiment_weight: float = 0.3
    timeseries_weight: float = 0.2

    # Normalization window size (rows)
    normalization_window: int = 20

    # Sentiment decay: older sentiment signals decay by this factor per period
    sentiment_decay: float = 0.9

    # Feature paths
    features_path: str = ""
    sentiment_path: str = ""
    output_path: str = ""

    def __post_init__(self):
        assert abs(self.market_weight + self.sentiment_weight + self.timeseries_weight - 1.0) < 1e-6, \
            f"Weights must sum to 1.0, got {self.market_weight + self.sentiment_weight + self.timeseries_weight}"

        if not self.features_path:
            self.features_path = f"{settings.storage.adls_delta_path}/gold/features"
        if not self.sentiment_path:
            self.sentiment_path = f"{settings.storage.adls_delta_path}/gold/sentiment"
        if not self.output_path:
            self.output_path = f"{settings.storage.adls_delta_path}/gold/fused_signals"


# =============================================================================
# Signal Fusion Pipeline
# =============================================================================

class SignalFusionPipeline:
    """
    Weighted fusion of heterogeneous market and NLP signals.

    Combines three signal families:
    1. **Market signals**: price_change_pct, VWAP deviation, buy/sell ratio
    2. **Sentiment signals**: FinBERT score, sentiment confidence, momentum
    3. **Time-series signals**: EMA crossover, price momentum, volatility trend

    Each family is Z-score normalised within a rolling window, then
    multiplied by its configured weight to produce a composite signal.
    """

    def __init__(
        self,
        spark: SparkSession | None = None,
        config: FusionConfig | None = None,
    ):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.config = config or FusionConfig()

    # ─── Data Loading ─────────────────────────────────────────────────────────

    def load_market_features(self) -> DataFrame:
        """Load Gold market features from Delta Lake."""
        return (
            self.spark.read
            .format("delta")
            .load(self.config.features_path)
            .filter(col("interval") == "1m")
        )

    def load_sentiment(self) -> DataFrame:
        """Load Gold sentiment aggregates from Delta Lake."""
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
            )
        )

    # ─── Signal Normalisation ─────────────────────────────────────────────────

    def _z_score_normalize(
        self,
        df: DataFrame,
        column: str,
        output_col: str | None = None,
    ) -> DataFrame:
        """
        Z-score normalize a column within a rolling window per symbol.

        z = (x - μ) / σ

        This makes signals from different families comparable before fusion.
        """
        out_col = output_col or f"{column}_normalised"
        w = self.config.normalization_window

        rolling = (
            Window.partitionBy("symbol")
            .orderBy("window_start")
            .rowsBetween(-w, 0)
        )

        return (
            df
            .withColumn(f"_{column}_mean", spark_avg(column).over(rolling))
            .withColumn(f"_{column}_std", spark_stddev(column).over(rolling))
            .withColumn(
                out_col,
                when(col(f"_{column}_std") > 0,
                     (col(column) - col(f"_{column}_mean")) / col(f"_{column}_std"))
                .otherwise(lit(0.0)),
            )
            .drop(f"_{column}_mean", f"_{column}_std")
        )

    # ─── Market Signal Computation ────────────────────────────────────────────

    def compute_market_signal(self, df: DataFrame) -> DataFrame:
        """
        Compute normalised market signal from microstructure features.

        Market signal = average of normalised:
        - price_change_pct (directional indicator)
        - VWAP deviation (mean reversion signal)
        - buy_sell_ratio deviation from 0.5 (order flow imbalance)
        """
        df = self._z_score_normalize(df, "price_change_pct")
        df = self._z_score_normalize(df, "buy_sell_ratio")

        # VWAP deviation: how far close is from VWAP
        df = df.withColumn(
            "vwap_deviation",
            when(col("vwap") > 0,
                 (col("close") - col("vwap")) / col("vwap") * 100)
            .otherwise(lit(0.0)),
        )
        df = self._z_score_normalize(df, "vwap_deviation")

        # Aggregate into a single market signal
        df = df.withColumn(
            "market_signal",
            (
                col("price_change_pct_normalised") * lit(0.4)
                + col("vwap_deviation_normalised") * lit(0.3)
                + col("buy_sell_ratio_normalised") * lit(0.3)
            ),
        )

        return df

    # ─── Sentiment Signal Computation ─────────────────────────────────────────

    def compute_sentiment_signal(self, df: DataFrame) -> DataFrame:
        """
        Compute normalised sentiment signal with decay for stale signals.

        If no articles were published recently, the sentiment signal
        decays toward zero using an exponential factor.
        """
        ts_window = Window.partitionBy("symbol").orderBy("window_start")

        # Sentiment momentum: change from previous period
        df = df.withColumn(
            "sentiment_momentum",
            col("sentiment_score") - coalesce(
                lag("sentiment_score", 1).over(ts_window),
                lit(0.0),
            ),
        )

        # Apply decay when no articles are present
        df = df.withColumn(
            "sentiment_signal",
            when(col("article_count") > 0,
                 col("sentiment_score") * col("sentiment_confidence"))
            .otherwise(
                lag("sentiment_score", 1).over(ts_window) * lit(self.config.sentiment_decay)
            ),
        )

        # Normalize
        df = self._z_score_normalize(df, "sentiment_signal")

        return df

    # ─── Time-Series Signal Computation ───────────────────────────────────────

    def compute_timeseries_signal(self, df: DataFrame) -> DataFrame:
        """
        Compute time-series momentum signal.

        Combines:
        - EMA crossover (short vs long period)
        - Price momentum (current vs N-period ago)
        - Volatility trend (expanding vs contracting)
        """
        ts_window = Window.partitionBy("symbol").orderBy("window_start")
        ema_short = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-4, 0)
        ema_long = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-19, 0)

        df = (
            df
            # EMA crossover
            .withColumn("ema_short", spark_avg("close").over(ema_short))
            .withColumn("ema_long", spark_avg("close").over(ema_long))
            .withColumn(
                "ema_crossover",
                when(col("ema_long") > 0,
                     (col("ema_short") - col("ema_long")) / col("ema_long") * 100)
                .otherwise(lit(0.0)),
            )
            # Price momentum (5-period)
            .withColumn("close_lag5", lag("close", 5).over(ts_window))
            .withColumn(
                "price_momentum",
                when(col("close_lag5") > 0,
                     (col("close") - col("close_lag5")) / col("close_lag5") * 100)
                .otherwise(lit(0.0)),
            )
            .drop("close_lag5")
            # Volatility trend
            .withColumn("prev_volatility", lag("volatility", 1).over(ts_window))
            .withColumn(
                "volatility_trend",
                when(col("prev_volatility") > 0,
                     (col("volatility") - col("prev_volatility")) / col("prev_volatility"))
                .otherwise(lit(0.0)),
            )
            .drop("prev_volatility")
        )

        # Normalize components
        df = self._z_score_normalize(df, "ema_crossover")
        df = self._z_score_normalize(df, "price_momentum")

        # Combine into timeseries signal
        df = df.withColumn(
            "timeseries_signal",
            col("ema_crossover_normalised") * lit(0.5)
            + col("price_momentum_normalised") * lit(0.3)
            + col("volatility_trend") * lit(0.2),
        )

        return df

    # ─── Weighted Fusion ──────────────────────────────────────────────────────

    def fuse(
        self,
        features_df: DataFrame | None = None,
        sentiment_df: DataFrame | None = None,
    ) -> DataFrame:
        """
        Execute the full weighted fusion pipeline.

        Returns a DataFrame with all original features plus:
        - market_signal (normalised)
        - sentiment_signal (normalised, with decay)
        - timeseries_signal (normalised)
        - weighted_composite_signal (final fused signal)
        - signal_strength (absolute magnitude of composite)
        - signal_direction (UP / DOWN based on composite sign)
        """
        # Load data
        if features_df is None:
            features_df = self.load_market_features()
        if sentiment_df is None:
            sentiment_df = self.load_sentiment()

        # Join features with sentiment
        fused = features_df.join(
            sentiment_df,
            on=["symbol", "window_start"],
            how="left",
        )

        # Fill nulls for missing sentiment
        fused = fused.na.fill({
            "sentiment_score": 0.0,
            "sentiment_confidence": 0.5,
            "article_count": 0,
        })

        logger.info(
            "fusing_signals",
            market_weight=self.config.market_weight,
            sentiment_weight=self.config.sentiment_weight,
            timeseries_weight=self.config.timeseries_weight,
        )

        # Compute individual signals
        fused = self.compute_market_signal(fused)
        fused = self.compute_sentiment_signal(fused)
        fused = self.compute_timeseries_signal(fused)

        # ── Weighted composite ──
        wm = self.config.market_weight
        ws = self.config.sentiment_weight
        wt = self.config.timeseries_weight

        fused = (
            fused
            .withColumn(
                "weighted_composite_signal",
                (
                    lit(wm) * col("market_signal")
                    + lit(ws) * col("sentiment_signal_normalised")
                    + lit(wt) * col("timeseries_signal")
                ),
            )
            .withColumn(
                "signal_strength",
                spark_abs(col("weighted_composite_signal")),
            )
            .withColumn(
                "signal_direction",
                when(col("weighted_composite_signal") > 0.1, lit("UP"))
                .when(col("weighted_composite_signal") < -0.1, lit("DOWN"))
                .otherwise(lit("NEUTRAL")),
            )
        )

        # Drop nulls from lag operations
        fused = fused.na.fill(0.0)

        logger.info("signal_fusion_complete", total_rows=fused.count())
        return fused

    # ─── Persist Fused Signals ────────────────────────────────────────────────

    def save_to_delta(self, fused_df: DataFrame) -> None:
        """Write fused signals to Delta Lake for downstream training."""
        (
            fused_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("symbol")
            .save(self.config.output_path)
        )
        logger.info("fused_signals_saved", path=self.config.output_path)


# =============================================================================
# Convenience Entrypoint
# =============================================================================

def run_fusion(
    spark: SparkSession | None = None,
    config: FusionConfig | None = None,
    save: bool = True,
) -> DataFrame:
    """
    Run the full signal fusion pipeline and optionally save to Delta Lake.

    Usage:
        fused_df = run_fusion(spark=spark)
    """
    pipeline = SignalFusionPipeline(spark=spark, config=config)
    fused_df = pipeline.fuse()

    if save:
        pipeline.save_to_delta(fused_df)

    return fused_df


if __name__ == "__main__":
    fused = run_fusion()
    print(f"\n✅ Signal fusion complete: {fused.count()} rows")
    fused.select(
        "symbol", "window_start",
        "market_signal", "sentiment_signal_normalised", "timeseries_signal",
        "weighted_composite_signal", "signal_direction",
    ).show(20, truncate=False)
