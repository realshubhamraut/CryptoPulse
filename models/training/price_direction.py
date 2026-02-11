"""
CryptoPulse - Price Direction Prediction Model

Gradient Boosted Trees classifier for predicting short-term price direction.
Combines market microstructure features with sentiment signals.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
)

try:
    import lightgbm as lgb
    HAS_LIGHTGBM = True
except ImportError:
    import xgboost as xgb
    HAS_LIGHTGBM = False

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="price_direction_model")


# =============================================================================
# Feature Configuration
# =============================================================================

MARKET_FEATURES = [
    # OHLC-derived
    "price_change_pct",
    "high_low_range",
    
    # Volume
    "volume",
    "quote_volume",
    "buy_sell_ratio",
    "trade_intensity",
    
    # VWAP
    "vwap_deviation",  # (close - vwap) / vwap
    
    # Volatility
    "volatility",
    
    # Lagged features (computed during feature engineering)
    "price_change_pct_lag1",
    "price_change_pct_lag2",
    "price_change_pct_lag3",
    "volume_change_pct",
    "volatility_change",
]

SENTIMENT_FEATURES = [
    "sentiment_score",
    "sentiment_confidence",
    "article_count",
    "sentiment_momentum",  # change in sentiment over time
]


@dataclass
class ModelConfig:
    """Configuration for price direction model."""
    
    prediction_horizon_minutes: int = 5
    min_price_change_threshold: float = 0.1  # % change to classify as up/down
    test_size: float = 0.2
    random_state: int = 42
    
    # LightGBM / XGBoost parameters
    n_estimators: int = 200
    max_depth: int = 6
    learning_rate: float = 0.05
    num_leaves: int = 31
    min_child_samples: int = 20
    subsample: float = 0.8
    colsample_bytree: float = 0.8


class PriceDirectionModel:
    """
    Price direction prediction model.
    
    Predicts whether price will go UP, DOWN, or stay NEUTRAL
    in the next prediction_horizon_minutes.
    """
    
    def __init__(self, config: ModelConfig | None = None):
        self.config = config or ModelConfig()
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.feature_names: list[str] = []
        self._is_fitted = False
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare feature matrix from raw data.
        
        Args:
            df: DataFrame with market features and sentiment
            
        Returns:
            DataFrame with engineered features
        """
        features = df.copy()
        
        # VWAP deviation
        if "vwap" in features.columns and "close" in features.columns:
            features["vwap_deviation"] = (
                (features["close"] - features["vwap"]) / features["vwap"] * 100
            )
        
        # Lagged price changes
        for lag in [1, 2, 3]:
            if "price_change_pct" in features.columns:
                features[f"price_change_pct_lag{lag}"] = features.groupby("symbol")[
                    "price_change_pct"
                ].shift(lag)
        
        # Volume change
        if "volume" in features.columns:
            features["volume_change_pct"] = features.groupby("symbol")["volume"].pct_change() * 100
        
        # Volatility change
        if "volatility" in features.columns:
            features["volatility_change"] = features.groupby("symbol")["volatility"].diff()
        
        # Sentiment momentum (if available)
        if "sentiment_score" in features.columns:
            features["sentiment_momentum"] = features.groupby("symbol")[
                "sentiment_score"
            ].diff()
        
        return features
    
    def create_labels(
        self,
        df: pd.DataFrame,
        price_col: str = "close",
        horizon: int = None,
    ) -> pd.Series:
        """
        Create target labels based on future price movement.
        
        Args:
            df: DataFrame with price data
            price_col: Column containing price
            horizon: Prediction horizon in rows
            
        Returns:
            Series with labels (UP, DOWN, NEUTRAL)
        """
        horizon = horizon or self.config.prediction_horizon_minutes
        threshold = self.config.min_price_change_threshold
        
        # Future price change
        future_price = df.groupby("symbol")[price_col].shift(-horizon)
        current_price = df[price_col]
        
        pct_change = (future_price - current_price) / current_price * 100
        
        # Classify
        labels = pd.Series(index=df.index, dtype=str)
        labels[pct_change > threshold] = "UP"
        labels[pct_change < -threshold] = "DOWN"
        labels[(pct_change >= -threshold) & (pct_change <= threshold)] = "NEUTRAL"
        
        return labels
    
    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        validation_data: tuple[pd.DataFrame, pd.Series] | None = None,
    ) -> dict[str, Any]:
        """
        Train the model.
        
        Args:
            X: Feature matrix
            y: Target labels
            validation_data: Optional (X_val, y_val) tuple
            
        Returns:
            Dictionary with training metrics
        """
        logger.info("training_price_direction_model", samples=len(X))
        
        # Store feature names
        self.feature_names = list(X.columns)
        
        # Encode labels
        y_encoded = self.label_encoder.fit_transform(y)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Split if no validation data provided
        if validation_data is None:
            X_train, X_val, y_train, y_val = train_test_split(
                X_scaled, y_encoded,
                test_size=self.config.test_size,
                random_state=self.config.random_state,
                stratify=y_encoded,
            )
        else:
            X_train, y_train = X_scaled, y_encoded
            X_val = self.scaler.transform(validation_data[0])
            y_val = self.label_encoder.transform(validation_data[1])
        
        # Train model
        if HAS_LIGHTGBM:
            self.model = lgb.LGBMClassifier(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                num_leaves=self.config.num_leaves,
                min_child_samples=self.config.min_child_samples,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                random_state=self.config.random_state,
                verbose=-1,
            )
        else:
            self.model = xgb.XGBClassifier(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                random_state=self.config.random_state,
                use_label_encoder=False,
                eval_metric="mlogloss",
            )
        
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
        )
        
        self._is_fitted = True
        
        # Evaluate
        y_pred = self.model.predict(X_val)
        
        metrics = {
            "accuracy": accuracy_score(y_val, y_pred),
            "precision_macro": precision_score(y_val, y_pred, average="macro"),
            "recall_macro": recall_score(y_val, y_pred, average="macro"),
            "f1_macro": f1_score(y_val, y_pred, average="macro"),
            "train_samples": len(X_train),
            "val_samples": len(X_val),
            "classes": list(self.label_encoder.classes_),
        }
        
        logger.info("model_training_complete", **metrics)
        
        return metrics
    
    def predict(self, X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """
        Make predictions with confidence scores.
        
        Args:
            X: Feature matrix
            
        Returns:
            Tuple of (predictions, probabilities)
        """
        if not self._is_fitted:
            raise RuntimeError("Model must be fitted before prediction")
        
        X_scaled = self.scaler.transform(X[self.feature_names])
        
        predictions = self.model.predict(X_scaled)
        probabilities = self.model.predict_proba(X_scaled)
        
        # Decode labels
        labels = self.label_encoder.inverse_transform(predictions)
        
        return labels, probabilities
    
    def predict_single(
        self,
        features: dict[str, float],
    ) -> dict[str, Any]:
        """
        Make prediction for a single observation.
        
        Args:
            features: Dictionary of feature values
            
        Returns:
            Prediction result with probabilities
        """
        X = pd.DataFrame([features])[self.feature_names]
        labels, probs = self.predict(X)
        
        prob_dict = dict(zip(self.label_encoder.classes_, probs[0]))
        
        return {
            "direction": labels[0],
            "confidence": float(max(probs[0])),
            "probabilities": prob_dict,
        }
    
    def feature_importance(self) -> pd.DataFrame:
        """Get feature importance rankings."""
        if not self._is_fitted:
            raise RuntimeError("Model must be fitted first")
        
        importance = self.model.feature_importances_
        
        return (
            pd.DataFrame({
                "feature": self.feature_names,
                "importance": importance,
            })
            .sort_values("importance", ascending=False)
            .reset_index(drop=True)
        )
    
    def save(self, path: str | Path) -> None:
        """Save model to disk."""
        import joblib
        
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        joblib.dump({
            "model": self.model,
            "scaler": self.scaler,
            "label_encoder": self.label_encoder,
            "feature_names": self.feature_names,
            "config": self.config,
        }, path / "model.joblib")
        
        logger.info("model_saved", path=str(path))
    
    @classmethod
    def load(cls, path: str | Path) -> "PriceDirectionModel":
        """Load model from disk."""
        import joblib
        
        data = joblib.load(Path(path) / "model.joblib")
        
        model = cls(config=data["config"])
        model.model = data["model"]
        model.scaler = data["scaler"]
        model.label_encoder = data["label_encoder"]
        model.feature_names = data["feature_names"]
        model._is_fitted = True
        
        logger.info("model_loaded", path=str(path))
        
        return model


# =============================================================================
# Training Pipeline
# =============================================================================

def train_from_delta(
    spark,
    features_path: str,
    sentiment_path: str | None = None,
    model_output_path: str | None = None,
) -> PriceDirectionModel:
    """
    Train model from Delta Lake feature tables.
    
    Args:
        spark: SparkSession
        features_path: Path to Gold features Delta table
        sentiment_path: Optional path to sentiment data
        model_output_path: Path to save trained model
        
    Returns:
        Trained PriceDirectionModel
    """
    logger.info("loading_training_data", features_path=features_path)
    
    # Load features
    features_df = spark.read.format("delta").load(features_path).toPandas()
    
    # Optionally join sentiment
    if sentiment_path:
        sentiment_df = spark.read.format("delta").load(sentiment_path).toPandas()
        # Aggregate sentiment by symbol and window
        sentiment_agg = sentiment_df.groupby(["symbol", "window_start"]).agg({
            "sentiment_score": "mean",
            "sentiment_confidence": "mean",
            "article_id": "count",
        }).rename(columns={"article_id": "article_count"}).reset_index()
        
        features_df = features_df.merge(
            sentiment_agg,
            on=["symbol", "window_start"],
            how="left",
        )
        features_df["sentiment_score"] = features_df["sentiment_score"].fillna(0)
        features_df["sentiment_confidence"] = features_df["sentiment_confidence"].fillna(0.5)
        features_df["article_count"] = features_df["article_count"].fillna(0)
    
    # Prepare model
    model = PriceDirectionModel()
    
    # Prepare features
    features_df = model.prepare_features(features_df)
    
    # Create labels
    features_df["label"] = model.create_labels(features_df)
    
    # Drop rows with NaN
    features_df = features_df.dropna()
    
    # Select feature columns
    feature_cols = [c for c in MARKET_FEATURES + SENTIMENT_FEATURES if c in features_df.columns]
    
    X = features_df[feature_cols]
    y = features_df["label"]
    
    # Train
    metrics = model.fit(X, y)
    
    # Save
    if model_output_path:
        model.save(model_output_path)
    
    return model


if __name__ == "__main__":
    # Example usage with synthetic data
    np.random.seed(42)
    n_samples = 1000
    
    # Generate synthetic features
    data = pd.DataFrame({
        "symbol": ["BTCUSDT"] * n_samples,
        "close": np.cumsum(np.random.randn(n_samples)) + 50000,
        "price_change_pct": np.random.randn(n_samples) * 0.5,
        "high_low_range": np.abs(np.random.randn(n_samples)) * 0.3,
        "volume": np.random.exponential(100, n_samples),
        "quote_volume": np.random.exponential(5000000, n_samples),
        "buy_sell_ratio": np.random.beta(5, 5, n_samples),
        "trade_intensity": np.random.exponential(10, n_samples),
        "volatility": np.abs(np.random.randn(n_samples)) * 0.5,
        "vwap": np.cumsum(np.random.randn(n_samples)) + 50000,
    })
    
    model = PriceDirectionModel()
    features = model.prepare_features(data)
    features["label"] = model.create_labels(features, horizon=1)
    features = features.dropna()
    
    feature_cols = ["price_change_pct", "high_low_range", "volume", "buy_sell_ratio",
                    "trade_intensity", "volatility", "vwap_deviation"]
    feature_cols = [c for c in feature_cols if c in features.columns]
    
    metrics = model.fit(features[feature_cols], features["label"])
    print(f"Model metrics: {metrics}")
    print(f"\nFeature importance:\n{model.feature_importance()}")
