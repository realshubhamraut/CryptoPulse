"""
CryptoPulse - Anomaly Detection Model

Isolation Forest for detecting unusual trading patterns:
- Pump/dump schemes
- Wash trading
- Volume spikes
- Price manipulation
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="anomaly_detection_model")


# =============================================================================
# Anomaly Features
# =============================================================================

ANOMALY_FEATURES = [
    # Volume anomalies
    "volume_zscore",           # Z-score of volume vs rolling mean
    "trade_count_zscore",      # Z-score of trade count
    
    # Price anomalies
    "price_change_zscore",     # Z-score of price change
    "high_low_range_zscore",   # Z-score of range
    
    # Order flow anomalies
    "buy_sell_imbalance",      # Extreme buy/sell ratios
    
    # Trade pattern anomalies
    "trade_size_uniformity",   # Measure of trade size uniformity (wash indicator)
    "trade_frequency_spike",   # Sudden increase in trading
]


@dataclass
class AnomalyConfig:
    """Configuration for anomaly detection model."""
    
    contamination: float = 0.01  # Expected proportion of anomalies
    n_estimators: int = 100
    max_samples: int = 256
    random_state: int = 42
    
    # Feature calculation parameters
    rolling_window: int = 60  # Minutes for rolling statistics
    zscore_threshold: float = 3.0
    
    # Severity thresholds
    high_severity_threshold: float = -0.5
    medium_severity_threshold: float = -0.3


class AnomalyType:
    """Anomaly type classification."""
    
    PUMP = "pump"
    DUMP = "dump"
    WASH_TRADE = "wash_trade"
    VOLUME_SPIKE = "volume_spike"
    PRICE_MANIPULATION = "price_manipulation"
    UNKNOWN = "unknown"


class AnomalyDetectionModel:
    """
    Isolation Forest-based anomaly detection for trading patterns.
    
    Detects:
    - Volume spikes without corresponding news/events
    - Unusual price movements
    - Wash trading patterns (uniform trade sizes)
    - Pump and dump schemes
    """
    
    def __init__(self, config: AnomalyConfig | None = None):
        self.config = config or AnomalyConfig()
        self.model = IsolationForest(
            contamination=self.config.contamination,
            n_estimators=self.config.n_estimators,
            max_samples=self.config.max_samples,
            random_state=self.config.random_state,
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        self.feature_stats: dict[str, dict] = {}
        self._is_fitted = False
    
    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute anomaly detection features from raw trade data.
        
        Args:
            df: DataFrame with market features
            
        Returns:
            DataFrame with anomaly features
        """
        features = df.copy()
        window = self.config.rolling_window
        
        # Group by symbol for rolling calculations
        for col in ["volume", "trade_count", "price_change_pct", "high_low_range"]:
            if col in features.columns:
                # Rolling mean and std
                roll = features.groupby("symbol")[col].transform(
                    lambda x: x.rolling(window=window, min_periods=1)
                )
                roll_mean = roll.mean()
                roll_std = roll.std()
                
                # Z-score
                features[f"{col}_zscore"] = (features[col] - roll_mean) / (roll_std + 1e-8)
        
        # Buy/sell imbalance
        if "buy_sell_ratio" in features.columns:
            features["buy_sell_imbalance"] = np.abs(features["buy_sell_ratio"] - 0.5) * 2
        
        # Trade size uniformity (coefficient of variation of trade sizes)
        # Low CV suggests wash trading
        if "volume" in features.columns and "trade_count" in features.columns:
            avg_trade_size = features["volume"] / (features["trade_count"] + 1)
            size_std = features.groupby("symbol")["volume"].transform(
                lambda x: x.rolling(window=window, min_periods=1).std()
            )
            features["trade_size_uniformity"] = 1 - (size_std / (avg_trade_size + 1e-8)).clip(0, 1)
        
        # Trade frequency spike
        if "trade_count" in features.columns:
            baseline = features.groupby("symbol")["trade_count"].transform(
                lambda x: x.rolling(window=window*2, min_periods=1).mean()
            )
            features["trade_frequency_spike"] = (
                features["trade_count"] / (baseline + 1)
            ).clip(0, 10)
        
        return features
    
    def fit(self, X: pd.DataFrame) -> dict[str, Any]:
        """
        Train the anomaly detection model.
        
        Args:
            X: Feature matrix
            
        Returns:
            Dictionary with training info
        """
        logger.info("training_anomaly_model", samples=len(X))
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Fit Isolation Forest
        self.model.fit(X_scaled)
        
        # Store feature statistics for interpretability
        self.feature_stats = {
            col: {
                "mean": float(X[col].mean()),
                "std": float(X[col].std()),
                "q95": float(X[col].quantile(0.95)),
                "q99": float(X[col].quantile(0.99)),
            }
            for col in X.columns
        }
        
        self._is_fitted = True
        
        # Score training data to estimate anomaly rate
        scores = self.model.decision_function(X_scaled)
        predictions = self.model.predict(X_scaled)
        
        n_anomalies = (predictions == -1).sum()
        
        info = {
            "samples_trained": len(X),
            "detected_anomalies": int(n_anomalies),
            "anomaly_rate": float(n_anomalies / len(X)),
            "avg_score": float(scores.mean()),
            "score_std": float(scores.std()),
        }
        
        logger.info("anomaly_model_trained", **info)
        
        return info
    
    def predict(
        self,
        X: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Detect anomalies.
        
        Args:
            X: Feature matrix
            
        Returns:
            Tuple of (is_anomaly, severity_score, anomaly_score)
        """
        if not self._is_fitted:
            raise RuntimeError("Model must be fitted before prediction")
        
        X_scaled = self.scaler.transform(X)
        
        # Get anomaly scores (negative = more anomalous)
        scores = self.model.decision_function(X_scaled)
        
        # Predict (-1 = anomaly, 1 = normal)
        predictions = self.model.predict(X_scaled)
        
        is_anomaly = predictions == -1
        
        # Convert scores to severity (0-1 scale)
        # More negative scores = higher severity
        severity = np.clip(-scores, 0, 1)
        
        return is_anomaly, severity, scores
    
    def classify_anomaly(
        self,
        features: dict[str, float],
        severity: float,
    ) -> tuple[str, str]:
        """
        Classify the type of anomaly based on feature patterns.
        
        Args:
            features: Feature values
            severity: Anomaly severity score
            
        Returns:
            Tuple of (anomaly_type, description)
        """
        # Volume spike without price movement
        if (features.get("volume_zscore", 0) > 3 and 
            abs(features.get("price_change_zscore", 0)) < 1):
            return AnomalyType.WASH_TRADE, "High volume with minimal price impact"
        
        # Extreme price increase with volume
        if (features.get("price_change_zscore", 0) > 3 and
            features.get("volume_zscore", 0) > 2):
            return AnomalyType.PUMP, "Rapid price increase with high volume"
        
        # Extreme price decrease
        if (features.get("price_change_zscore", 0) < -3 and
            features.get("volume_zscore", 0) > 2):
            return AnomalyType.DUMP, "Rapid price decrease with high volume"
        
        # High trade uniformity (wash trading indicator)
        if features.get("trade_size_uniformity", 0) > 0.9:
            return AnomalyType.WASH_TRADE, "Suspiciously uniform trade sizes"
        
        # Extreme buy/sell imbalance
        if features.get("buy_sell_imbalance", 0) > 0.8:
            return AnomalyType.PRICE_MANIPULATION, "One-sided order flow"
        
        # Volume spike
        if features.get("volume_zscore", 0) > 4:
            return AnomalyType.VOLUME_SPIKE, "Unusual volume activity"
        
        return AnomalyType.UNKNOWN, "Unclassified anomaly pattern"
    
    def detect_and_classify(
        self,
        df: pd.DataFrame,
        feature_cols: list[str],
    ) -> pd.DataFrame:
        """
        Run full detection and classification pipeline.
        
        Args:
            df: DataFrame with features
            feature_cols: List of feature column names
            
        Returns:
            DataFrame with anomaly flags and classifications
        """
        X = df[feature_cols]
        
        is_anomaly, severity, scores = self.predict(X)
        
        results = df.copy()
        results["is_anomaly"] = is_anomaly
        results["anomaly_severity"] = severity
        results["anomaly_score"] = scores
        
        # Classify anomalies
        anomaly_types = []
        descriptions = []
        
        for idx, row in results.iterrows():
            if row["is_anomaly"]:
                features = {col: row[col] for col in feature_cols}
                atype, desc = self.classify_anomaly(features, row["anomaly_severity"])
                anomaly_types.append(atype)
                descriptions.append(desc)
            else:
                anomaly_types.append(None)
                descriptions.append(None)
        
        results["anomaly_type"] = anomaly_types
        results["anomaly_description"] = descriptions
        
        return results
    
    def save(self, path: str | Path) -> None:
        """Save model to disk."""
        import joblib
        
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        joblib.dump({
            "model": self.model,
            "scaler": self.scaler,
            "feature_stats": self.feature_stats,
            "config": self.config,
        }, path / "anomaly_model.joblib")
        
        logger.info("anomaly_model_saved", path=str(path))
    
    @classmethod
    def load(cls, path: str | Path) -> "AnomalyDetectionModel":
        """Load model from disk."""
        import joblib
        
        data = joblib.load(Path(path) / "anomaly_model.joblib")
        
        model = cls(config=data["config"])
        model.model = data["model"]
        model.scaler = data["scaler"]
        model.feature_stats = data["feature_stats"]
        model._is_fitted = True
        
        logger.info("anomaly_model_loaded", path=str(path))
        
        return model


if __name__ == "__main__":
    # Example with synthetic data
    np.random.seed(42)
    n_samples = 1000
    
    # Generate normal trading data
    data = pd.DataFrame({
        "symbol": ["BTCUSDT"] * n_samples,
        "volume": np.random.exponential(100, n_samples),
        "trade_count": np.random.poisson(50, n_samples),
        "price_change_pct": np.random.randn(n_samples) * 0.5,
        "high_low_range": np.abs(np.random.randn(n_samples)) * 0.3,
        "buy_sell_ratio": np.random.beta(5, 5, n_samples),
    })
    
    # Inject some anomalies
    data.loc[10:15, "volume"] = 1000  # Volume spike
    data.loc[50:55, "price_change_pct"] = 5  # Pump
    data.loc[100:105, "buy_sell_ratio"] = 0.95  # One-sided
    
    model = AnomalyDetectionModel()
    features = model.compute_features(data)
    
    feature_cols = ["volume_zscore", "price_change_pct_zscore", "buy_sell_imbalance",
                    "trade_size_uniformity", "trade_frequency_spike"]
    feature_cols = [c for c in feature_cols if c in features.columns]
    
    features = features.dropna(subset=feature_cols)
    
    model.fit(features[feature_cols])
    
    results = model.detect_and_classify(features, feature_cols)
    
    anomalies = results[results["is_anomaly"]]
    print(f"Detected {len(anomalies)} anomalies")
    print(anomalies[["symbol", "anomaly_type", "anomaly_severity", "anomaly_description"]].head(10))
