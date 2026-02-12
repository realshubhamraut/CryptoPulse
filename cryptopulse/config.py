"""
CryptoPulse - Core Configuration Module

Centralized configuration management using Pydantic Settings.
Supports both local development and Azure deployment.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BinanceConfig(BaseSettings):
    """Binance API configuration."""

    model_config = SettingsConfigDict(env_prefix="BINANCE_")

    api_key: str = Field(default="", description="Binance API key")
    api_secret: str = Field(default="", description="Binance API secret")
    websocket_url: str = Field(
        default="wss://stream.binance.com:9443/ws",
        description="Binance WebSocket endpoint",
    )
    rest_url: str = Field(
        default="https://api.binance.com",
        description="Binance REST API endpoint",
    )
    trading_pairs: str = Field(
        default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,DOTUSDT,MATICUSDT,LINKUSDT,UNIUSDT,ATOMUSDT,LTCUSDT,ETCUSDT,XLMUSDT,NEARUSDT,APTUSDT,FILUSDT,ARBUSDT",
        description="Comma-separated trading pairs to track",
    )
    rate_limit_per_minute: int = Field(
        default=1200,
        description="Binance rate limit per minute",
    )

    @property
    def trading_pair_list(self) -> list[str]:
        """Get trading pairs as a list."""
        return [p.strip().upper() for p in self.trading_pairs.split(",")]


class KafkaConfig(BaseSettings):
    """Kafka configuration for local development."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers",
    )
    trades_topic: str = Field(default="trades", description="Trades topic")
    news_topic: str = Field(default="news", description="News topic")
    features_topic: str = Field(default="features", description="Features topic")
    predictions_topic: str = Field(default="predictions", description="Predictions topic")
    consumer_group: str = Field(
        default="cryptopulse-consumer",
        description="Consumer group ID",
    )


class AzureEventHubConfig(BaseSettings):
    """Azure Event Hubs configuration."""

    model_config = SettingsConfigDict(env_prefix="AZURE_EVENTHUB_")

    namespace: str = Field(default="", description="Event Hub namespace FQDN")
    connection_string: str = Field(default="", description="Connection string")
    trades_name: str = Field(default="trades", description="Trades hub name")
    news_name: str = Field(default="news", description="News hub name")
    features_name: str = Field(default="features", description="Features hub name")
    predictions_name: str = Field(default="predictions", description="Predictions hub name")


class StorageConfig(BaseSettings):
    """Storage configuration for Delta Lake and ADLS Gen2."""

    model_config = SettingsConfigDict(env_prefix="")

    # Local development (MinIO)
    minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")

    # Azure (ADLS Gen2)
    azure_storage_account_name: str = Field(
        default="", alias="AZURE_STORAGE_ACCOUNT_NAME"
    )
    azure_storage_account_key: str = Field(
        default="", alias="AZURE_STORAGE_ACCOUNT_KEY"
    )
    azure_storage_container_name: str = Field(
        default="cryptopulse-delta", alias="AZURE_STORAGE_CONTAINER_NAME"
    )
    azure_storage_connection_string: str = Field(
        default="", alias="AZURE_STORAGE_CONNECTION_STRING"
    )

    # Delta Lake paths
    delta_lake_path: str = Field(
        default="/data/delta",
        alias="DELTA_LAKE_PATH",
        description="Base path for Delta tables (overridden by ADLS when configured)",
    )

    @property
    def is_azure(self) -> bool:
        """Check if Azure ADLS Gen2 storage is configured."""
        return bool(self.azure_storage_account_name)

    @property
    def adls_delta_path(self) -> str:
        """
        Resolve Delta Lake path to ADLS Gen2 abfss:// URL.

        When Azure credentials are present, returns:
            abfss://{container}@{account}.dfs.core.windows.net/delta

        Otherwise falls back to local delta_lake_path.

        Resume claim: "Delta Lake on ADLS Gen2"
        """
        if self.is_azure:
            return (
                f"abfss://{self.azure_storage_container_name}"
                f"@{self.azure_storage_account_name}.dfs.core.windows.net/delta"
            )
        return self.delta_lake_path

    @property
    def adls_model_path(self) -> str:
        """Resolve model artifact storage path for ADLS Gen2."""
        if self.is_azure:
            return (
                f"abfss://{self.azure_storage_container_name}"
                f"@{self.azure_storage_account_name}.dfs.core.windows.net/models"
            )
        return f"{self.delta_lake_path}/models"


class SparkConfig(BaseSettings):
    """Spark configuration."""

    model_config = SettingsConfigDict(env_prefix="SPARK_")

    master_url: str = Field(
        default="spark://localhost:7077",
        description="Spark master URL",
    )
    app_name: str = Field(default="CryptoPulse", description="Spark application name")
    executor_memory: str = Field(default="4g", description="Executor memory")
    executor_cores: int = Field(default=2, description="Executor cores")
    driver_memory: str = Field(default="2g", description="Driver memory")


class MLConfig(BaseSettings):
    """ML configuration."""

    model_config = SettingsConfigDict(env_prefix="")

    mlflow_tracking_uri: str = Field(
        default="http://localhost:5000",
        alias="MLFLOW_TRACKING_URI",
    )
    mlflow_experiment_name: str = Field(
        default="cryptopulse",
        alias="MLFLOW_EXPERIMENT_NAME",
    )
    model_price_direction_path: str = Field(
        default="models/artifacts/price_direction",
        alias="MODEL_PRICE_DIRECTION_PATH",
    )
    model_anomaly_detection_path: str = Field(
        default="models/artifacts/anomaly_detection",
        alias="MODEL_ANOMALY_DETECTION_PATH",
    )
    prediction_horizon_minutes: int = Field(
        default=5,
        alias="PREDICTION_HORIZON_MINUTES",
    )
    prediction_confidence_threshold: float = Field(
        default=0.6,
        alias="PREDICTION_CONFIDENCE_THRESHOLD",
    )


class APIConfig(BaseSettings):
    """REST API configuration."""

    model_config = SettingsConfigDict(env_prefix="API_")

    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")
    workers: int = Field(default=4, description="Number of workers")
    reload: bool = Field(default=True, description="Enable auto-reload")
    rate_limit_per_minute: int = Field(
        default=100,
        description="Rate limit per minute",
    )
    api_keys: list[str] = Field(
        default=[],
        description="Valid API keys for authentication",
    )

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [key.strip() for key in v.split(",") if key.strip()]
        return v


class RedisConfig(BaseSettings):
    """Redis configuration."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL",
    )


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    env: Literal["development", "staging", "production"] = Field(
        default="development",
        alias="CRYPTOPULSE_ENV",
    )
    debug: bool = Field(default=True, alias="CRYPTOPULSE_DEBUG")
    log_level: str = Field(default="INFO", alias="CRYPTOPULSE_LOG_LEVEL")

    # Sub-configurations
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    azure_eventhub: AzureEventHubConfig = Field(default_factory=AzureEventHubConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)

    @property
    def is_local(self) -> bool:
        """Check if running in local development mode."""
        return self.env == "development"

    @property
    def is_azure(self) -> bool:
        """Check if Azure configuration is available."""
        return bool(self.azure_eventhub.connection_string)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience alias
settings = get_settings()
