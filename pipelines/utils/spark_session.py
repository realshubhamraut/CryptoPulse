"""
CryptoPulse - Spark Session Factory

Creates properly configured SparkSession instances with:
- Delta Lake extensions
- ADLS Gen2 credentials (when running on Azure/Databricks)
- Local/MinIO storage (when running locally)

This ensures all pipelines share the same Azure credential
configuration for reading/writing Delta tables on ADLS Gen2.

Resume claim: "event-driven pipelines on Azure Databricks"
"""

from pyspark.sql import SparkSession

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="spark_factory")


def create_spark_session(
    app_name: str = "CryptoPulse",
    enable_delta: bool = True,
    enable_adls: bool = True,
    extra_config: dict[str, str] | None = None,
) -> SparkSession:
    """
    Create a SparkSession with Delta Lake and ADLS Gen2 support.

    When Azure credentials are configured, adds:
    - spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net
    - spark.hadoop.fs.azure.account.auth.type (SharedKey)

    This allows Spark to read/write Delta tables from:
        abfss://<container>@<account>.dfs.core.windows.net/...

    Args:
        app_name: Spark application name
        enable_delta: Enable Delta Lake SQL extensions
        enable_adls: Enable ADLS Gen2 credentials (when Azure is configured)
        extra_config: Additional Spark config key-value pairs

    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    # ── Delta Lake ──
    if enable_delta:
        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0",
            )
        )

    # ── ADLS Gen2 credentials ──
    if enable_adls and settings.storage.is_azure:
        account = settings.storage.azure_storage_account_name
        key = settings.storage.azure_storage_account_key

        builder = (
            builder
            # Authentication type
            .config(
                f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net",
                "SharedKey",
            )
            # Account key
            .config(
                f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net",
                key,
            )
            # Enable ABFSS scheme
            .config(
                "spark.hadoop.fs.azure.impl",
                "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            )
            .config(
                "spark.hadoop.fs.abfss.impl",
                "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            )
        )

        logger.info(
            "spark_adls_configured",
            account=account,
            container=settings.storage.azure_storage_container_name,
        )

    # ── Extra config ──
    if extra_config:
        for k, v in extra_config.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()

    logger.info(
        "spark_session_created",
        app_name=app_name,
        adls_enabled=settings.storage.is_azure,
        delta_enabled=enable_delta,
    )

    return spark


def get_delta_path(layer: str, table: str) -> str:
    """
    Get the full Delta table path for a layer and table name.

    Returns abfss:// path on Azure, local path otherwise.

    Args:
        layer: Data layer (bronze, silver, gold)
        table: Table name (trades, news, features, sentiment)

    Returns:
        Full path like "abfss://container@account.dfs.core.windows.net/delta/bronze/trades"
        or "/data/delta/bronze/trades"

    Examples:
        >>> get_delta_path("bronze", "trades")
        "abfss://cryptopulse-delta@myaccount.dfs.core.windows.net/delta/bronze/trades"

        >>> get_delta_path("gold", "features")
        "/data/delta/gold/features"  # (when Azure not configured)
    """
    base = settings.storage.adls_delta_path
    return f"{base}/{layer}/{table}"


def get_checkpoint_path(pipeline_name: str) -> str:
    """
    Get the checkpoint path for a streaming pipeline.

    Args:
        pipeline_name: Pipeline identifier (e.g., "bronze_trades")

    Returns:
        Full checkpoint path on ADLS or local filesystem
    """
    base = settings.storage.adls_delta_path
    return f"{base}/checkpoints/{pipeline_name}"
