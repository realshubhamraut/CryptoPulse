"""
CryptoPulse - Azure Data Lake Storage Gen2 Client

Provides file operations on ADLS Gen2 for:
- ML model artifact storage and retrieval
- Pipeline output management
- Data lifecycle operations

Resume claim: "Lakehouse architecture with Delta Lake on ADLS Gen2"

Uses the Azure SDK's DataLakeServiceClient for hierarchical
namespace operations on ADLS Gen2.
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="adls_client")


class ADLSClient:
    """
    Azure Data Lake Storage Gen2 client.

    Wraps the azure.storage.filedatalake SDK to provide high-level
    operations for the CryptoPulse platform:
    - Upload/download model artifacts
    - List and manage Delta table files
    - Upload pipeline outputs
    """

    def __init__(
        self,
        account_name: str | None = None,
        account_key: str | None = None,
        connection_string: str | None = None,
        container_name: str | None = None,
    ):
        self.account_name = account_name or settings.storage.azure_storage_account_name
        self.account_key = account_key or settings.storage.azure_storage_account_key
        self.connection_string = (
            connection_string or settings.storage.azure_storage_connection_string
        )
        self.container_name = (
            container_name or settings.storage.azure_storage_container_name
        )
        self._service_client = None
        self._file_system_client = None

    # ─── Connection ───────────────────────────────────────────────────────────

    def connect(self) -> "ADLSClient":
        """
        Initialize the DataLakeServiceClient.

        Supports authentication via:
        1. Connection string (preferred for development)
        2. Account name + key (for programmatic access)
        3. DefaultAzureCredential (for managed identity on Azure)
        """
        from azure.storage.filedatalake import DataLakeServiceClient

        if self.connection_string:
            self._service_client = DataLakeServiceClient.from_connection_string(
                conn_str=self.connection_string,
            )
        elif self.account_name and self.account_key:
            self._service_client = DataLakeServiceClient(
                account_url=f"https://{self.account_name}.dfs.core.windows.net",
                credential=self.account_key,
            )
        else:
            # Fall back to Azure Default Credential (managed identity, CLI, etc.)
            from azure.identity import DefaultAzureCredential
            self._service_client = DataLakeServiceClient(
                account_url=f"https://{self.account_name}.dfs.core.windows.net",
                credential=DefaultAzureCredential(),
            )

        self._file_system_client = self._service_client.get_file_system_client(
            file_system=self.container_name,
        )

        logger.info(
            "adls_connected",
            account=self.account_name,
            container=self.container_name,
        )
        return self

    def _ensure_connected(self) -> None:
        """Ensure client is connected, auto-connect if not."""
        if not self._file_system_client:
            self.connect()

    # ─── File Operations ──────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        overwrite: bool = True,
    ) -> dict[str, Any]:
        """
        Upload a local file to ADLS Gen2.

        Args:
            local_path: Path to local file
            remote_path: Destination path in ADLS (e.g., "models/gbt_v1/model.pkl")
            overwrite: Whether to overwrite existing file

        Returns:
            Dict with upload metadata
        """
        self._ensure_connected()
        local_path = Path(local_path)

        file_client = self._file_system_client.get_file_client(remote_path)

        with open(local_path, "rb") as f:
            file_client.upload_data(f, overwrite=overwrite)

        logger.info(
            "file_uploaded",
            local=str(local_path),
            remote=remote_path,
            size_bytes=local_path.stat().st_size,
        )

        return {
            "remote_path": remote_path,
            "size_bytes": local_path.stat().st_size,
            "container": self.container_name,
            "url": f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{remote_path}",
        }

    def upload_bytes(
        self,
        data: bytes | BinaryIO,
        remote_path: str,
        overwrite: bool = True,
    ) -> dict[str, Any]:
        """Upload raw bytes to ADLS Gen2."""
        self._ensure_connected()

        file_client = self._file_system_client.get_file_client(remote_path)
        file_client.upload_data(data, overwrite=overwrite)

        size = len(data) if isinstance(data, bytes) else 0
        logger.info("bytes_uploaded", remote=remote_path, size=size)

        return {"remote_path": remote_path, "size_bytes": size}

    def download_file(
        self,
        remote_path: str,
        local_path: str | Path,
    ) -> Path:
        """
        Download a file from ADLS Gen2 to local filesystem.

        Args:
            remote_path: Source path in ADLS
            local_path: Destination local path

        Returns:
            Path to downloaded file
        """
        self._ensure_connected()
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        file_client = self._file_system_client.get_file_client(remote_path)
        download = file_client.download_file()

        with open(local_path, "wb") as f:
            download.readinto(f)

        logger.info(
            "file_downloaded",
            remote=remote_path,
            local=str(local_path),
        )
        return local_path

    def download_bytes(self, remote_path: str) -> bytes:
        """Download a file from ADLS Gen2 as bytes."""
        self._ensure_connected()

        file_client = self._file_system_client.get_file_client(remote_path)
        download = file_client.download_file()
        return download.readall()

    # ─── Model Artifact Operations ────────────────────────────────────────────

    def upload_model(
        self,
        local_model_dir: str | Path,
        model_name: str,
        version: str | None = None,
    ) -> dict[str, Any]:
        """
        Upload a trained model directory to ADLS Gen2.

        Uploads all files in the model directory to:
            models/{model_name}/{version}/

        Args:
            local_model_dir: Local directory containing model files
            model_name: Name of the model (e.g., "spark_gbt_price_direction")
            version: Version tag (defaults to timestamp)

        Returns:
            Dict with model location metadata
        """
        self._ensure_connected()
        local_dir = Path(local_model_dir)
        version = version or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        remote_base = f"models/{model_name}/{version}"

        uploaded_files = []
        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                relative = file_path.relative_to(local_dir)
                remote = f"{remote_base}/{relative}"
                self.upload_file(file_path, remote)
                uploaded_files.append(remote)

        # Write metadata
        metadata = {
            "model_name": model_name,
            "version": version,
            "files": uploaded_files,
            "uploaded_at": datetime.utcnow().isoformat(),
            "adls_path": f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{remote_base}",
        }
        self.upload_bytes(
            json.dumps(metadata, indent=2).encode(),
            f"{remote_base}/_metadata.json",
        )

        logger.info(
            "model_uploaded",
            model=model_name,
            version=version,
            files=len(uploaded_files),
        )
        return metadata

    def download_model(
        self,
        model_name: str,
        version: str,
        local_dir: str | Path,
    ) -> Path:
        """
        Download a model from ADLS Gen2 to local filesystem.

        Downloads all files from:
            models/{model_name}/{version}/

        Args:
            model_name: Name of the model
            version: Version to download
            local_dir: Local directory to download into

        Returns:
            Path to local model directory
        """
        self._ensure_connected()
        local_dir = Path(local_dir) / model_name / version
        local_dir.mkdir(parents=True, exist_ok=True)

        remote_base = f"models/{model_name}/{version}"
        paths = self.list_files(remote_base)

        for file_info in paths:
            remote = file_info["name"]
            relative = remote[len(remote_base) + 1:]
            local_path = local_dir / relative
            self.download_file(remote, local_path)

        logger.info(
            "model_downloaded",
            model=model_name,
            version=version,
            files=len(paths),
        )
        return local_dir

    # ─── Directory Operations ─────────────────────────────────────────────────

    def list_files(
        self,
        path: str = "",
        recursive: bool = True,
    ) -> list[dict[str, Any]]:
        """
        List files and directories in ADLS Gen2.

        Args:
            path: Directory path to list (empty for root)
            recursive: Whether to list recursively

        Returns:
            List of dicts with name, size, last_modified, is_directory
        """
        self._ensure_connected()

        paths = self._file_system_client.get_paths(path=path, recursive=recursive)
        results = []
        for p in paths:
            results.append({
                "name": p.name,
                "size": p.content_length,
                "last_modified": str(p.last_modified) if p.last_modified else None,
                "is_directory": p.is_directory,
            })

        return results

    def delete_file(self, remote_path: str) -> None:
        """Delete a file from ADLS Gen2."""
        self._ensure_connected()
        file_client = self._file_system_client.get_file_client(remote_path)
        file_client.delete_file()
        logger.info("file_deleted", remote=remote_path)

    def create_directory(self, path: str) -> None:
        """Create a directory in ADLS Gen2."""
        self._ensure_connected()
        dir_client = self._file_system_client.get_directory_client(path)
        dir_client.create_directory()
        logger.info("directory_created", path=path)

    # ─── ADLS Gen2 URL Helpers ────────────────────────────────────────────────

    def get_abfss_url(self, path: str) -> str:
        """
        Get the abfss:// URL for a path in ADLS Gen2.

        Used by Spark to read/write Delta tables on ADLS:
            abfss://{container}@{account}.dfs.core.windows.net/{path}
        """
        return (
            f"abfss://{self.container_name}"
            f"@{self.account_name}.dfs.core.windows.net/{path}"
        )

    def get_https_url(self, path: str) -> str:
        """Get the HTTPS URL for a path in ADLS Gen2."""
        return (
            f"https://{self.account_name}.dfs.core.windows.net"
            f"/{self.container_name}/{path}"
        )

    # ─── Context Manager ─────────────────────────────────────────────────────

    def __enter__(self) -> "ADLSClient":
        return self.connect()

    def __exit__(self, *args) -> None:
        if self._service_client:
            self._service_client.close()


# =============================================================================
# Convenience Functions
# =============================================================================

def get_adls_client() -> ADLSClient:
    """Get an auto-configured ADLS client from settings."""
    return ADLSClient().connect()


def upload_model_to_adls(
    local_dir: str | Path,
    model_name: str,
    version: str | None = None,
) -> dict[str, Any]:
    """
    Upload model artifacts to ADLS Gen2.

    Usage:
        metadata = upload_model_to_adls(
            "models/artifacts/spark_ml",
            "spark_gbt_price_direction",
        )
        print(metadata["adls_path"])  # abfss://...
    """
    with ADLSClient() as client:
        return client.upload_model(local_dir, model_name, version)
