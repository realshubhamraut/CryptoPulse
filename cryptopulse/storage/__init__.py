"""
CryptoPulse - Storage Package

Provides storage backends for Delta Lake data:
- ADLS Gen2 (Azure cloud deployment)
- MinIO (local development)
"""

from cryptopulse.storage.adls_client import ADLSClient, get_adls_client

__all__ = ["ADLSClient", "get_adls_client"]
