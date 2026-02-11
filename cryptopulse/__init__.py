"""
CryptoPulse - Core Package

Enterprise-grade real-time cryptocurrency market intelligence platform.
"""

__version__ = "0.1.0"
__author__ = "CryptoPulse Team"

from cryptopulse.config import settings, get_settings

__all__ = ["settings", "get_settings", "__version__"]
