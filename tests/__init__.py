"""Tests for cryptopulse.config module."""

import os
from unittest.mock import patch

import pytest


class TestSettings:
    """Test Settings configuration loading."""

    def test_default_settings_load(self):
        """Settings should load with defaults (no .env file needed)."""
        from cryptopulse.config import Settings

        s = Settings()
        assert s.env == "development"
        assert s.debug is True
        assert s.log_level == "INFO"

    def test_environment_values(self):
        """Settings should accept valid environment values."""
        from cryptopulse.config import Settings

        for env in ["development", "staging", "production"]:
            s = Settings(CRYPTOPULSE_ENV=env)
            assert s.env == env

    def test_kafka_config_defaults(self):
        """KafkaConfig should have sensible defaults."""
        from cryptopulse.config import KafkaConfig

        k = KafkaConfig()
        assert k.trades_topic == "crypto.trades.raw"
        assert k.news_topic == "crypto.news.raw"

    def test_binance_config_defaults(self):
        """BinanceConfig should default to no API keys."""
        from cryptopulse.config import BinanceConfig

        b = BinanceConfig()
        assert b.api_key == ""
        assert b.api_secret == ""

    def test_ml_config_defaults(self):
        """MLConfig should have default model paths."""
        from cryptopulse.config import MLConfig

        m = MLConfig()
        assert "price_direction" in m.model_price_direction_path
        assert "anomaly_detection" in m.model_anomaly_detection_path
