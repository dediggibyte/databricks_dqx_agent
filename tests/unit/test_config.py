"""
Unit tests for application configuration.
"""
import os
import pytest


class TestConfig:
    """Tests for Config class."""

    def test_default_sample_limit(self):
        """Test default sample data limit."""
        from app.config import Config
        assert Config.SAMPLE_DATA_LIMIT == 100

    def test_default_model_endpoint(self):
        """Test default model serving endpoint."""
        from app.config import Config
        assert Config.MODEL_SERVING_ENDPOINT == "databricks-claude-sonnet-4-5"

    def test_default_lakebase_database(self):
        """Test default Lakebase database name."""
        from app.config import Config
        assert Config.LAKEBASE_DATABASE == "databricks_postgres"

    def test_default_lakebase_port(self):
        """Test default Lakebase port."""
        from app.config import Config
        assert Config.LAKEBASE_PORT == 5432

    def test_is_lakebase_configured_false(self):
        """Test Lakebase not configured when host is empty."""
        from app.config import Config

        original = Config.LAKEBASE_HOST
        Config.LAKEBASE_HOST = None
        assert Config.is_lakebase_configured() is False
        Config.LAKEBASE_HOST = original

    def test_is_lakebase_configured_true(self, monkeypatch):
        """Test Lakebase configured when host is set."""
        from app.config import Config

        original = Config.LAKEBASE_HOST
        Config.LAKEBASE_HOST = "test-host.cloud.databricks.com"
        assert Config.is_lakebase_configured() is True
        Config.LAKEBASE_HOST = original

    def test_is_job_configured(self):
        """Test job configuration check."""
        from app.config import Config
        # DQ_GENERATION_JOB_ID is set in conftest.py
        assert Config.is_job_configured() is True

    def test_is_validation_job_configured(self):
        """Test validation job configuration check."""
        from app.config import Config
        # DQ_VALIDATION_JOB_ID is set in conftest.py
        assert Config.is_validation_job_configured() is True
