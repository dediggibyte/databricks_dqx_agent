"""
Configuration management for the DQ Rule Generator App.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration."""

    # Flask
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dq-rule-generator-secret-key")
    DEBUG = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    # Databricks Jobs
    DQ_GENERATION_JOB_ID = os.getenv("DQ_GENERATION_JOB_ID")
    DQ_VALIDATION_JOB_ID = os.getenv("DQ_VALIDATION_JOB_ID")

    # Sample Data
    SAMPLE_DATA_LIMIT = int(os.getenv("SAMPLE_DATA_LIMIT", "100"))

    # Lakebase (PostgreSQL)
    LAKEBASE_HOST = os.getenv("LAKEBASE_HOST")
    LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", "5432"))

    # AI Model Serving
    MODEL_SERVING_ENDPOINT = os.getenv("MODEL_SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")

    # SQL Warehouse
    SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")

    # Databricks SDK
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    @classmethod
    def is_lakebase_configured(cls) -> bool:
        """Check if Lakebase is configured."""
        return bool(cls.LAKEBASE_HOST)

    @classmethod
    def is_job_configured(cls) -> bool:
        """Check if the DQ generation job is configured."""
        return bool(cls.DQ_GENERATION_JOB_ID)

    @classmethod
    def is_validation_job_configured(cls) -> bool:
        """Check if the DQ validation job is configured."""
        return bool(cls.DQ_VALIDATION_JOB_ID)
