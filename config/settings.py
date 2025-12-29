"""
Configuration settings for the DQX Agent application.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AppConfig:
    """Application configuration settings."""

    # Databricks settings
    DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN", "")

    # Catalog and schema settings
    DEFAULT_CATALOG: str = os.getenv("DEFAULT_CATALOG", "main")
    DEFAULT_SCHEMA: str = os.getenv("DEFAULT_SCHEMA", "default")

    # Lakebase settings
    LAKEBASE_HOST: str = os.getenv("LAKEBASE_HOST", "")
    LAKEBASE_DATABASE: str = os.getenv("LAKEBASE_DATABASE", "dqx_rules_db")
    LAKEBASE_SCHEMA: str = os.getenv("LAKEBASE_SCHEMA", "dq_rules")
    LAKEBASE_TABLE: str = os.getenv("LAKEBASE_TABLE", "dq_rule_events")

    # Job settings
    DQ_GENERATION_JOB_ID: Optional[str] = os.getenv("DQ_GENERATION_JOB_ID")
    DQ_GENERATION_NOTEBOOK_PATH: str = os.getenv(
        "DQ_GENERATION_NOTEBOOK_PATH",
        "/Workspace/dqx_agent/notebooks/generate_dq_rules"
    )

    # AI/LLM settings
    MODEL_SERVING_ENDPOINT: str = os.getenv(
        "MODEL_SERVING_ENDPOINT",
        "databricks-meta-llama-3-1-70b-instruct"
    )

    # App settings
    SAMPLE_DATA_LIMIT: int = int(os.getenv("SAMPLE_DATA_LIMIT", "100"))


# Global config instance
config = AppConfig()
