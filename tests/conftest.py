"""
Pytest fixtures for DQX Data Quality Manager tests.
"""
import os
import sys
import pytest

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session", autouse=True)
def set_test_env():
    """Set environment variables for testing."""
    os.environ.setdefault("FLASK_SECRET_KEY", "test-secret-key")
    os.environ.setdefault("SQL_WAREHOUSE_ID", "test-warehouse-id")
    os.environ.setdefault("DQ_GENERATION_JOB_ID", "12345")
    os.environ.setdefault("DQ_VALIDATION_JOB_ID", "67890")
    os.environ.setdefault("DATABRICKS_HOST", "https://test-workspace.cloud.databricks.com")


@pytest.fixture
def app():
    """Create Flask application for testing."""
    from app import create_app

    app = create_app()
    app.config['TESTING'] = True
    return app


@pytest.fixture
def client(app):
    """Create Flask test client."""
    return app.test_client()


@pytest.fixture
def mock_databricks_service(mocker):
    """Mock DatabricksService for unit tests."""
    mock_service = mocker.MagicMock()

    # Default return values
    mock_service.get_catalogs.return_value = ["main", "samples"]
    mock_service.get_schemas.return_value = ["default", "information_schema"]
    mock_service.get_tables.return_value = ["customers", "orders", "products"]
    mock_service.get_table_sample.return_value = {
        "columns": ["id", "name", "email"],
        "rows": [
            {"id": 1, "name": "Test User", "email": "test@example.com"}
        ],
        "row_count": 1
    }
    mock_service.trigger_dq_job.return_value = {"run_id": 111}
    mock_service.trigger_validation_job.return_value = {"run_id": 222}
    mock_service.get_job_status.return_value = {"status": "completed", "result": {"rules": []}}

    return mock_service


@pytest.fixture
def mock_lakebase_service(mocker):
    """Mock LakebaseService for unit tests."""
    mock_service = mocker.MagicMock()

    mock_service.check_connection.return_value = {
        "connected": True,
        "configured": True,
        "host": "test-lakebase.cloud.databricks.com"
    }
    mock_service.save_rules.return_value = {
        "success": True,
        "id": "test-uuid",
        "version": 1,
        "created_at": "2024-01-01T00:00:00"
    }
    mock_service.get_history.return_value = {
        "success": True,
        "history": []
    }

    return mock_service


@pytest.fixture
def mock_ai_service(mocker):
    """Mock AIAnalysisService for unit tests."""
    mock_service = mocker.MagicMock()

    mock_service.analyze_rules.return_value = {
        "success": True,
        "analysis": {
            "summary": "Test analysis summary",
            "rule_analysis": [],
            "coverage_assessment": "Good coverage",
            "recommendations": [],
            "overall_quality_score": 8
        }
    }

    return mock_service


@pytest.fixture
def sample_rules():
    """Sample DQ rules for testing."""
    return [
        {
            "check": {
                "function": "is_not_null",
                "arguments": {"col_name": "id"}
            },
            "name": "id_not_null",
            "criticality": "error"
        },
        {
            "check": {
                "function": "is_not_null",
                "arguments": {"col_name": "email"}
            },
            "name": "email_not_null",
            "criticality": "warn"
        }
    ]
