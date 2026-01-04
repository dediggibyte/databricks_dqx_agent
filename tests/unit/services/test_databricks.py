"""
Unit tests for DatabricksService.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestDatabricksServiceInit:
    """Tests for DatabricksService initialization."""

    def test_service_initialization(self):
        """Test service initializes correctly."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()
        assert service._sdk_config is None

    def test_get_sql_warehouse_id(self):
        """Test getting SQL warehouse ID from config."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()
        warehouse_id = service.get_sql_warehouse_id()
        # Set in conftest.py
        assert warehouse_id == "test-warehouse-id"


class TestDatabricksServiceHelpers:
    """Tests for DatabricksService helper methods."""

    def test_get_host_from_config(self):
        """Test getting host from config."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()
        host = service._get_host()
        assert host == "https://test-workspace.cloud.databricks.com"

    def test_get_sql_http_path(self):
        """Test SQL HTTP path generation."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()
        http_path = service._get_sql_http_path()
        assert http_path == "/sql/1.0/warehouses/test-warehouse-id"

    def test_get_user_token_no_context(self):
        """Test getting user token outside request context."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()
        token = service._get_user_token()
        assert token is None


class TestDatabricksServiceCatalogOperations:
    """Tests for Unity Catalog operations."""

    def test_get_catalogs_with_mock(self, app):
        """Test get_catalogs with mocked SQL connection."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with patch.object(service, 'execute_sql') as mock_sql:
            mock_sql.return_value = ["main", "samples", "hive_metastore"]

            with app.test_request_context():
                catalogs = service.get_catalogs()

            assert catalogs == ["main", "samples", "hive_metastore"]
            mock_sql.assert_called_once_with("SHOW CATALOGS")

    def test_get_catalogs_error_returns_main(self, app):
        """Test get_catalogs returns ['main'] on error."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with patch.object(service, 'execute_sql') as mock_sql:
            mock_sql.side_effect = Exception("Connection error")

            with app.test_request_context():
                catalogs = service.get_catalogs()

            assert catalogs == ["main"]

    def test_get_schemas_with_mock(self, app):
        """Test get_schemas with mocked SQL connection."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with patch.object(service, 'execute_sql') as mock_sql:
            mock_sql.return_value = ["default", "sales", "analytics"]

            with app.test_request_context():
                schemas = service.get_schemas("main")

            assert schemas == ["default", "sales", "analytics"]
            mock_sql.assert_called_once_with("SHOW SCHEMAS IN `main`")

    def test_get_tables_with_mock(self, app):
        """Test get_tables with mocked SQL connection."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with patch.object(service, 'execute_sql_with_schema') as mock_sql:
            mock_sql.return_value = {
                "rows": [
                    {"database": "default", "tableName": "customers", "isTemporary": False},
                    {"database": "default", "tableName": "orders", "isTemporary": False}
                ]
            }

            with app.test_request_context():
                tables = service.get_tables("main", "default")

            assert tables == ["customers", "orders"]


class TestDatabricksServiceJobOperations:
    """Tests for job operations."""

    def test_trigger_dq_job_not_configured(self):
        """Test trigger_dq_job when job ID not configured."""
        from app.services.databricks import DatabricksService
        from app.config import Config

        service = DatabricksService()

        original = Config.DQ_GENERATION_JOB_ID
        Config.DQ_GENERATION_JOB_ID = None

        result = service.trigger_dq_job("catalog.schema.table", "test prompt")

        assert "error" in result
        assert "not configured" in result["error"]

        Config.DQ_GENERATION_JOB_ID = original

    def test_trigger_validation_job_not_configured(self):
        """Test trigger_validation_job when job ID not configured."""
        from app.services.databricks import DatabricksService
        from app.config import Config

        service = DatabricksService()

        original = Config.DQ_VALIDATION_JOB_ID
        Config.DQ_VALIDATION_JOB_ID = None

        result = service.trigger_validation_job("catalog.schema.table", [])

        assert "error" in result
        assert "not configured" in result["error"]

        Config.DQ_VALIDATION_JOB_ID = original

    def test_trigger_dq_job_with_mock(self, app):
        """Test trigger_dq_job with mocked WorkspaceClient."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.run_id = 12345
        mock_client.jobs.run_now.return_value = mock_response

        with patch.object(service, '_get_client', return_value=mock_client):
            result = service.trigger_dq_job(
                "catalog.schema.table",
                "Validate all columns are not null"
            )

        assert result == {"run_id": 12345}
