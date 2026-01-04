"""
Integration tests for Databricks connection.

These tests require:
- DATABRICKS_HOST environment variable
- DATABRICKS_TOKEN environment variable (for local testing)
- SQL_WAREHOUSE_ID environment variable

Run with: pytest -m integration
"""
import os
import pytest


pytestmark = pytest.mark.integration


@pytest.fixture
def skip_if_no_databricks():
    """Skip test if Databricks is not configured."""
    if not os.getenv("DATABRICKS_HOST"):
        pytest.skip("DATABRICKS_HOST not set")
    if not os.getenv("DATABRICKS_TOKEN") and not os.getenv("SQL_WAREHOUSE_ID"):
        pytest.skip("Databricks credentials not configured")


class TestDatabricksConnection:
    """Integration tests for Databricks connectivity."""

    def test_list_catalogs(self, skip_if_no_databricks, app):
        """Test listing catalogs from Unity Catalog."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with app.test_request_context():
            catalogs = service.get_catalogs()

        assert isinstance(catalogs, list)
        assert len(catalogs) > 0

    def test_list_schemas(self, skip_if_no_databricks, app):
        """Test listing schemas from a catalog."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with app.test_request_context():
            catalogs = service.get_catalogs()
            if catalogs:
                schemas = service.get_schemas(catalogs[0])
                assert isinstance(schemas, list)

    def test_sql_warehouse_connection(self, skip_if_no_databricks, app):
        """Test SQL warehouse connectivity."""
        from app.services.databricks import DatabricksService

        service = DatabricksService()

        with app.test_request_context():
            # Simple query to test connection
            result = service.execute_sql("SELECT 1 as test")
            assert result == [1]
