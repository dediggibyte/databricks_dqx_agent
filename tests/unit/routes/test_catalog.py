"""
Unit tests for catalog routes.
"""
import pytest
from unittest.mock import patch


class TestCatalogRoutes:
    """Tests for catalog API routes."""

    def test_get_catalogs_success(self, client):
        """Test GET /api/catalogs returns catalog list."""
        with patch('app.routes.catalog.databricks_service') as mock_service:
            mock_service.get_catalogs.return_value = ["main", "samples"]

            response = client.get('/api/catalogs')

        assert response.status_code == 200
        data = response.get_json()
        assert data == ["main", "samples"]

    def test_get_catalogs_error(self, client):
        """Test GET /api/catalogs handles errors."""
        with patch('app.routes.catalog.databricks_service') as mock_service:
            mock_service.get_catalogs.side_effect = Exception("Connection error")

            response = client.get('/api/catalogs')

        assert response.status_code == 500
        data = response.get_json()
        assert "error" in data

    def test_get_schemas_success(self, client):
        """Test GET /api/schemas/<catalog> returns schema list."""
        with patch('app.routes.catalog.databricks_service') as mock_service:
            mock_service.get_schemas.return_value = ["default", "sales"]

            response = client.get('/api/schemas/main')

        assert response.status_code == 200
        data = response.get_json()
        assert data == ["default", "sales"]
        mock_service.get_schemas.assert_called_once_with("main")

    def test_get_tables_success(self, client):
        """Test GET /api/tables/<catalog>/<schema> returns table list."""
        with patch('app.routes.catalog.databricks_service') as mock_service:
            mock_service.get_tables.return_value = ["customers", "orders"]

            response = client.get('/api/tables/main/default')

        assert response.status_code == 200
        data = response.get_json()
        assert data == ["customers", "orders"]
        mock_service.get_tables.assert_called_once_with("main", "default")

    def test_get_sample_success(self, client):
        """Test GET /api/sample/<catalog>/<schema>/<table> returns sample data."""
        with patch('app.routes.catalog.databricks_service') as mock_service:
            mock_service.get_table_sample.return_value = {
                "columns": ["id", "name"],
                "rows": [{"id": 1, "name": "Test"}],
                "row_count": 1
            }

            response = client.get('/api/sample/main/default/customers')

        assert response.status_code == 200
        data = response.get_json()
        assert data["columns"] == ["id", "name"]
        assert data["row_count"] == 1

    def test_debug_endpoint(self, client):
        """Test GET /api/debug returns debug info."""
        response = client.get('/api/debug')

        assert response.status_code == 200
        data = response.get_json()
        assert "user_token_present" in data
        assert "sql_warehouse_id" in data
        assert "databricks_host" in data
