"""
Unit tests for Flask application factory.
"""
import pytest


class TestAppFactory:
    """Tests for create_app factory function."""

    def test_create_app(self, app):
        """Test app is created successfully."""
        assert app is not None
        assert app.config['TESTING'] is True

    def test_app_has_secret_key(self, app):
        """Test app has secret key configured."""
        assert app.secret_key is not None

    def test_blueprints_registered(self, app):
        """Test all blueprints are registered."""
        blueprint_names = list(app.blueprints.keys())
        assert 'catalog' in blueprint_names
        assert 'rules' in blueprint_names
        assert 'lakebase' in blueprint_names


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_endpoint(self, client):
        """Test health endpoint returns healthy status."""
        response = client.get('/health')

        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'healthy'
        assert 'timestamp' in data

    def test_health_endpoint_json_content_type(self, client):
        """Test health endpoint returns JSON content type."""
        response = client.get('/health')
        assert response.content_type == 'application/json'


class TestMainRoutes:
    """Tests for main application routes."""

    def test_index_redirects_to_generator(self, client):
        """Test index redirects to generator page."""
        response = client.get('/')

        assert response.status_code == 302
        assert '/generator' in response.location

    def test_generator_page(self, client):
        """Test generator page loads."""
        response = client.get('/generator')
        assert response.status_code == 200

    def test_validator_page(self, client):
        """Test validator page loads."""
        response = client.get('/validator')
        assert response.status_code == 200
