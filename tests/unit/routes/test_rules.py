"""
Unit tests for rules routes.
"""
import pytest
from unittest.mock import patch
import json


class TestGenerateRoute:
    """Tests for rule generation endpoint."""

    def test_generate_missing_table_name(self, client):
        """Test POST /api/generate with missing table_name."""
        response = client.post(
            '/api/generate',
            data=json.dumps({"user_prompt": "test prompt"}),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_generate_missing_prompt(self, client):
        """Test POST /api/generate with missing user_prompt."""
        response = client.post(
            '/api/generate',
            data=json.dumps({"table_name": "catalog.schema.table"}),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_generate_success(self, client):
        """Test POST /api/generate triggers job successfully."""
        with patch('app.routes.rules.databricks_service') as mock_service:
            mock_service.trigger_dq_job.return_value = {"run_id": 12345}

            response = client.post(
                '/api/generate',
                data=json.dumps({
                    "table_name": "catalog.schema.table",
                    "user_prompt": "Check all columns are not null"
                }),
                content_type='application/json'
            )

        assert response.status_code == 200
        data = response.get_json()
        assert data["run_id"] == 12345


class TestStatusRoute:
    """Tests for job status endpoint."""

    def test_get_status_success(self, client):
        """Test GET /api/status/<run_id> returns status."""
        with patch('app.routes.rules.databricks_service') as mock_service:
            mock_service.get_job_status.return_value = {
                "status": "completed",
                "result": {"rules": []}
            }

            response = client.get('/api/status/12345')

        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "completed"


class TestAnalyzeRoute:
    """Tests for AI analysis endpoint."""

    def test_analyze_missing_rules(self, client):
        """Test POST /api/analyze with missing rules."""
        response = client.post(
            '/api/analyze',
            data=json.dumps({
                "table_name": "catalog.schema.table",
                "user_prompt": "test"
            }),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert data["success"] is False

    def test_analyze_success(self, client, sample_rules):
        """Test POST /api/analyze with AI analysis."""
        with patch('app.routes.rules.AIAnalysisService') as mock_service:
            mock_service.analyze_rules.return_value = {
                "success": True,
                "analysis": {"summary": "Test analysis"}
            }

            response = client.post(
                '/api/analyze',
                data=json.dumps({
                    "rules": sample_rules,
                    "table_name": "catalog.schema.table",
                    "user_prompt": "test"
                }),
                content_type='application/json'
            )

        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True


class TestConfirmRoute:
    """Tests for rule confirmation/save endpoint."""

    def test_confirm_missing_rules(self, client):
        """Test POST /api/confirm with missing rules."""
        response = client.post(
            '/api/confirm',
            data=json.dumps({
                "table_name": "catalog.schema.table"
            }),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert data["success"] is False
        assert "rules" in data["error"].lower()

    def test_confirm_missing_table_name(self, client, sample_rules):
        """Test POST /api/confirm with missing table_name."""
        response = client.post(
            '/api/confirm',
            data=json.dumps({
                "rules": sample_rules
            }),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert data["success"] is False
        assert "table" in data["error"].lower()

    def test_confirm_success(self, client, sample_rules):
        """Test POST /api/confirm saves rules."""
        with patch('app.routes.rules.LakebaseService') as mock_service:
            mock_service.save_rules.return_value = {
                "success": True,
                "id": "test-uuid",
                "version": 1
            }

            response = client.post(
                '/api/confirm',
                data=json.dumps({
                    "rules": sample_rules,
                    "table_name": "catalog.schema.table",
                    "user_prompt": "test"
                }),
                content_type='application/json'
            )

        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["version"] == 1


class TestValidateRoute:
    """Tests for rule validation endpoint."""

    def test_validate_missing_table_name(self, client, sample_rules):
        """Test POST /api/validate with missing table_name."""
        response = client.post(
            '/api/validate',
            data=json.dumps({"rules": sample_rules}),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_validate_missing_rules(self, client):
        """Test POST /api/validate with missing rules."""
        response = client.post(
            '/api/validate',
            data=json.dumps({"table_name": "catalog.schema.table"}),
            content_type='application/json'
        )

        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_validate_success(self, client, sample_rules):
        """Test POST /api/validate triggers validation job."""
        with patch('app.routes.rules.databricks_service') as mock_service:
            mock_service.trigger_validation_job.return_value = {"run_id": 67890}

            response = client.post(
                '/api/validate',
                data=json.dumps({
                    "table_name": "catalog.schema.table",
                    "rules": sample_rules
                }),
                content_type='application/json'
            )

        assert response.status_code == 200
        data = response.get_json()
        assert data["run_id"] == 67890


class TestHistoryRoute:
    """Tests for rules history endpoint."""

    def test_get_history_success(self, client):
        """Test GET /api/history/<table_name> returns history."""
        with patch('app.routes.rules.LakebaseService') as mock_service:
            mock_service.get_history.return_value = {
                "success": True,
                "history": [
                    {"version": 1, "is_active": True}
                ]
            }

            response = client.get('/api/history/catalog.schema.table')

        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert len(data["history"]) == 1
