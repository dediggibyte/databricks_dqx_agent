"""
Unit tests for AIAnalysisService.
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import json


class TestAIAnalysisService:
    """Tests for AIAnalysisService."""

    def test_analyze_rules_no_warehouse(self, app, sample_rules):
        """Test analyze_rules fails without warehouse ID."""
        from app.services.ai import AIAnalysisService

        mock_client = MagicMock()

        with patch('app.services.ai.databricks_service') as mock_service:
            mock_service.client = mock_client
            mock_service.get_sql_warehouse_id.return_value = None

            with app.test_request_context():
                result = AIAnalysisService.analyze_rules(
                    sample_rules,
                    "catalog.schema.table",
                    "test prompt"
                )

        assert result["success"] is False
        assert "warehouse" in result["error"].lower()

    def test_analyze_rules_success(self, app, sample_rules):
        """Test analyze_rules with successful AI response."""
        from app.services.ai import AIAnalysisService

        mock_response = MagicMock()
        mock_response.statement_id = "stmt-123"
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.result.data_array = [[json.dumps({
            "summary": "Test summary",
            "rule_analysis": [],
            "coverage_assessment": "Good",
            "recommendations": [],
            "overall_quality_score": 8
        })]]

        mock_client = MagicMock()
        mock_client.statement_execution.execute_statement.return_value = mock_response
        mock_client.statement_execution.get_statement.return_value = mock_response

        with patch('app.services.ai.databricks_service') as mock_service:
            mock_service.client = mock_client
            mock_service.get_sql_warehouse_id.return_value = "wh-123"

            with app.test_request_context():
                result = AIAnalysisService.analyze_rules(
                    sample_rules,
                    "catalog.schema.table",
                    "test prompt"
                )

        assert result["success"] is True
        assert "analysis" in result
        assert result["analysis"]["summary"] == "Test summary"

    def test_analyze_rules_handles_exception(self, app, sample_rules):
        """Test analyze_rules handles exceptions gracefully."""
        from app.services.ai import AIAnalysisService

        mock_client = MagicMock()
        mock_client.statement_execution.execute_statement.side_effect = Exception("API Error")

        with patch('app.services.ai.databricks_service') as mock_service:
            mock_service.client = mock_client
            mock_service.get_sql_warehouse_id.return_value = "wh-123"

            with app.test_request_context():
                result = AIAnalysisService.analyze_rules(
                    sample_rules,
                    "catalog.schema.table",
                    "test prompt"
                )

        assert result["success"] is False
        assert "error" in result
