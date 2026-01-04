"""
Unit tests for LakebaseService.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestLakebaseServiceConnection:
    """Tests for LakebaseService connection methods."""

    def test_check_connection_not_configured(self, app):
        """Test check_connection when Lakebase not configured."""
        from app.services.lakebase import LakebaseService
        from app.config import Config

        original = Config.LAKEBASE_HOST
        Config.LAKEBASE_HOST = None

        with app.test_request_context():
            result = LakebaseService.check_connection()

        assert result["connected"] is False
        assert result["configured"] is False

        Config.LAKEBASE_HOST = original

    def test_check_connection_no_token(self, app):
        """Test check_connection without OAuth token."""
        from app.services.lakebase import LakebaseService
        from app.config import Config

        original = Config.LAKEBASE_HOST
        Config.LAKEBASE_HOST = "test-host.cloud.databricks.com"

        with app.test_request_context():
            result = LakebaseService.check_connection()

        assert result["connected"] is False
        assert result["configured"] is True
        assert "OAuth token" in result["message"] or "authenticated" in result["message"]

        Config.LAKEBASE_HOST = original

    def test_get_user_oauth_credentials_no_token(self, app):
        """Test get_user_oauth_credentials raises without token."""
        from app.services.lakebase import LakebaseService

        with app.test_request_context():
            with pytest.raises(Exception) as exc_info:
                LakebaseService.get_user_oauth_credentials()

            assert "OAuth token" in str(exc_info.value) or "authenticated" in str(exc_info.value)


class TestLakebaseServiceOperations:
    """Tests for LakebaseService data operations."""

    def test_get_next_version_default(self, app, sample_rules):
        """Test get_next_version returns 1 on error."""
        from app.services.lakebase import LakebaseService

        with patch.object(LakebaseService, 'get_connection') as mock_conn:
            mock_conn.side_effect = Exception("Connection failed")

            with app.test_request_context(
                headers={'x-forwarded-access-token': 'test-token'}
            ):
                version = LakebaseService.get_next_version("catalog.schema.table")

        assert version == 1

    def test_save_rules_success(self, app, sample_rules):
        """Test save_rules with mocked connection."""
        from app.services.lakebase import LakebaseService
        import uuid
        from datetime import datetime

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            str(uuid.uuid4()),
            1,
            datetime.now()
        )

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(LakebaseService, 'get_connection', return_value=mock_conn):
            with patch.object(LakebaseService, 'init_table', return_value=True):
                with patch.object(LakebaseService, 'get_next_version', return_value=1):
                    with app.test_request_context(
                        headers={'x-forwarded-access-token': 'test-token'}
                    ):
                        result = LakebaseService.save_rules(
                            table_name="catalog.schema.table",
                            rules=sample_rules,
                            user_prompt="test prompt"
                        )

        assert result["success"] is True
        assert result["version"] == 1

    def test_get_history_success(self, app):
        """Test get_history with mocked connection."""
        from app.services.lakebase import LakebaseService
        import uuid
        from datetime import datetime

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                uuid.uuid4(),
                1,
                [{"check": {"function": "is_not_null"}}],
                "test prompt",
                {"summary": "test"},
                datetime.now(),
                True
            )
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(LakebaseService, 'get_connection', return_value=mock_conn):
            with app.test_request_context(
                headers={'x-forwarded-access-token': 'test-token'}
            ):
                result = LakebaseService.get_history("catalog.schema.table")

        assert result["success"] is True
        assert len(result["history"]) == 1
        assert result["history"][0]["version"] == 1

    def test_get_history_error(self, app):
        """Test get_history handles errors gracefully."""
        from app.services.lakebase import LakebaseService

        with patch.object(LakebaseService, 'get_connection') as mock_conn:
            mock_conn.side_effect = Exception("Connection failed")

            with app.test_request_context(
                headers={'x-forwarded-access-token': 'test-token'}
            ):
                result = LakebaseService.get_history("catalog.schema.table")

        assert result["success"] is False
        assert "error" in result
