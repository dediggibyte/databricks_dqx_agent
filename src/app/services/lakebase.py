"""
Lakebase (PostgreSQL) service for storing DQ rules.
"""
import json
import uuid
from typing import Optional, Dict, Any, List, Tuple
from flask import request

from ..config import Config


class LakebaseService:
    """Service for Lakebase database operations."""

    @staticmethod
    def get_user_oauth_credentials() -> Tuple[str, str]:
        """Get user OAuth credentials from request headers."""
        user_token = request.headers.get('x-forwarded-access-token')
        user_email = request.headers.get('x-forwarded-email')

        if not user_token:
            raise Exception("No OAuth token found. User must be authenticated via Databricks Apps.")

        if not user_email:
            try:
                from databricks.sdk import WorkspaceClient
                ws = WorkspaceClient(token=user_token)
                current_user = ws.current_user.me()
                user_email = current_user.user_name
            except Exception as e:
                raise Exception(f"Could not determine user email: {e}")

        return user_email, user_token

    @staticmethod
    def get_connection():
        """Get PostgreSQL connection to Lakebase using OAuth."""
        import psycopg2

        if not Config.LAKEBASE_HOST:
            raise Exception("Lakebase connection not configured. Set LAKEBASE_HOST in app.yaml")

        user_email, user_token = LakebaseService.get_user_oauth_credentials()

        conn = psycopg2.connect(
            host=Config.LAKEBASE_HOST,
            database=Config.LAKEBASE_DATABASE,
            user=user_email,
            password=user_token,
            port=Config.LAKEBASE_PORT,
            sslmode='require'
        )
        return conn

    @staticmethod
    def init_table() -> bool:
        """Initialize the DQ rules table if it doesn't exist."""
        try:
            conn = LakebaseService.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dq_rules_events (
                    id UUID PRIMARY KEY,
                    table_name VARCHAR(500) NOT NULL,
                    version INTEGER NOT NULL,
                    rules JSONB NOT NULL,
                    user_prompt TEXT,
                    ai_summary JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    metadata JSONB,
                    UNIQUE(table_name, version)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dq_rules_table_name
                ON dq_rules_events(table_name)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dq_rules_active
                ON dq_rules_events(table_name, is_active)
                WHERE is_active = TRUE
            """)

            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error initializing Lakebase table: {e}")
            return False

    @staticmethod
    def get_next_version(table_name: str) -> int:
        """Get the next version number for a table's DQ rules."""
        try:
            conn = LakebaseService.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COALESCE(MAX(version), 0) + 1
                FROM dq_rules_events
                WHERE table_name = %s
            """, (table_name,))

            result = cursor.fetchone()
            next_version = result[0] if result else 1

            cursor.close()
            conn.close()
            return next_version
        except Exception as e:
            print(f"Error getting next version: {e}")
            return 1

    @staticmethod
    def save_rules(
        table_name: str,
        rules: List[Dict],
        user_prompt: str,
        ai_summary: Optional[Dict] = None,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Save DQ rules to Lakebase with versioning."""
        try:
            LakebaseService.init_table()

            conn = LakebaseService.get_connection()
            cursor = conn.cursor()

            version = LakebaseService.get_next_version(table_name)

            # Deactivate previous versions
            cursor.execute("""
                UPDATE dq_rules_events
                SET is_active = FALSE
                WHERE table_name = %s AND is_active = TRUE
            """, (table_name,))

            # Insert new version
            rule_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO dq_rules_events
                (id, table_name, version, rules, user_prompt, ai_summary, created_by, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, version, created_at
            """, (
                rule_id,
                table_name,
                version,
                json.dumps(rules),
                user_prompt,
                json.dumps(ai_summary) if ai_summary else None,
                "dq-rule-generator-app",
                json.dumps(metadata) if metadata else None
            ))

            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()

            return {
                "success": True,
                "id": result[0],
                "version": result[1],
                "created_at": result[2].isoformat() if result[2] else None
            }
        except Exception as e:
            print(f"Error saving rules to Lakebase: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def get_history(table_name: str, limit: int = 10) -> Dict[str, Any]:
        """Get history of DQ rules for a table."""
        try:
            conn = LakebaseService.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, version, rules, user_prompt, ai_summary, created_at, is_active
                FROM dq_rules_events
                WHERE table_name = %s
                ORDER BY version DESC
                LIMIT %s
            """, (table_name, limit))

            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            history = []
            for row in rows:
                history.append({
                    "id": str(row[0]),
                    "version": row[1],
                    "rules": row[2],
                    "user_prompt": row[3],
                    "ai_summary": row[4],
                    "created_at": row[5].isoformat() if row[5] else None,
                    "is_active": row[6]
                })

            return {"success": True, "history": history}
        except Exception as e:
            print(f"Error getting rules history: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def check_connection() -> Dict[str, Any]:
        """Check Lakebase connection status."""
        if not Config.LAKEBASE_HOST:
            return {
                "connected": False,
                "configured": False,
                "message": "Lakebase host not configured"
            }

        user_token = request.headers.get('x-forwarded-access-token')
        if not user_token:
            return {
                "connected": False,
                "configured": True,
                "message": "No OAuth token - user must be authenticated via Databricks Apps"
            }

        try:
            conn = LakebaseService.get_connection()
            conn.close()

            user_email, _ = LakebaseService.get_user_oauth_credentials()
            return {
                "connected": True,
                "configured": True,
                "host": Config.LAKEBASE_HOST,
                "database": Config.LAKEBASE_DATABASE,
                "auth_type": "oauth",
                "user": user_email
            }
        except Exception as e:
            return {
                "connected": False,
                "configured": True,
                "error": str(e)
            }
