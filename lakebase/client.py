"""
Lakebase Client for DQ Rule Storage

This module provides a client for storing and managing DQ rules
in Databricks Lakebase (PostgreSQL-compatible OLTP database).
"""
import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class DQRuleEvent:
    """Represents a DQ rule event stored in Lakebase."""
    id: Optional[int]
    table_name: str
    rule_definition: Dict[str, Any]
    version: int
    created_at: datetime
    created_by: str
    status: str
    metadata: Optional[Dict[str, Any]] = None


class LakebaseClient:
    """
    Client for interacting with Databricks Lakebase for DQ rule storage.

    Supports:
    - OAuth token-based authentication
    - Version-controlled rule storage
    - Rule event tracking
    - Connection pooling (optional)
    """

    def __init__(
        self,
        host: str = None,
        database: str = None,
        schema: str = "dq_rules",
        use_oauth: bool = True
    ):
        """
        Initialize the Lakebase client.

        Args:
            host: Lakebase host URL
            database: Database name
            schema: Schema name for DQ rules (default: dq_rules)
            use_oauth: Whether to use OAuth tokens for authentication
        """
        self.host = host or os.getenv("LAKEBASE_HOST", "")
        self.database = database or os.getenv("LAKEBASE_DATABASE", "dqx_rules_db")
        self.schema = schema
        self.use_oauth = use_oauth
        self._connection = None

    def _get_credentials(self) -> Dict[str, str]:
        """Get authentication credentials."""
        if self.use_oauth:
            try:
                from databricks.sdk.core import Config
                config = Config()
                return {
                    "user": config.client_id or os.getenv("LAKEBASE_USER", ""),
                    "password": config.token or os.getenv("LAKEBASE_PASSWORD", "")
                }
            except Exception as e:
                logger.warning(f"OAuth config failed, falling back to env vars: {e}")

        return {
            "user": os.getenv("LAKEBASE_USER", ""),
            "password": os.getenv("LAKEBASE_PASSWORD", "")
        }

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        import psycopg

        creds = self._get_credentials()

        conn = psycopg.connect(
            host=self.host,
            dbname=self.database,
            user=creds["user"],
            password=creds["password"],
            sslmode="require"
        )

        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self) -> bool:
        """
        Initialize the Lakebase schema and tables for DQ rule storage.

        Creates:
        - dq_rules schema
        - dq_rule_events table with versioning support
        - Necessary indexes

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    # Create schema
                    cur.execute(f"""
                        CREATE SCHEMA IF NOT EXISTS {self.schema}
                    """)

                    # Create the DQ rule events table
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.schema}.dq_rule_events (
                            id SERIAL PRIMARY KEY,
                            table_name VARCHAR(500) NOT NULL,
                            rule_definition JSONB NOT NULL,
                            version INTEGER NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            created_by VARCHAR(255) NOT NULL,
                            status VARCHAR(50) DEFAULT 'active',
                            metadata JSONB,
                            UNIQUE(table_name, version)
                        )
                    """)

                    # Create indexes for efficient querying
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_dq_rule_events_table_name
                        ON {self.schema}.dq_rule_events (table_name)
                    """)

                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_dq_rule_events_status
                        ON {self.schema}.dq_rule_events (status)
                    """)

                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_dq_rule_events_created_at
                        ON {self.schema}.dq_rule_events (created_at DESC)
                    """)

                    # Create a view for the latest rules
                    cur.execute(f"""
                        CREATE OR REPLACE VIEW {self.schema}.dq_rules_latest AS
                        SELECT DISTINCT ON (table_name)
                            id, table_name, rule_definition, version,
                            created_at, created_by, status, metadata
                        FROM {self.schema}.dq_rule_events
                        WHERE status = 'active'
                        ORDER BY table_name, version DESC
                    """)

                    conn.commit()

            logger.info("Lakebase schema initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Lakebase schema: {e}")
            return False

    def get_next_version(self, table_name: str) -> int:
        """
        Get the next version number for a table's rules.

        Args:
            table_name: Full table name

        Returns:
            Next version number (starts at 1)
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT COALESCE(MAX(version), 0) + 1
                        FROM {self.schema}.dq_rule_events
                        WHERE table_name = %s
                    """, (table_name,))
                    result = cur.fetchone()
                    return result[0] if result else 1
        except Exception as e:
            logger.error(f"Error getting next version: {e}")
            return 1

    def save_rule_event(
        self,
        table_name: str,
        rule_definition: Dict[str, Any],
        created_by: str = None,
        metadata: Dict[str, Any] = None
    ) -> Optional[DQRuleEvent]:
        """
        Save a DQ rule event with automatic versioning.

        Args:
            table_name: Full table name the rule applies to
            rule_definition: The DQ rule definition (JSON-compatible dict)
            created_by: Username of the creator
            metadata: Optional additional metadata

        Returns:
            The created DQRuleEvent, or None if failed
        """
        version = self.get_next_version(table_name)
        created_by = created_by or os.getenv("DATABRICKS_USER", "system")

        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {self.schema}.dq_rule_events
                        (table_name, rule_definition, version, created_by, status, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id, created_at
                    """, (
                        table_name,
                        json.dumps(rule_definition),
                        version,
                        created_by,
                        "active",
                        json.dumps(metadata) if metadata else None
                    ))

                    result = cur.fetchone()
                    conn.commit()

                    if result:
                        return DQRuleEvent(
                            id=result[0],
                            table_name=table_name,
                            rule_definition=rule_definition,
                            version=version,
                            created_at=result[1],
                            created_by=created_by,
                            status="active",
                            metadata=metadata
                        )

        except Exception as e:
            logger.error(f"Failed to save rule event: {e}")

        return None

    def get_latest_rule(self, table_name: str) -> Optional[DQRuleEvent]:
        """
        Get the latest active rule for a table.

        Args:
            table_name: Full table name

        Returns:
            The latest DQRuleEvent, or None if not found
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, table_name, rule_definition, version,
                               created_at, created_by, status, metadata
                        FROM {self.schema}.dq_rule_events
                        WHERE table_name = %s AND status = 'active'
                        ORDER BY version DESC
                        LIMIT 1
                    """, (table_name,))

                    result = cur.fetchone()
                    if result:
                        return DQRuleEvent(
                            id=result[0],
                            table_name=result[1],
                            rule_definition=json.loads(result[2]) if isinstance(result[2], str) else result[2],
                            version=result[3],
                            created_at=result[4],
                            created_by=result[5],
                            status=result[6],
                            metadata=json.loads(result[7]) if result[7] and isinstance(result[7], str) else result[7]
                        )

        except Exception as e:
            logger.error(f"Failed to get latest rule: {e}")

        return None

    def get_rule_history(
        self,
        table_name: str,
        limit: int = 10
    ) -> List[DQRuleEvent]:
        """
        Get the version history of rules for a table.

        Args:
            table_name: Full table name
            limit: Maximum number of versions to return

        Returns:
            List of DQRuleEvents, newest first
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, table_name, rule_definition, version,
                               created_at, created_by, status, metadata
                        FROM {self.schema}.dq_rule_events
                        WHERE table_name = %s
                        ORDER BY version DESC
                        LIMIT %s
                    """, (table_name, limit))

                    results = cur.fetchall()
                    return [
                        DQRuleEvent(
                            id=r[0],
                            table_name=r[1],
                            rule_definition=json.loads(r[2]) if isinstance(r[2], str) else r[2],
                            version=r[3],
                            created_at=r[4],
                            created_by=r[5],
                            status=r[6],
                            metadata=json.loads(r[7]) if r[7] and isinstance(r[7], str) else r[7]
                        )
                        for r in results
                    ]

        except Exception as e:
            logger.error(f"Failed to get rule history: {e}")
            return []

    def deactivate_rule(self, table_name: str, version: int) -> bool:
        """
        Deactivate a specific rule version.

        Args:
            table_name: Full table name
            version: Version number to deactivate

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.dq_rule_events
                        SET status = 'inactive'
                        WHERE table_name = %s AND version = %s
                    """, (table_name, version))
                    conn.commit()
                    return cur.rowcount > 0

        except Exception as e:
            logger.error(f"Failed to deactivate rule: {e}")
            return False

    def list_tables_with_rules(self) -> List[str]:
        """
        List all tables that have DQ rules defined.

        Returns:
            List of table names
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT DISTINCT table_name
                        FROM {self.schema}.dq_rule_events
                        WHERE status = 'active'
                        ORDER BY table_name
                    """)

                    return [r[0] for r in cur.fetchall()]

        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []


# Global client instance (lazy initialization)
_lakebase_client: Optional[LakebaseClient] = None


def get_lakebase_client() -> LakebaseClient:
    """Get or create the global Lakebase client."""
    global _lakebase_client
    if _lakebase_client is None:
        _lakebase_client = LakebaseClient()
    return _lakebase_client
