"""
Databricks SDK service for Unity Catalog and Job operations.

Uses user authorization - the app acts on behalf of the logged-in user
using their access token forwarded via x-forwarded-access-token header.

SQL operations use databricks-sql-connector with OBO (On-Behalf-Of) authentication.
"""
import os
import json
from typing import Optional, List, Dict, Any
from flask import request, has_request_context
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config as SdkConfig
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks import sql

from ..config import Config


class DatabricksService:
    """Service for Databricks SDK operations using user authorization."""

    def __init__(self):
        """Initialize the service."""
        self._sdk_config = None

    @property
    def client(self) -> WorkspaceClient:
        """Get WorkspaceClient with user's token (for SQL Statement Execution API)."""
        return self._get_client(use_user_token=True)

    def get_sql_warehouse_id(self) -> Optional[str]:
        """Get configured SQL warehouse ID."""
        return Config.SQL_WAREHOUSE_ID

    def _get_sdk_config(self) -> Optional[SdkConfig]:
        """Lazily get SDK config (only when running in Databricks environment)."""
        if self._sdk_config is None:
            try:
                self._sdk_config = SdkConfig()
            except Exception as e:
                print(f"[DEBUG] Could not initialize SDK config: {e}")
                return None
        return self._sdk_config

    def _get_host(self) -> str:
        """Get Databricks host from config or environment."""
        host = Config.DATABRICKS_HOST or os.getenv("DATABRICKS_HOST")
        if not host:
            sdk_config = self._get_sdk_config()
            if sdk_config:
                host = sdk_config.host
        return host

    def _get_user_token(self) -> Optional[str]:
        """Get user's access token from request headers."""
        if has_request_context():
            token = request.headers.get('x-forwarded-access-token')
            if token:
                print(f"[DEBUG] User token found (length: {len(token)})")
            else:
                print("[DEBUG] No user token in x-forwarded-access-token header")
            return token
        print("[DEBUG] No request context available")
        return None

    def _get_sql_http_path(self) -> Optional[str]:
        """Get SQL warehouse HTTP path."""
        warehouse_id = Config.SQL_WAREHOUSE_ID
        if warehouse_id:
            return f"/sql/1.0/warehouses/{warehouse_id}"
        return None

    def _get_sql_connection(self):
        """Get SQL connection using user's token (OBO) or service principal.

        Uses databricks-sql-connector which properly handles OBO authentication.
        """
        host = self._get_host()
        http_path = self._get_sql_http_path()

        if not host or not http_path:
            raise Exception(f"SQL connection not configured: host={host}, http_path={http_path}")

        # Remove https:// prefix if present (connector expects just hostname)
        if host.startswith("https://"):
            host = host[8:]
        elif host.startswith("http://"):
            host = host[7:]

        user_token = self._get_user_token()

        if user_token:
            # OBO (On-Behalf-Of) authentication using user's token
            print(f"[DEBUG] Creating SQL connection with OBO auth, host={host}")
            return sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=user_token
            )
        elif Config.DATABRICKS_TOKEN:
            # Fallback to configured token (local dev)
            print(f"[DEBUG] Creating SQL connection with configured token, host={host}")
            return sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=Config.DATABRICKS_TOKEN
            )
        else:
            # Use service principal credentials
            sdk_config = self._get_sdk_config()
            if sdk_config:
                print(f"[DEBUG] Creating SQL connection with SP credentials, host={host}")
                return sql.connect(
                    server_hostname=host,
                    http_path=http_path,
                    credentials_provider=lambda: sdk_config.authenticate
                )
            else:
                raise Exception(
                    "No authentication method available "
                    "(no user token, no configured token, no SP credentials)"
                )

    def _get_client(self, use_user_token: bool = True) -> WorkspaceClient:
        """Get WorkspaceClient with user's token or default auth.

        Used for non-SQL operations like job management.
        """
        user_token = self._get_user_token() if use_user_token else None

        if user_token:
            host = self._get_host()
            print(f"[DEBUG] Creating WorkspaceClient with user token, host={host}")
            return WorkspaceClient(
                host=host,
                token=user_token,
                auth_type="pat"
            )
        elif Config.DATABRICKS_HOST and Config.DATABRICKS_TOKEN:
            return WorkspaceClient(
                host=Config.DATABRICKS_HOST,
                token=Config.DATABRICKS_TOKEN,
                auth_type="pat"
            )
        else:
            return WorkspaceClient()

    # ============================================================
    # SQL Operations (using databricks-sql-connector)
    # ============================================================

    def execute_sql(self, statement: str) -> List[Any]:
        """Execute SQL and return first column values."""
        try:
            with self._get_sql_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(statement)
                    rows = cursor.fetchall()
                    return [row[0] for row in rows] if rows else []
        except Exception as e:
            print(f"[ERROR] execute_sql failed: {e}")
            raise

    def execute_sql_with_schema(self, statement: str) -> Dict[str, Any]:
        """Execute SQL and return full results with column info."""
        try:
            with self._get_sql_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(statement)
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    rows = cursor.fetchall()

                    # Convert rows to list of dicts
                    result_rows = []
                    for row in rows:
                        result_rows.append(dict(zip(columns, row)))

                    return {
                        "columns": columns,
                        "rows": result_rows,
                        "row_count": len(result_rows)
                    }
        except Exception as e:
            print(f"[ERROR] execute_sql_with_schema failed: {e}")
            raise

    # ============================================================
    # Unity Catalog Operations
    # ============================================================

    def get_catalogs(self) -> List[str]:
        """Get list of available catalogs (uses user's permissions)."""
        try:
            catalogs = self.execute_sql("SHOW CATALOGS")
            print(f"[DEBUG] Found catalogs: {catalogs}")
            return catalogs if catalogs else ["main"]
        except Exception as e:
            print(f"Error listing catalogs: {e}")
            return ["main"]

    def get_schemas(self, catalog: str) -> List[str]:
        """Get list of schemas in a catalog (uses user's permissions)."""
        try:
            schemas = self.execute_sql(f"SHOW SCHEMAS IN `{catalog}`")
            return schemas if schemas else ["default"]
        except Exception as e:
            print(f"Error listing schemas: {e}")
            return ["default"]

    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Get list of tables in a schema (uses user's permissions)."""
        try:
            result = self.execute_sql_with_schema(f"SHOW TABLES IN `{catalog}`.`{schema}`")
            # SHOW TABLES returns: database, tableName, isTemporary
            if result["rows"]:
                # tableName is typically the second column
                return [row.get("tableName", row.get("table_name", list(row.values())[1]))
                        for row in result["rows"]]
            return []
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def get_table_sample(self, full_table_name: str, limit: int = 100) -> Dict[str, Any]:
        """Get sample data from a table (uses user's permissions)."""
        try:
            result = self.execute_sql_with_schema(
                f"SELECT * FROM {full_table_name} LIMIT {limit}"
            )
            return result
        except Exception as e:
            print(f"Error getting sample data: {e}")
            return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}

    # ============================================================
    # Job Operations (using WorkspaceClient with App SP credentials)
    # Note: Jobs use app service principal, not user token, because
    # there's no "jobs" scope available for user authorization.
    # The app SP has CAN_MANAGE_RUN permission via resource binding.
    # ============================================================

    def trigger_dq_job(self, table_name: str, user_prompt: str, sample_limit: Optional[int] = None) -> Dict[str, Any]:
        """Trigger the DQ rule generation job (uses app SP credentials)."""
        if not Config.DQ_GENERATION_JOB_ID:
            return {"error": "DQ_GENERATION_JOB_ID not configured"}

        try:
            # Use app SP credentials (not user token) for job operations
            client = self._get_client(use_user_token=False)
            job_parameters = {
                "table_name": table_name,
                "user_prompt": user_prompt
            }

            if sample_limit is not None:
                job_parameters["sample_limit"] = str(sample_limit)

            response = client.jobs.run_now(
                job_id=int(Config.DQ_GENERATION_JOB_ID),
                job_parameters=job_parameters
            )
            return {"run_id": response.run_id}
        except Exception as e:
            return {"error": str(e)}

    def trigger_validation_job(self, table_name: str, rules: List[Dict]) -> Dict[str, Any]:
        """Trigger the DQ rule validation job (uses app SP credentials)."""
        if not Config.DQ_VALIDATION_JOB_ID:
            return {"error": "DQ_VALIDATION_JOB_ID not configured"}

        try:
            # Use app SP credentials (not user token) for job operations
            client = self._get_client(use_user_token=False)
            response = client.jobs.run_now(
                job_id=int(Config.DQ_VALIDATION_JOB_ID),
                job_parameters={
                    "table_name": table_name,
                    "rules": json.dumps(rules)
                }
            )
            return {"run_id": response.run_id}
        except Exception as e:
            return {"error": str(e)}

    def get_job_status(self, run_id: int) -> Dict[str, Any]:
        """Get job run status (uses app SP credentials)."""
        try:
            # Use app SP credentials (not user token) for job operations
            client = self._get_client(use_user_token=False)
            run = client.jobs.get_run(run_id=run_id)
            state = run.state

            if state.life_cycle_state == RunLifeCycleState.TERMINATED:
                if state.result_state == RunResultState.SUCCESS:
                    result = self._get_job_output(run, client)
                    return {"status": "completed", "result": result}
                else:
                    return {"status": "failed", "message": state.state_message}

            elif state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR:
                return {"status": "error", "message": state.state_message}

            else:
                return {"status": "running", "state": str(state.life_cycle_state)}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def _get_job_output(self, run, client: WorkspaceClient) -> Optional[Any]:
        """Extract output from a completed job run."""
        result = None

        if run.tasks and len(run.tasks) > 0:
            for task in run.tasks:
                if task.run_id:
                    try:
                        task_output = client.jobs.get_run_output(run_id=task.run_id)
                        if task_output.notebook_output and task_output.notebook_output.result:
                            try:
                                result = json.loads(task_output.notebook_output.result)
                            except (json.JSONDecodeError, ValueError):
                                result = task_output.notebook_output.result
                            break
                    except Exception as e:
                        print(f"Error getting task output: {e}")
                        continue
        else:
            try:
                output = client.jobs.get_run_output(run_id=run.run_id)
                if output.notebook_output and output.notebook_output.result:
                    try:
                        result = json.loads(output.notebook_output.result)
                    except (json.JSONDecodeError, ValueError):
                        result = output.notebook_output.result
            except Exception as e:
                print(f"Error getting run output: {e}")

        return result


# Service instance
databricks_service = DatabricksService()
