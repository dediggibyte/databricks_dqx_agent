"""
Databricks SDK service for Unity Catalog and Job operations.

Uses user authorization - the app acts on behalf of the logged-in user
using their access token forwarded via x-forwarded-access-token header.
"""
import json
from typing import Optional, List, Dict, Any
from flask import request, has_request_context
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from ..config import Config


class DatabricksService:
    """Service for Databricks SDK operations using user authorization."""

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

    def _get_client(self, use_user_token: bool = True) -> WorkspaceClient:
        """Get WorkspaceClient with user's token or default auth.

        Args:
            use_user_token: If True, use user's forwarded token for user authorization.
                          If False, use default app authentication.
        """
        import os
        user_token = self._get_user_token() if use_user_token else None

        if user_token:
            # User authorization: use the logged-in user's token
            # Must explicitly set auth_type="pat" to prevent SDK from detecting
            # OAuth credentials in the Databricks Apps environment
            host = Config.DATABRICKS_HOST or os.getenv("DATABRICKS_HOST")
            print(f"[DEBUG] Creating client with user token, host={host}")
            return WorkspaceClient(
                host=host,
                token=user_token,
                auth_type="pat"  # Force PAT auth, ignore OAuth env vars
            )
        elif Config.DATABRICKS_HOST and Config.DATABRICKS_TOKEN:
            # Fallback to explicit token config (for local dev)
            return WorkspaceClient(
                host=Config.DATABRICKS_HOST,
                token=Config.DATABRICKS_TOKEN,
                auth_type="pat"
            )
        else:
            # Default authentication (app service principal via OAuth)
            return WorkspaceClient()

    def get_sql_warehouse_id(self) -> Optional[str]:
        """Get a SQL warehouse ID for executing queries.

        Uses configured SQL_WAREHOUSE_ID if available, otherwise falls back
        to dynamically listing warehouses.
        """
        # Use configured warehouse ID if available
        if Config.SQL_WAREHOUSE_ID:
            print(f"[DEBUG] Using configured SQL warehouse: {Config.SQL_WAREHOUSE_ID}")
            return Config.SQL_WAREHOUSE_ID

        # Fallback: dynamically list warehouses
        try:
            print("[DEBUG] No configured warehouse, listing available warehouses...")
            client = self._get_client()
            warehouses = list(client.warehouses.list())
            for wh in warehouses:
                if wh.state and wh.state.value == "RUNNING":
                    return wh.id
            if warehouses:
                return warehouses[0].id
        except Exception as e:
            print(f"Error getting warehouse: {e}")
        return None

    def execute_sql(self, statement: str) -> List[Any]:
        """Execute SQL using Statement Execution API."""
        client = self._get_client()
        warehouse_id = self.get_sql_warehouse_id()
        if not warehouse_id:
            raise Exception("No SQL warehouse available")

        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            wait_timeout="30s"
        )

        if response.result and response.result.data_array:
            return [row[0] for row in response.result.data_array]
        return []

    # ============================================================
    # Unity Catalog Operations
    # ============================================================

    def get_catalogs(self) -> List[str]:
        """Get list of available catalogs (uses user's permissions)."""
        try:
            catalogs = self.execute_sql("SHOW CATALOGS")
            return catalogs if catalogs else ["main"]
        except Exception as e:
            print(f"Error listing catalogs: {e}")
            try:
                client = self._get_client()
                catalogs = list(client.catalogs.list())
                return [c.name for c in catalogs if c.name]
            except:
                return ["main"]

    def get_schemas(self, catalog: str) -> List[str]:
        """Get list of schemas in a catalog (uses user's permissions)."""
        try:
            schemas = self.execute_sql(f"SHOW SCHEMAS IN `{catalog}`")
            return schemas if schemas else ["default"]
        except Exception as e:
            print(f"Error listing schemas: {e}")
            try:
                client = self._get_client()
                schemas = list(client.schemas.list(catalog_name=catalog))
                return [s.name for s in schemas if s.name]
            except:
                return ["default"]

    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Get list of tables in a schema (uses user's permissions)."""
        try:
            client = self._get_client()
            warehouse_id = self.get_sql_warehouse_id()
            if not warehouse_id:
                raise Exception("No SQL warehouse available")

            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SHOW TABLES IN `{catalog}`.`{schema}`",
                wait_timeout="30s"
            )

            if response.result and response.result.data_array:
                return [row[1] for row in response.result.data_array]
            return []
        except Exception as e:
            print(f"Error listing tables: {e}")
            try:
                client = self._get_client()
                tables = list(client.tables.list(catalog_name=catalog, schema_name=schema))
                return [t.name for t in tables if t.name]
            except:
                return []

    def get_table_sample(self, full_table_name: str, limit: int = 100) -> Dict[str, Any]:
        """Get sample data from a table (uses user's permissions)."""
        try:
            client = self._get_client()
            warehouse_id = self.get_sql_warehouse_id()
            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT * FROM {full_table_name} LIMIT {limit}",
                wait_timeout="30s"
            )

            if response.result and response.manifest:
                columns = [col.name for col in response.manifest.schema.columns]
                rows = []
                if response.result.data_array:
                    for row in response.result.data_array:
                        rows.append(dict(zip(columns, row)))
                return {"columns": columns, "rows": rows, "row_count": len(rows)}

            return {"columns": [], "rows": [], "row_count": 0}
        except Exception as e:
            print(f"Error getting sample data: {e}")
            return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}

    # ============================================================
    # Job Operations
    # ============================================================

    def trigger_dq_job(self, table_name: str, user_prompt: str, sample_limit: Optional[int] = None) -> Dict[str, Any]:
        """Trigger the DQ rule generation job (uses user's permissions).

        Args:
            table_name: Full table name (catalog.schema.table)
            user_prompt: User's description of desired DQ rules
            sample_limit: Optional row limit for data profiling. If None, uses all rows.
        """
        if not Config.DQ_GENERATION_JOB_ID:
            return {"error": "DQ_GENERATION_JOB_ID not configured"}

        try:
            client = self._get_client()
            job_parameters = {
                "table_name": table_name,
                "user_prompt": user_prompt
            }

            # Only pass sample_limit if specified (empty string means use all rows)
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
        """Trigger the DQ rule validation job (uses user's permissions)."""
        if not Config.DQ_VALIDATION_JOB_ID:
            return {"error": "DQ_VALIDATION_JOB_ID not configured"}

        try:
            client = self._get_client()
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
        """Get job run status (uses user's permissions)."""
        try:
            client = self._get_client()
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
                            except:
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
                    except:
                        result = output.notebook_output.result
            except Exception as e:
                print(f"Error getting run output: {e}")

        return result


# Service instance (no longer singleton since each request uses different user token)
databricks_service = DatabricksService()
