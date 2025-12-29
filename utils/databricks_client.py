"""
Databricks client utilities for workspace operations.
"""
from typing import List, Dict, Any, Optional
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo
from databricks.sdk.service.jobs import RunNowResponse, Run, RunLifeCycleState

from config import config

logger = logging.getLogger(__name__)


class DatabricksClientManager:
    """Manager class for Databricks workspace operations."""

    def __init__(self):
        """Initialize the Databricks workspace client."""
        self._client: Optional[WorkspaceClient] = None

    @property
    def client(self) -> WorkspaceClient:
        """Get or create the workspace client."""
        if self._client is None:
            self._client = WorkspaceClient(
                host=config.DATABRICKS_HOST,
                token=config.DATABRICKS_TOKEN
            )
        return self._client

    def list_catalogs(self) -> List[str]:
        """List all available catalogs."""
        try:
            catalogs = self.client.catalogs.list()
            return [c.name for c in catalogs if c.name]
        except Exception as e:
            logger.error(f"Error listing catalogs: {e}")
            return []

    def list_schemas(self, catalog: str) -> List[str]:
        """List all schemas in a catalog."""
        try:
            schemas = self.client.schemas.list(catalog_name=catalog)
            return [s.name for s in schemas if s.name]
        except Exception as e:
            logger.error(f"Error listing schemas for catalog {catalog}: {e}")
            return []

    def list_tables(self, catalog: str, schema: str) -> List[str]:
        """List all tables in a catalog.schema."""
        try:
            tables = self.client.tables.list(
                catalog_name=catalog,
                schema_name=schema
            )
            return [t.name for t in tables if t.name]
        except Exception as e:
            logger.error(f"Error listing tables for {catalog}.{schema}: {e}")
            return []

    def get_table_info(self, full_table_name: str) -> Optional[TableInfo]:
        """Get detailed information about a table."""
        try:
            return self.client.tables.get(full_name=full_table_name)
        except Exception as e:
            logger.error(f"Error getting table info for {full_table_name}: {e}")
            return None

    def trigger_notebook_job(
        self,
        job_id: str,
        parameters: Dict[str, str]
    ) -> Optional[RunNowResponse]:
        """Trigger a notebook job with parameters."""
        try:
            response = self.client.jobs.run_now(
                job_id=int(job_id),
                notebook_params=parameters
            )
            logger.info(f"Triggered job {job_id}, run_id: {response.run_id}")
            return response
        except Exception as e:
            logger.error(f"Error triggering job {job_id}: {e}")
            return None

    def get_job_run_status(self, run_id: int) -> Optional[Run]:
        """Get the status of a job run."""
        try:
            return self.client.jobs.get_run(run_id=run_id)
        except Exception as e:
            logger.error(f"Error getting run status for {run_id}: {e}")
            return None

    def wait_for_job_completion(
        self,
        run_id: int,
        timeout_seconds: int = 600,
        poll_interval: int = 10
    ) -> Optional[Run]:
        """Wait for a job run to complete."""
        import time

        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            run = self.get_job_run_status(run_id)
            if run is None:
                return None

            state = run.state
            if state and state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR
            ]:
                return run

            time.sleep(poll_interval)

        logger.warning(f"Job run {run_id} timed out after {timeout_seconds}s")
        return self.get_job_run_status(run_id)

    def get_job_run_output(self, run_id: int) -> Optional[str]:
        """Get the output of a job run."""
        try:
            output = self.client.jobs.get_run_output(run_id=run_id)
            if output.notebook_output and output.notebook_output.result:
                return output.notebook_output.result
            return None
        except Exception as e:
            logger.error(f"Error getting run output for {run_id}: {e}")
            return None


# Global client instance
databricks_client = DatabricksClientManager()
