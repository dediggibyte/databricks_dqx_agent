"""
Data Quality Rule Generator - Flask Application
================================================
A Databricks App for generating DQ rules using AI assistance.

Step 1: Select a table from Unity Catalog
Step 2: Enter requirements and trigger DQ rule generation job
"""

from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import os
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dq-rule-generator-secret-key")

# Configuration
DQ_GENERATION_JOB_ID = os.getenv("DQ_GENERATION_JOB_ID")
SAMPLE_DATA_LIMIT = int(os.getenv("SAMPLE_DATA_LIMIT", "100"))

# Global workspace client
_workspace_client = None


def get_workspace_client():
    """Get or create WorkspaceClient for Databricks API calls."""
    global _workspace_client
    if _workspace_client is None:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        if host and token:
            _workspace_client = WorkspaceClient(host=host, token=token)
        else:
            _workspace_client = WorkspaceClient()

    return _workspace_client


# ============================================================
# UNITY CATALOG API
# ============================================================

def get_catalogs():
    """Get list of available catalogs."""
    try:
        ws = get_workspace_client()
        catalogs = list(ws.catalogs.list())
        return [c.name for c in catalogs if c.name]
    except Exception as e:
        print(f"Error listing catalogs: {e}")
        return ["main"]


def get_schemas(catalog):
    """Get list of schemas in a catalog."""
    try:
        ws = get_workspace_client()
        schemas = list(ws.schemas.list(catalog_name=catalog))
        return [s.name for s in schemas if s.name]
    except Exception as e:
        print(f"Error listing schemas: {e}")
        return ["default"]


def get_tables(catalog, schema):
    """Get list of tables in a schema."""
    try:
        ws = get_workspace_client()
        tables = list(ws.tables.list(catalog_name=catalog, schema_name=schema))
        return [t.name for t in tables if t.name]
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []


def get_table_sample(full_table_name, limit=100):
    """Get sample data from a table using Statement Execution API."""
    try:
        ws = get_workspace_client()

        # Use Statement Execution API for serverless SQL
        response = ws.statement_execution.execute_statement(
            warehouse_id=get_sql_warehouse_id(),
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


def get_sql_warehouse_id():
    """Get a SQL warehouse ID for executing queries."""
    try:
        ws = get_workspace_client()
        warehouses = list(ws.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                return wh.id
        # Return first warehouse if none running
        if warehouses:
            return warehouses[0].id
    except Exception as e:
        print(f"Error getting warehouse: {e}")
    return None


# ============================================================
# JOB TRIGGER API
# ============================================================

def trigger_dq_job(table_name, user_prompt):
    """Trigger the DQ rule generation job."""
    if not DQ_GENERATION_JOB_ID:
        return {"error": "DQ_GENERATION_JOB_ID not configured"}

    try:
        ws = get_workspace_client()
        response = ws.jobs.run_now(
            job_id=int(DQ_GENERATION_JOB_ID),
            notebook_params={
                "table_name": table_name,
                "user_prompt": user_prompt
            }
        )
        return {"run_id": response.run_id}
    except Exception as e:
        return {"error": str(e)}


def get_job_status(run_id):
    """Get job run status."""
    try:
        ws = get_workspace_client()
        run = ws.jobs.get_run(run_id=int(run_id))
        state = run.state

        from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

        if state.life_cycle_state == RunLifeCycleState.TERMINATED:
            if state.result_state == RunResultState.SUCCESS:
                output = ws.jobs.get_run_output(run_id=int(run_id))
                result = None
                if output.notebook_output and output.notebook_output.result:
                    try:
                        result = json.loads(output.notebook_output.result)
                    except:
                        result = output.notebook_output.result
                return {"status": "completed", "result": result}
            else:
                return {"status": "failed", "message": state.state_message}

        elif state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR:
            return {"status": "error", "message": state.state_message}

        else:
            return {"status": "running", "state": str(state.life_cycle_state)}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():
    """Main page."""
    return render_template("index.html", job_id=DQ_GENERATION_JOB_ID)


@app.route("/api/catalogs")
def api_catalogs():
    """API: Get catalogs."""
    return jsonify(get_catalogs())


@app.route("/api/schemas/<catalog>")
def api_schemas(catalog):
    """API: Get schemas for a catalog."""
    return jsonify(get_schemas(catalog))


@app.route("/api/tables/<catalog>/<schema>")
def api_tables(catalog, schema):
    """API: Get tables for a schema."""
    return jsonify(get_tables(catalog, schema))


@app.route("/api/sample/<catalog>/<schema>/<table>")
def api_sample(catalog, schema, table):
    """API: Get sample data from a table."""
    full_table_name = f"{catalog}.{schema}.{table}"
    return jsonify(get_table_sample(full_table_name, SAMPLE_DATA_LIMIT))


@app.route("/api/generate", methods=["POST"])
def api_generate():
    """API: Trigger DQ rule generation job."""
    data = request.json
    table_name = data.get("table_name")
    user_prompt = data.get("user_prompt")

    if not table_name or not user_prompt:
        return jsonify({"error": "Missing table_name or user_prompt"}), 400

    result = trigger_dq_job(table_name, user_prompt)
    return jsonify(result)


@app.route("/api/status/<run_id>")
def api_status(run_id):
    """API: Get job status."""
    return jsonify(get_job_status(run_id))


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
