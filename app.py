"""
Data Quality Rule Generator - Flask Application
================================================
A Databricks App for generating DQ rules using AI assistance.

Step 1: Select a table from Unity Catalog
Step 2: Enter requirements and trigger DQ rule generation job
Step 3: Review and edit generated rules
Step 4: Confirm and save rules to Lakebase with AI summary
"""

from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import os
import json
import uuid
from datetime import datetime
from databricks.sdk import WorkspaceClient

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dq-rule-generator-secret-key")

# Configuration
DQ_GENERATION_JOB_ID = os.getenv("DQ_GENERATION_JOB_ID")
SAMPLE_DATA_LIMIT = int(os.getenv("SAMPLE_DATA_LIMIT", "100"))

# Lakebase Configuration (OAuth-based)
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST")
LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE", "databricks_postgres")

# Model Serving Endpoint for AgentBricks
MODEL_SERVING_ENDPOINT = os.getenv("MODEL_SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")

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


def execute_sql(statement):
    """Execute SQL using Statement Execution API."""
    ws = get_workspace_client()
    warehouse_id = get_sql_warehouse_id()

    if not warehouse_id:
        raise Exception("No SQL warehouse available")

    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s"
    )

    if response.result and response.result.data_array:
        return [row[0] for row in response.result.data_array]
    return []


def get_catalogs():
    """Get list of available catalogs."""
    try:
        catalogs = execute_sql("SHOW CATALOGS")
        return catalogs if catalogs else ["main"]
    except Exception as e:
        print(f"Error listing catalogs: {e}")
        # Fallback to SDK
        try:
            ws = get_workspace_client()
            catalogs = list(ws.catalogs.list())
            return [c.name for c in catalogs if c.name]
        except:
            return ["main"]


def get_schemas(catalog):
    """Get list of schemas in a catalog."""
    try:
        schemas = execute_sql(f"SHOW SCHEMAS IN `{catalog}`")
        return schemas if schemas else ["default"]
    except Exception as e:
        print(f"Error listing schemas: {e}")
        # Fallback to SDK
        try:
            ws = get_workspace_client()
            schemas = list(ws.schemas.list(catalog_name=catalog))
            return [s.name for s in schemas if s.name]
        except:
            return ["default"]


def get_tables(catalog, schema):
    """Get list of tables in a schema."""
    try:
        # SHOW TABLES returns: database, tableName, isTemporary
        # We need the second column (index 1) for table names
        ws = get_workspace_client()
        warehouse_id = get_sql_warehouse_id()

        if not warehouse_id:
            raise Exception("No SQL warehouse available")

        response = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SHOW TABLES IN `{catalog}`.`{schema}`",
            wait_timeout="30s"
        )

        if response.result and response.result.data_array:
            # Table name is in the second column (index 1)
            return [row[1] for row in response.result.data_array]
        return []
    except Exception as e:
        print(f"Error listing tables: {e}")
        # Fallback to SDK
        try:
            ws = get_workspace_client()
            tables = list(ws.tables.list(catalog_name=catalog, schema_name=schema))
            return [t.name for t in tables if t.name]
        except:
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


# ============================================================
# AGENTBRICKS - DQ RULE ANALYZER TOOL
# ============================================================

def analyze_dq_rules_with_agent(rules, table_name, user_prompt):
    """
    AgentBricks tool that analyzes DQ rules and provides summary with reasoning.
    Uses ai_query() via SQL Statement Execution API.
    """
    try:
        import re

        ws = get_workspace_client()
        warehouse_id = get_sql_warehouse_id()

        if not warehouse_id:
            raise Exception("No SQL warehouse available")

        # Build the analysis prompt
        rules_json = json.dumps(rules, indent=2)

        # Escape single quotes for SQL
        rules_escaped = rules_json.replace("'", "''").replace("\\", "\\\\")
        table_escaped = table_name.replace("'", "''")
        prompt_escaped = user_prompt.replace("'", "''")

        analysis_prompt = f"""You are a Data Quality expert. Analyze the following DQ rules generated for table '{table_escaped}'.

User's original requirement: {prompt_escaped}

Generated DQ Rules:
{rules_escaped}

Please provide:
1. **Summary**: A concise summary of what these rules check (2-3 sentences)
2. **Rule Analysis**: For each rule, explain what it checks, why it's important, and the criticality justification
3. **Coverage Assessment**: How well do these rules cover the user's requirements?
4. **Recommendations**: Any additional rules that might be beneficial

Format your response as JSON with this structure:
{{"summary": "...", "rule_analysis": [...], "coverage_assessment": "...", "recommendations": [...], "overall_quality_score": 1-10}}"""

        # Escape for SQL string
        prompt_sql_escaped = analysis_prompt.replace("'", "''")

        # Call ai_query via SQL Statement Execution
        sql = f"""
        SELECT ai_query(
            '{MODEL_SERVING_ENDPOINT}',
            '{prompt_sql_escaped}'
        ) as analysis
        """

        response = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="120s"
        )

        if response.result and response.result.data_array and len(response.result.data_array) > 0:
            content = response.result.data_array[0][0]

            # Try to extract JSON from the response
            try:
                json_match = re.search(r'\{[\s\S]*\}', content)
                if json_match:
                    analysis = json.loads(json_match.group())
                    return {"success": True, "analysis": analysis}
            except json.JSONDecodeError:
                pass

            # If JSON parsing fails, return raw content
            return {"success": True, "analysis": {"summary": content, "raw_response": True}}

        return {"success": False, "error": "No response from ai_query"}

    except Exception as e:
        print(f"Error analyzing rules with agent: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# LAKEBASE - DQ RULES STORAGE (OAuth-based)
# ============================================================

def get_user_oauth_credentials():
    """Get user OAuth credentials from request headers."""
    user_token = request.headers.get('x-forwarded-access-token')
    user_email = request.headers.get('x-forwarded-email')

    if not user_token:
        raise Exception("No OAuth token found. User must be authenticated via Databricks Apps.")

    if not user_email:
        # Try to get user email from Databricks SDK using the token
        try:
            from databricks.sdk import WorkspaceClient
            ws = WorkspaceClient(token=user_token)
            current_user = ws.current_user.me()
            user_email = current_user.user_name
        except Exception as e:
            raise Exception(f"Could not determine user email: {e}")

    return user_email, user_token


def get_lakebase_connection():
    """Get PostgreSQL connection to Lakebase using OAuth."""
    import psycopg2

    if not LAKEBASE_HOST:
        raise Exception("Lakebase connection not configured. Set LAKEBASE_HOST in app.yaml")

    user_email, user_token = get_user_oauth_credentials()

    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        database=LAKEBASE_DATABASE,
        user=user_email,
        password=user_token,
        port=5432,
        sslmode='require'
    )
    return conn


def init_lakebase_table():
    """Initialize the DQ rules table in Lakebase if it doesn't exist."""
    try:
        conn = get_lakebase_connection()
        cursor = conn.cursor()

        # Create table for storing DQ rules with versioning
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

        # Create index for faster queries
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


def get_next_version(table_name):
    """Get the next version number for a table's DQ rules."""
    try:
        conn = get_lakebase_connection()
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


def save_dq_rules_to_lakebase(table_name, rules, user_prompt, ai_summary, metadata=None):
    """Save DQ rules to Lakebase with versioning."""
    try:
        # Initialize table if needed
        init_lakebase_table()

        conn = get_lakebase_connection()
        cursor = conn.cursor()

        # Get next version
        version = get_next_version(table_name)

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


def get_dq_rules_history(table_name, limit=10):
    """Get history of DQ rules for a table."""
    try:
        conn = get_lakebase_connection()
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


# ============================================================
# JOB TRIGGER API
# ============================================================

def trigger_dq_job(table_name, user_prompt):
    """Trigger the DQ rule generation job."""
    if not DQ_GENERATION_JOB_ID:
        return {"error": "DQ_GENERATION_JOB_ID not configured"}

    try:
        ws = get_workspace_client()
        # Use job_parameters instead of notebook_params for jobs with parameters configured
        response = ws.jobs.run_now(
            job_id=int(DQ_GENERATION_JOB_ID),
            job_parameters={
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
                result = None

                # For multi-task jobs, get output from individual tasks
                if run.tasks and len(run.tasks) > 0:
                    # Find the notebook task and get its output
                    for task in run.tasks:
                        if task.run_id:
                            try:
                                task_output = ws.jobs.get_run_output(run_id=task.run_id)
                                if task_output.notebook_output and task_output.notebook_output.result:
                                    try:
                                        result = json.loads(task_output.notebook_output.result)
                                    except:
                                        result = task_output.notebook_output.result
                                    break  # Found output, stop looking
                            except Exception as task_err:
                                print(f"Error getting task output: {task_err}")
                                continue
                else:
                    # Single task job - get output directly
                    try:
                        output = ws.jobs.get_run_output(run_id=int(run_id))
                        if output.notebook_output and output.notebook_output.result:
                            try:
                                result = json.loads(output.notebook_output.result)
                            except:
                                result = output.notebook_output.result
                    except Exception as e:
                        print(f"Error getting run output: {e}")

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
# STEP 4: CONFIRM AND SAVE ROUTES
# ============================================================

@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """API: Analyze DQ rules using AgentBricks tool."""
    data = request.json
    rules = data.get("rules", [])
    table_name = data.get("table_name", "")
    user_prompt = data.get("user_prompt", "")

    if not rules:
        return jsonify({"success": False, "error": "No rules provided"}), 400

    result = analyze_dq_rules_with_agent(rules, table_name, user_prompt)
    return jsonify(result)


@app.route("/api/confirm", methods=["POST"])
def api_confirm():
    """API: Confirm and save DQ rules to Lakebase."""
    data = request.json
    rules = data.get("rules", [])
    table_name = data.get("table_name", "")
    user_prompt = data.get("user_prompt", "")
    ai_summary = data.get("ai_summary")
    metadata = data.get("metadata")

    if not rules:
        return jsonify({"success": False, "error": "No rules provided"}), 400

    if not table_name:
        return jsonify({"success": False, "error": "No table name provided"}), 400

    # Save to Lakebase
    result = save_dq_rules_to_lakebase(
        table_name=table_name,
        rules=rules,
        user_prompt=user_prompt,
        ai_summary=ai_summary,
        metadata=metadata
    )

    return jsonify(result)


@app.route("/api/history/<path:table_name>")
def api_history(table_name):
    """API: Get DQ rules history for a table."""
    limit = request.args.get("limit", 10, type=int)
    result = get_dq_rules_history(table_name, limit)
    return jsonify(result)


@app.route("/api/lakebase/status")
def api_lakebase_status():
    """API: Check Lakebase connection status (OAuth-based)."""
    try:
        if not LAKEBASE_HOST:
            return jsonify({
                "connected": False,
                "configured": False,
                "message": "Lakebase host not configured"
            })

        # Check if user has OAuth token
        user_token = request.headers.get('x-forwarded-access-token')
        if not user_token:
            return jsonify({
                "connected": False,
                "configured": True,
                "message": "No OAuth token - user must be authenticated via Databricks Apps"
            })

        conn = get_lakebase_connection()
        conn.close()

        user_email, _ = get_user_oauth_credentials()
        return jsonify({
            "connected": True,
            "configured": True,
            "host": LAKEBASE_HOST,
            "database": LAKEBASE_DATABASE,
            "auth_type": "oauth",
            "user": user_email
        })
    except Exception as e:
        return jsonify({
            "connected": False,
            "configured": True,
            "error": str(e)
        })


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
