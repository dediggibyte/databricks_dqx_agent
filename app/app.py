"""
Databricks Data Quality App - Main Application

This Streamlit app provides:
1. Table selection with sample data preview
2. Prompt-based DQ rule generation
3. Rule editing and confirmation
4. Rule persistence to Lakebase with versioning
"""
import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

# Initialize session state
if "generated_rules" not in st.session_state:
    st.session_state.generated_rules = None
if "rule_summary" not in st.session_state:
    st.session_state.rule_summary = None
if "job_status" not in st.session_state:
    st.session_state.job_status = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "edited_rules" not in st.session_state:
    st.session_state.edited_rules = None


def init_databricks_client():
    """Initialize Databricks client for the app."""
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except Exception as e:
        st.error(f"Failed to initialize Databricks client: {e}")
        return None


def get_catalogs(ws: Any) -> List[str]:
    """Get list of available catalogs."""
    try:
        catalogs = list(ws.catalogs.list())
        return [c.name for c in catalogs if c.name]
    except Exception as e:
        st.warning(f"Could not list catalogs: {e}")
        return ["main"]


def get_schemas(ws: Any, catalog: str) -> List[str]:
    """Get list of schemas in a catalog."""
    try:
        schemas = list(ws.schemas.list(catalog_name=catalog))
        return [s.name for s in schemas if s.name]
    except Exception as e:
        st.warning(f"Could not list schemas: {e}")
        return ["default"]


def get_tables(ws: Any, catalog: str, schema: str) -> List[str]:
    """Get list of tables in a schema."""
    try:
        tables = list(ws.tables.list(catalog_name=catalog, schema_name=schema))
        return [t.name for t in tables if t.name]
    except Exception as e:
        st.warning(f"Could not list tables: {e}")
        return []


def get_sample_data(full_table_name: str, limit: int = 100) -> pd.DataFrame:
    """Get sample data from a table using Spark."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.table(full_table_name).limit(limit)
        return df.toPandas()
    except Exception as e:
        st.error(f"Error loading sample data: {e}")
        return pd.DataFrame()


def trigger_dq_generation_job(
    ws: Any,
    table_name: str,
    prompt: str,
    job_id: str
) -> Optional[int]:
    """Trigger the DQ rule generation notebook job."""
    try:
        response = ws.jobs.run_now(
            job_id=int(job_id),
            notebook_params={
                "table_name": table_name,
                "user_prompt": prompt,
                "timestamp": datetime.now().isoformat()
            }
        )
        return response.run_id
    except Exception as e:
        st.error(f"Failed to trigger job: {e}")
        return None


def poll_job_status(ws: Any, run_id: int) -> Dict[str, Any]:
    """Poll job status and return result when complete."""
    from databricks.sdk.service.jobs import RunLifeCycleState

    try:
        run = ws.jobs.get_run(run_id=run_id)
        state = run.state

        if state.life_cycle_state == RunLifeCycleState.TERMINATED:
            # Get the output
            output = ws.jobs.get_run_output(run_id=run_id)
            if output.notebook_output and output.notebook_output.result:
                return {
                    "status": "completed",
                    "result": json.loads(output.notebook_output.result)
                }
            return {"status": "completed", "result": None}

        elif state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR:
            return {"status": "error", "message": str(state.state_message)}

        else:
            return {"status": "running", "state": str(state.life_cycle_state)}

    except Exception as e:
        return {"status": "error", "message": str(e)}


def save_rule_to_lakebase(
    rule_data: Dict[str, Any],
    table_name: str,
    version: int
) -> bool:
    """Save the DQ rule to Lakebase."""
    try:
        import psycopg
        from databricks.sdk.core import Config

        db_config = Config()

        # Get Lakebase connection details from environment
        import os
        lakebase_host = os.getenv("LAKEBASE_HOST", "")
        lakebase_db = os.getenv("LAKEBASE_DATABASE", "dqx_rules_db")

        conn = psycopg.connect(
            host=lakebase_host,
            dbname=lakebase_db,
            user=db_config.client_id if db_config.client_id else os.getenv("LAKEBASE_USER"),
            password=db_config.token if db_config.token else os.getenv("LAKEBASE_PASSWORD"),
            sslmode="require"
        )

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dq_rules.dq_rule_events
                (table_name, rule_definition, version, created_at, created_by, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                table_name,
                json.dumps(rule_data),
                version,
                datetime.now(),
                os.getenv("DATABRICKS_USER", "system"),
                "active"
            ))
            conn.commit()

        conn.close()
        return True

    except Exception as e:
        st.error(f"Failed to save rule to Lakebase: {e}")
        return False


def get_rule_version(table_name: str) -> int:
    """Get the next version number for a table's rules."""
    try:
        import psycopg
        from databricks.sdk.core import Config
        import os

        db_config = Config()
        lakebase_host = os.getenv("LAKEBASE_HOST", "")
        lakebase_db = os.getenv("LAKEBASE_DATABASE", "dqx_rules_db")

        conn = psycopg.connect(
            host=lakebase_host,
            dbname=lakebase_db,
            user=db_config.client_id if db_config.client_id else os.getenv("LAKEBASE_USER"),
            password=db_config.token if db_config.token else os.getenv("LAKEBASE_PASSWORD"),
            sslmode="require"
        )

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(MAX(version), 0) + 1
                FROM dq_rules.dq_rule_events
                WHERE table_name = %s
            """, (table_name,))
            result = cur.fetchone()

        conn.close()
        return result[0] if result else 1

    except Exception:
        return 1


# Page configuration
st.set_page_config(
    page_title="Data Quality Rule Generator",
    page_icon="🔍",
    layout="wide"
)

st.title("Data Quality Rule Generator")
st.markdown("Generate, edit, and manage data quality rules using AI")

# Initialize Databricks client
ws = init_databricks_client()

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")

    # Job ID input
    import os
    job_id = st.text_input(
        "DQ Generation Job ID",
        value=os.getenv("DQ_GENERATION_JOB_ID", ""),
        help="Enter the Databricks Job ID for DQ rule generation"
    )

    st.divider()
    st.markdown("### About")
    st.markdown("""
    This app uses Databricks DQX to generate data quality rules
    based on your table data and natural language prompts.
    """)

# Main content area
if ws is None:
    st.error("Cannot connect to Databricks. Please check your configuration.")
    st.stop()

# Step 1: Table Selection
st.header("Step 1: Select a Table")

col1, col2, col3 = st.columns(3)

with col1:
    catalogs = get_catalogs(ws)
    selected_catalog = st.selectbox("Catalog", catalogs, index=0)

with col2:
    schemas = get_schemas(ws, selected_catalog) if selected_catalog else []
    selected_schema = st.selectbox("Schema", schemas, index=0 if schemas else None)

with col3:
    tables = get_tables(ws, selected_catalog, selected_schema) if selected_schema else []
    selected_table = st.selectbox("Table", tables, index=0 if tables else None)

# Build full table name
if selected_catalog and selected_schema and selected_table:
    full_table_name = f"{selected_catalog}.{selected_schema}.{selected_table}"
    st.session_state.selected_table = full_table_name

    # Show sample data
    st.subheader("Sample Data Preview")

    with st.spinner("Loading sample data..."):
        sample_df = get_sample_data(full_table_name, limit=100)

    if not sample_df.empty:
        st.dataframe(sample_df, use_container_width=True, height=300)
        st.caption(f"Showing up to 100 rows from `{full_table_name}`")
    else:
        st.info("No data available or unable to load sample data.")

    # Step 2: Prompt for DQ Rule Generation
    st.header("Step 2: Generate Data Quality Rules")

    user_prompt = st.text_area(
        "Describe the data quality rules you need",
        placeholder="Example: Ensure email column contains valid email addresses, check that amounts are positive, verify dates are not in the future...",
        height=100
    )

    if st.button("Generate DQ Rules", type="primary", disabled=not job_id):
        if not user_prompt:
            st.warning("Please enter a prompt describing the data quality rules you need.")
        else:
            with st.spinner("Triggering DQ rule generation job..."):
                run_id = trigger_dq_generation_job(ws, full_table_name, user_prompt, job_id)

                if run_id:
                    st.info(f"Job triggered! Run ID: {run_id}")
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    # Poll for completion
                    max_polls = 60  # 5 minutes max
                    for i in range(max_polls):
                        status = poll_job_status(ws, run_id)

                        if status["status"] == "completed":
                            progress_bar.progress(100)
                            status_text.success("DQ rules generated successfully!")
                            st.session_state.generated_rules = status.get("result")
                            st.session_state.job_status = "completed"
                            break
                        elif status["status"] == "error":
                            progress_bar.empty()
                            status_text.error(f"Job failed: {status.get('message')}")
                            st.session_state.job_status = "error"
                            break
                        else:
                            progress_bar.progress(min(95, (i + 1) * 2))
                            status_text.info(f"Job running... ({status.get('state', 'processing')})")
                            time.sleep(5)
                    else:
                        status_text.warning("Job is taking longer than expected. Please check the Jobs UI.")

    # Step 3: Display and Edit Generated Rules
    if st.session_state.generated_rules:
        st.header("Step 3: Review and Edit DQ Rules")

        rules = st.session_state.generated_rules

        # Display rule summary from AgentBricks tool
        if isinstance(rules, dict) and "summary" in rules:
            st.subheader("Rule Summary")
            st.markdown(rules["summary"])

        # Editable rule definition
        st.subheader("Rule Definition (Editable)")

        rule_json = rules.get("rules", rules) if isinstance(rules, dict) else rules
        edited_rules_str = st.text_area(
            "Edit the JSON rule definition below:",
            value=json.dumps(rule_json, indent=2),
            height=400,
            key="rule_editor"
        )

        # Validate JSON
        try:
            edited_rules = json.loads(edited_rules_str)
            st.session_state.edited_rules = edited_rules
            st.success("Valid JSON")
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
            st.session_state.edited_rules = None

        # Step 4: Confirm and Save
        st.header("Step 4: Confirm and Save to Lakebase")

        col1, col2 = st.columns(2)

        with col1:
            if st.button(
                "Confirm and Save Rule",
                type="primary",
                disabled=st.session_state.edited_rules is None
            ):
                with st.spinner("Saving rule to Lakebase..."):
                    version = get_rule_version(full_table_name)
                    success = save_rule_to_lakebase(
                        st.session_state.edited_rules,
                        full_table_name,
                        version
                    )

                    if success:
                        st.success(f"Rule saved successfully! Version: {version}")
                        st.balloons()
                    else:
                        st.error("Failed to save rule. Please check your Lakebase connection.")

        with col2:
            if st.button("Reset", type="secondary"):
                st.session_state.generated_rules = None
                st.session_state.rule_summary = None
                st.session_state.edited_rules = None
                st.session_state.job_status = None
                st.rerun()

else:
    st.info("Please select a catalog, schema, and table to get started.")

# Footer
st.divider()
st.markdown(
    "*Powered by [Databricks DQX](https://databrickslabs.github.io/dqx/) | "
    "Data Quality at Scale*"
)
