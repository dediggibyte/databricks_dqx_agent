"""
Databricks Data Quality App - Step 1 & Step 2

Step 1: Table selection with sample data preview
Step 2: Prompt submission to trigger DQ rule generation via Databricks Job
"""
import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
from typing import List, Any, Optional

# Page configuration
st.set_page_config(
    page_title="Data Quality Rule Generator",
    page_icon="🔍",
    layout="wide"
)


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


def poll_job_status(ws: Any, run_id: int) -> dict:
    """Poll job status and return result when complete."""
    from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

    try:
        run = ws.jobs.get_run(run_id=run_id)
        state = run.state

        if state.life_cycle_state == RunLifeCycleState.TERMINATED:
            if state.result_state == RunResultState.SUCCESS:
                # Get the output
                output = ws.jobs.get_run_output(run_id=run_id)
                if output.notebook_output and output.notebook_output.result:
                    return {
                        "status": "completed",
                        "result": json.loads(output.notebook_output.result)
                    }
                return {"status": "completed", "result": None}
            else:
                return {
                    "status": "failed",
                    "message": f"Job failed: {state.state_message}"
                }

        elif state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR:
            return {"status": "error", "message": str(state.state_message)}

        else:
            return {"status": "running", "state": str(state.life_cycle_state)}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# Initialize session state
if "generated_rules" not in st.session_state:
    st.session_state.generated_rules = None
if "job_run_id" not in st.session_state:
    st.session_state.job_run_id = None

# Main App
st.title("Data Quality Rule Generator")
st.markdown("Generate data quality rules using AI assistance")

# Initialize Databricks client
ws = init_databricks_client()

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")

    import os
    job_id = st.text_input(
        "DQ Generation Job ID",
        value=os.getenv("DQ_GENERATION_JOB_ID", ""),
        help="Enter the Databricks Job ID for DQ rule generation notebook"
    )

    sample_limit = st.number_input(
        "Sample Data Rows",
        min_value=10,
        max_value=1000,
        value=100,
        help="Number of rows to preview"
    )

    st.divider()
    st.markdown("### How to Use")
    st.markdown("""
    1. Select a table from the dropdowns
    2. Review the sample data
    3. Enter your DQ requirements
    4. Click Generate to create rules
    """)

# Main content
if ws is None:
    st.error("Cannot connect to Databricks. Please check your configuration.")
    st.stop()

# ============================================
# STEP 1: Table Selection with Sample Data
# ============================================
st.header("Step 1: Select a Table")

col1, col2, col3 = st.columns(3)

with col1:
    catalogs = get_catalogs(ws)
    selected_catalog = st.selectbox(
        "Catalog",
        catalogs,
        index=0 if catalogs else None
    )

with col2:
    schemas = get_schemas(ws, selected_catalog) if selected_catalog else []
    selected_schema = st.selectbox(
        "Schema",
        schemas,
        index=0 if schemas else None
    )

with col3:
    tables = get_tables(ws, selected_catalog, selected_schema) if selected_schema else []
    selected_table = st.selectbox(
        "Table",
        tables,
        index=0 if tables else None
    )

# Display sample data when table is selected
if selected_catalog and selected_schema and selected_table:
    full_table_name = f"{selected_catalog}.{selected_schema}.{selected_table}"

    st.subheader("Sample Data Preview")

    with st.spinner(f"Loading sample data from `{full_table_name}`..."):
        sample_df = get_sample_data(full_table_name, limit=sample_limit)

    if not sample_df.empty:
        # Show table info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Columns", len(sample_df.columns))
        with col2:
            st.metric("Rows (sample)", len(sample_df))
        with col3:
            st.metric("Table", selected_table)

        # Show data
        st.dataframe(sample_df, use_container_width=True, height=300)

        # Show column info
        with st.expander("Column Information"):
            col_info = pd.DataFrame({
                "Column": sample_df.columns,
                "Type": sample_df.dtypes.astype(str),
                "Non-Null Count": sample_df.count().values,
                "Null Count": sample_df.isnull().sum().values
            })
            st.dataframe(col_info, use_container_width=True)

    else:
        st.warning("No data available or unable to load sample data.")

    # ============================================
    # STEP 2: Prompt Input and Job Trigger
    # ============================================
    st.header("Step 2: Generate Data Quality Rules")

    user_prompt = st.text_area(
        "Describe the data quality rules you need",
        placeholder="""Examples:
- Ensure email column contains valid email addresses
- Check that amounts are positive numbers
- Verify dates are not in the future
- Make sure customer_id is not null and unique
- Validate phone numbers match expected format""",
        height=150
    )

    # Show column names for reference
    if not sample_df.empty:
        st.info(f"**Available columns:** {', '.join(sample_df.columns.tolist())}")

    # Generate button
    col1, col2 = st.columns([1, 3])

    with col1:
        generate_btn = st.button(
            "Generate DQ Rules",
            type="primary",
            disabled=not job_id or not user_prompt
        )

    if not job_id:
        st.warning("Please enter the DQ Generation Job ID in the sidebar.")

    if generate_btn and user_prompt and job_id:
        with st.spinner("Triggering DQ rule generation job..."):
            run_id = trigger_dq_generation_job(
                ws,
                full_table_name,
                user_prompt,
                job_id
            )

            if run_id:
                st.session_state.job_run_id = run_id
                st.info(f"Job triggered! Run ID: **{run_id}**")

                # Create progress indicators
                progress_bar = st.progress(0)
                status_container = st.empty()

                # Poll for completion
                max_polls = 120  # 10 minutes max (5s intervals)
                for i in range(max_polls):
                    status = poll_job_status(ws, run_id)

                    if status["status"] == "completed":
                        progress_bar.progress(100)
                        status_container.success("DQ rules generated successfully!")
                        st.session_state.generated_rules = status.get("result")
                        break

                    elif status["status"] in ["failed", "error"]:
                        progress_bar.empty()
                        status_container.error(f"Job failed: {status.get('message')}")
                        break

                    else:
                        progress_pct = min(95, (i + 1) * 1)
                        progress_bar.progress(progress_pct)
                        status_container.info(
                            f"Job running... Status: {status.get('state', 'processing')}"
                        )
                        time.sleep(5)
                else:
                    status_container.warning(
                        "Job is taking longer than expected. "
                        f"Check Jobs UI for Run ID: {run_id}"
                    )

    # ============================================
    # Display Generated Rules (Output of Step 2)
    # ============================================
    if st.session_state.generated_rules:
        st.header("Generated DQ Rules")

        rules = st.session_state.generated_rules

        # Display summary if available
        if isinstance(rules, dict):
            if "summary" in rules:
                st.subheader("Summary")
                st.markdown(rules["summary"])

            # Display rules as formatted JSON
            if "rules" in rules:
                st.subheader("Rule Definitions")
                st.json(rules["rules"])

            # Display metadata
            if "metadata" in rules:
                with st.expander("Generation Metadata"):
                    st.json(rules["metadata"])
        else:
            st.json(rules)

        # Provide download option
        st.download_button(
            "Download Rules JSON",
            data=json.dumps(rules, indent=2),
            file_name=f"dq_rules_{selected_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

        # Reset button
        if st.button("Generate New Rules", type="secondary"):
            st.session_state.generated_rules = None
            st.session_state.job_run_id = None
            st.rerun()

else:
    st.info("Please select a catalog, schema, and table to get started.")

# Footer
st.divider()
st.caption(
    "Powered by [Databricks DQX](https://databrickslabs.github.io/dqx/) | "
    "Data Quality at Scale"
)
