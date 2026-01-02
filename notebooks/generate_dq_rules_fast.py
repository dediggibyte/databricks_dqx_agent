# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Generation (Optimized)
# MAGIC Fast version optimized for Databricks App integration.
# MAGIC
# MAGIC **Note:** The `databricks-labs-dqx[llm]` library is installed via job environment dependencies.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("user_prompt", "", "User Prompt")
dbutils.widgets.text("timestamp", "", "Timestamp")
dbutils.widgets.text("sample_limit", "", "Sample Limit (rows)")

table_name = dbutils.widgets.get("table_name")
user_prompt = dbutils.widgets.get("user_prompt")
timestamp = dbutils.widgets.get("timestamp")
sample_limit_str = dbutils.widgets.get("sample_limit")

# COMMAND ----------

import json
import os
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import InputConfig, LLMModelConfig

# Initialize clients
ws = WorkspaceClient()

# Load data - get full table first to determine row count
df_full = spark.table(table_name)
total_row_count = df_full.count()
columns = df_full.columns
column_count = len(columns)

# Apply sample limit if specified, otherwise use all rows
sample_limit = None
if sample_limit_str and sample_limit_str.strip():
    try:
        sample_limit = int(sample_limit_str)
    except ValueError:
        sample_limit = None

# Use sampled data for profiling if limit is specified and less than total rows
if sample_limit and sample_limit < total_row_count:
    df = df_full.limit(sample_limit)
    row_count = sample_limit
    print(f"Using sample of {sample_limit:,} rows (out of {total_row_count:,} total)")
else:
    df = df_full
    row_count = total_row_count
    print(f"Using all {total_row_count:,} rows")

# COMMAND ----------

# Profile data with optimized settings
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(
    df,
    options={
        "sample_fraction": 0.5,  # Reduced for speed
        "include_nulls": True,
        "include_patterns": False  # Disable pattern detection for speed
    }
)

# COMMAND ----------

# Generate DQ rules with AI
model_name = os.getenv("MODEL_SERVING_ENDPOINT", "databricks/databricks-claude-sonnet-4-5")
llm_config = LLMModelConfig(model_name=model_name)
generator = DQGenerator(workspace_client=ws, spark=spark, llm_model_config=llm_config)

input_config = InputConfig(location=table_name)
generated_checks = generator.generate_dq_rules_ai_assisted(
    user_input=user_prompt,
    input_config=input_config,
    summary_stats=summary_stats
)

# COMMAND ----------

# Prepare minimal output for App UI
rule_functions = [r.get("check", {}).get("function", "unknown") for r in generated_checks]
summary = f"Generated {len(generated_checks)} DQ rules: {', '.join(rule_functions)}"

# Simplified column profiles for UI display
simple_profiles = []
if isinstance(profiles, dict):
    for col_name, profile in profiles.items():
        simple_profiles.append({
            "name": profile.get("name", "unknown") if isinstance(profile, dict) else str(profile),
            "column": col_name,
            "description": profile.get("description", "") if isinstance(profile, dict) else "",
            "parameters": profile.get("parameters") if isinstance(profile, dict) else None
        })
elif isinstance(profiles, list):
    for profile in profiles:
        if hasattr(profile, '__dict__'):
            simple_profiles.append({
                "name": getattr(profile, 'name', 'unknown'),
                "column": getattr(profile, 'column', ''),
                "description": getattr(profile, 'description', ''),
                "parameters": getattr(profile, 'parameters', None)
            })
        else:
            simple_profiles.append(str(profile))

output = {
    "table_name": table_name,
    "user_prompt": user_prompt,
    "timestamp": timestamp,
    "summary": summary,
    "rules": generated_checks,
    "column_profiles": simple_profiles,
    "metadata": {
        "row_count": row_count,
        "total_row_count": total_row_count,
        "sample_limit": sample_limit,
        "column_count": column_count,
        "columns": columns,
        "rules_generated": len(generated_checks)
    }
}

# Return result
dbutils.notebook.exit(json.dumps(output, default=str))
