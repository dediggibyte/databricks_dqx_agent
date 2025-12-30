# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Generation (Optimized)
# MAGIC Fast version optimized for Databricks App integration.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx[llm] --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Get parameters
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("user_prompt", "", "User Prompt")
dbutils.widgets.text("timestamp", "", "Timestamp")

table_name = dbutils.widgets.get("table_name")
user_prompt = dbutils.widgets.get("user_prompt")
timestamp = dbutils.widgets.get("timestamp")

# COMMAND ----------

import json
import os
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import InputConfig, LLMModelConfig

# Initialize clients
ws = WorkspaceClient()

# Load data - use cache to avoid multiple scans
df = spark.table(table_name)
row_count = df.count()
columns = df.columns
column_count = len(columns)

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
        "column_count": column_count,
        "columns": columns,
        "rules_generated": len(generated_checks)
    }
}

# Return result
dbutils.notebook.exit(json.dumps(output, default=str))
