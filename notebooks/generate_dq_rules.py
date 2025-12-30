# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Generation Notebook
# MAGIC
# MAGIC This notebook generates data quality rules using Databricks DQX with AI assistance.
# MAGIC It is designed to run as a serverless job triggered by the Data Quality App.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `table_name`: Full table name (catalog.schema.table)
# MAGIC - `user_prompt`: Natural language description of desired DQ rules
# MAGIC - `timestamp`: Request timestamp for tracking

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx[llm] --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Get widget parameters
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("user_prompt", "", "User Prompt")
dbutils.widgets.text("timestamp", "", "Timestamp")

table_name = dbutils.widgets.get("table_name")
user_prompt = dbutils.widgets.get("user_prompt")
timestamp = dbutils.widgets.get("timestamp")

print(f"Table: {table_name}")
print(f"Prompt: {user_prompt}")
print(f"Timestamp: {timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: Load the Data

# COMMAND ----------

# Load the DataFrame from the specified table
df = spark.table(table_name)
print(f"Loaded table with {df.count()} rows and {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Profile the Data with DQX

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()

# Initialize the profiler
profiler = DQProfiler(ws)

# Profile the data
print("Profiling data...")
summary_stats, profiles = profiler.profile(
    df,
    options={
        "sample_fraction": 0.8,
        "include_nulls": True,
        "include_patterns": True
    }
)

print(f"Profiling complete. Generated {len(profiles)} column profiles.")

# COMMAND ----------

# Display profiling results
for col_name, profile in profiles.items():
    print(f"\nColumn: {col_name}")
    print(f"  Type: {profile.get('data_type', 'unknown')}")
    print(f"  Null %: {profile.get('null_percentage', 0):.2f}%")
    if 'unique_count' in profile:
        print(f"  Unique values: {profile['unique_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate DQ Rules Using AI
# MAGIC
# MAGIC Using DQX's built-in `DQGenerator.generate_dq_rules_ai_assisted()` method as documented at:
# MAGIC https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/

# COMMAND ----------

import json
import os
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import InputConfig, LLMModelConfig

# Configure the LLM model
model_name = os.getenv(
    "MODEL_SERVING_ENDPOINT",
    "databricks/databricks-claude-sonnet-4-5"
)

llm_config = LLMModelConfig(model_name=model_name)

# Initialize the DQ Generator with the workspace client and LLM config
generator = DQGenerator(
    workspace_client=ws,
    spark=spark,
    llm_model_config=llm_config
)

print(f"Initialized DQGenerator with model: {model_name}")

# COMMAND ----------

# Generate DQ rules using the AI-assisted approach
# This combines:
# 1. Schema awareness from InputConfig (table location)
# 2. Statistical profiling from summary_stats
# 3. Natural language requirements from user_prompt

print("Generating DQ rules with AI assistance...")

# Create input config for schema-aware generation
input_config = InputConfig(location=table_name)

# Generate rules using the built-in AI-assisted method
# Pass both the profiler statistics and user requirements
generated_checks = generator.generate_dq_rules_ai_assisted(
    user_input=user_prompt,
    input_config=input_config,
    summary_stats=summary_stats
)

print(f"Generation complete! Generated {len(generated_checks)} rules.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Display Generated Rules
# MAGIC
# MAGIC The `generate_dq_rules_ai_assisted()` method returns validated DQX check objects directly.

# COMMAND ----------

# Display the generated checks
print("Generated DQ Rules:")
print("-" * 50)

for i, check in enumerate(generated_checks, 1):
    print(f"\nRule {i}:")
    print(f"  Name: {check.name}")
    print(f"  Criticality: {check.criticality}")
    print(f"  Check Function: {check.check_func}")
    if hasattr(check, 'arguments') and check.arguments:
        print(f"  Arguments: {check.arguments}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Optionally Save Rules with DQEngine
# MAGIC
# MAGIC You can save the generated rules to a workspace file for reuse.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

# Initialize DQX engine
dq_engine = DQEngine(ws)

# Convert checks to serializable format for output
validated_rules = []
for check in generated_checks:
    rule_dict = {
        "name": check.name,
        "criticality": check.criticality,
        "check": {
            "function": check.check_func,
            "arguments": getattr(check, 'arguments', {})
        }
    }
    validated_rules.append(rule_dict)

print(f"\nPrepared {len(validated_rules)} rules for output")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Return Results

# COMMAND ----------

# Generate a summary of the rules
rule_names = [r["name"] for r in validated_rules]
summary = f"Generated {len(validated_rules)} data quality rules for table '{table_name}': {', '.join(rule_names)}"

# Prepare the output
output = {
    "table_name": table_name,
    "user_prompt": user_prompt,
    "timestamp": timestamp,
    "summary": summary,
    "rules": validated_rules,
    "column_profiles": profiles,
    "metadata": {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "rules_generated": len(validated_rules)
    }
}

print("Output Summary:")
print(summary)
print(f"\nRules: {json.dumps(validated_rules, indent=2)}")

# Return as notebook output
output_json = json.dumps(output, default=str)
dbutils.notebook.exit(output_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQX Built-in Check Functions
# MAGIC
# MAGIC The `generate_dq_rules_ai_assisted()` method uses DQX's built-in check functions:
# MAGIC
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `is_not_null` | Column value is not null |
# MAGIC | `is_not_null_and_not_empty` | Column value is not null and not empty string |
# MAGIC | `is_in_list` | Column value is in a specified list |
# MAGIC | `is_not_in_list` | Column value is not in a specified list |
# MAGIC | `is_greater_than` | Column value is greater than limit |
# MAGIC | `is_less_than` | Column value is less than limit |
# MAGIC | `is_in_range` | Column value is within a range |
# MAGIC | `matches_regex` | Column value matches a regex pattern |
# MAGIC | `is_unique` | Column values are unique |
# MAGIC
# MAGIC See full documentation: https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/
