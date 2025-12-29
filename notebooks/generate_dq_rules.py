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
# MAGIC ## Step 1: Load and Profile the Table Data

# COMMAND ----------

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# Load the table
df = spark.table(table_name)
print(f"Table loaded: {df.count()} rows, {len(df.columns)} columns")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Profile the Data with DQX

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator

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

# COMMAND ----------

import json
import dspy

# Configure DSPy for Databricks model serving
# Use the configured model serving endpoint
import os

model_endpoint = os.getenv(
    "MODEL_SERVING_ENDPOINT",
    "databricks-meta-llama-3-1-70b-instruct"
)

# Initialize DSPy with Databricks
lm = dspy.LM(
    model=f"databricks/{model_endpoint}",
    api_base=f"{ws.config.host}/serving-endpoints",
    api_key=ws.config.token
)
dspy.configure(lm=lm)

# COMMAND ----------

# Define the DQ rule generation signature
class GenerateDQRules(dspy.Signature):
    """Generate data quality rules based on data profiles and user requirements."""

    table_name: str = dspy.InputField(desc="The name of the table")
    column_profiles: str = dspy.InputField(desc="JSON string of column profiles")
    user_requirements: str = dspy.InputField(desc="User's natural language requirements")
    sample_data: str = dspy.InputField(desc="Sample data from the table as JSON")

    dq_rules: str = dspy.OutputField(desc="JSON array of DQ rule definitions")
    summary: str = dspy.OutputField(desc="Human-readable summary of the generated rules")


# Create the rule generator
rule_generator = dspy.ChainOfThought(GenerateDQRules)

# COMMAND ----------

# Prepare inputs for the LLM
column_profiles_json = json.dumps(profiles, default=str)

# Get sample data
sample_rows = df.limit(10).toPandas().to_dict(orient='records')
sample_data_json = json.dumps(sample_rows, default=str)

print("Generating DQ rules with AI...")

# Generate rules
result = rule_generator(
    table_name=table_name,
    column_profiles=column_profiles_json,
    user_requirements=user_prompt,
    sample_data=sample_data_json
)

print("Generation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Parse and Validate Generated Rules

# COMMAND ----------

# Parse the generated rules
try:
    generated_rules = json.loads(result.dq_rules)
except json.JSONDecodeError:
    # Try to extract JSON from the response
    import re
    json_match = re.search(r'\[.*\]', result.dq_rules, re.DOTALL)
    if json_match:
        generated_rules = json.loads(json_match.group())
    else:
        generated_rules = []

summary = result.summary

print("Generated Rules Summary:")
print(summary)
print("\nRules JSON:")
print(json.dumps(generated_rules, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Rules Against DQX Schema

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

# Initialize DQX engine
dq_engine = DQEngine(ws)

# Validate the generated rules format
validated_rules = []
for rule in generated_rules:
    # Ensure required fields
    validated_rule = {
        "name": rule.get("name", f"rule_{len(validated_rules) + 1}"),
        "criticality": rule.get("criticality", "warn"),
        "check": rule.get("check", {}),
        "filter": rule.get("filter"),
    }

    # Validate check structure
    check = validated_rule["check"]
    if isinstance(check, dict) and "function" in check:
        validated_rules.append(validated_rule)
    else:
        print(f"Skipping invalid rule: {rule.get('name', 'unnamed')}")

print(f"\nValidated {len(validated_rules)} rules out of {len(generated_rules)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Return Results

# COMMAND ----------

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

# Return as notebook output
output_json = json.dumps(output, default=str)
dbutils.notebook.exit(output_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example DQ Rule Format
# MAGIC
# MAGIC The generated rules follow this DQX-compatible format:
# MAGIC
# MAGIC ```json
# MAGIC [
# MAGIC   {
# MAGIC     "name": "valid_email_check",
# MAGIC     "criticality": "error",
# MAGIC     "check": {
# MAGIC       "function": "is_not_null_and_not_empty",
# MAGIC       "arguments": {"col_name": "email"}
# MAGIC     },
# MAGIC     "filter": null
# MAGIC   },
# MAGIC   {
# MAGIC     "name": "positive_amount_check",
# MAGIC     "criticality": "warn",
# MAGIC     "check": {
# MAGIC       "function": "is_greater_than",
# MAGIC       "arguments": {"col_name": "amount", "limit": 0}
# MAGIC     },
# MAGIC     "filter": "status = 'active'"
# MAGIC   }
# MAGIC ]
# MAGIC ```
