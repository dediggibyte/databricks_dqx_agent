# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Generator - Unity Catalog Function
# MAGIC
# MAGIC This notebook creates a Unity Catalog Function that generates DQ rules
# MAGIC based on table schema and user requirements. Can be used as a tool
# MAGIC in Databricks AgentBricks Multi-Agent Supervisor.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Configuration - Update these values for your environment
CATALOG = "main"  # Your Unity Catalog name
SCHEMA = "dq_tools"  # Schema for DQ tools

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the DQ Rule Generator Function

# COMMAND ----------

# Drop existing function if it exists
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.generate_dq_rules")

# COMMAND ----------

# Create the Unity Catalog Function for DQ Rule Generation
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.generate_dq_rules(
    table_name STRING COMMENT 'Full table name (catalog.schema.table) to generate rules for',
    user_requirements STRING COMMENT 'Natural language description of desired data quality checks'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Generates data quality rules for a table based on user requirements. Returns a JSON array of DQX-compatible rule definitions that can be edited and applied. Use this when users want to create new DQ rules for a table.'
AS $$
import json

# Define available DQX check functions
DQX_CHECK_FUNCTIONS = {{
    "null_checks": ["is_not_null", "is_not_null_and_not_empty"],
    "numeric_checks": ["is_greater_than", "is_less_than", "is_in_range", "is_positive", "is_negative"],
    "string_checks": ["matches_regex", "has_length", "has_min_length", "has_max_length"],
    "list_checks": ["is_in_list", "is_not_in_list"],
    "uniqueness_checks": ["is_unique", "is_primary_key"],
    "date_checks": ["is_valid_date", "is_not_in_future", "is_not_in_past"],
    "comparison_checks": ["equals", "not_equals", "is_between"]
}}

def parse_requirements(requirements):
    '''Parse user requirements to identify needed checks.'''
    requirements_lower = requirements.lower()

    suggested_checks = []

    # Null checks
    if any(kw in requirements_lower for kw in ["null", "empty", "missing", "required", "mandatory"]):
        suggested_checks.append(("is_not_null_and_not_empty", "Checks for null or empty values"))

    # Positive numbers
    if any(kw in requirements_lower for kw in ["positive", "greater than 0", "> 0", "non-negative"]):
        suggested_checks.append(("is_greater_than", "Ensures values are positive"))

    # Email validation
    if any(kw in requirements_lower for kw in ["email", "e-mail"]):
        suggested_checks.append(("matches_regex", "Validates email format"))

    # Phone validation
    if any(kw in requirements_lower for kw in ["phone", "telephone", "mobile"]):
        suggested_checks.append(("matches_regex", "Validates phone number format"))

    # Date checks
    if any(kw in requirements_lower for kw in ["date", "future", "past"]):
        if "future" in requirements_lower:
            suggested_checks.append(("is_not_in_future", "Ensures dates are not in the future"))
        if "past" in requirements_lower:
            suggested_checks.append(("is_not_in_past", "Ensures dates are not in the past"))

    # Uniqueness
    if any(kw in requirements_lower for kw in ["unique", "distinct", "no duplicates", "primary key"]):
        suggested_checks.append(("is_unique", "Ensures values are unique"))

    # Range checks
    if any(kw in requirements_lower for kw in ["range", "between", "within"]):
        suggested_checks.append(("is_in_range", "Ensures values are within a specified range"))

    # List membership
    if any(kw in requirements_lower for kw in ["list", "allowed values", "valid values", "enum"]):
        suggested_checks.append(("is_in_list", "Ensures values are in an allowed list"))

    # Length checks
    if any(kw in requirements_lower for kw in ["length", "characters", "max length", "min length"]):
        suggested_checks.append(("has_length", "Validates string length"))

    return suggested_checks

def extract_columns_from_requirements(requirements):
    '''Try to extract column names mentioned in requirements.'''
    # Common column name patterns
    import re

    # Look for quoted column names
    quoted = re.findall(r'["\']([a-zA-Z_][a-zA-Z0-9_]*)["\']', requirements)

    # Look for column-like words (snake_case or camelCase)
    snake_case = re.findall(r'\\b([a-z]+_[a-z_]+)\\b', requirements.lower())

    # Common column names
    common_cols = []
    common_patterns = [
        ("email", ["email", "e-mail", "email_address"]),
        ("phone", ["phone", "telephone", "mobile", "phone_number"]),
        ("amount", ["amount", "price", "cost", "total", "value"]),
        ("date", ["date", "created_at", "updated_at", "timestamp"]),
        ("id", ["id", "customer_id", "user_id", "order_id"]),
        ("name", ["name", "first_name", "last_name", "full_name"]),
        ("status", ["status", "state", "type"]),
    ]

    req_lower = requirements.lower()
    for col_type, patterns in common_patterns:
        for pattern in patterns:
            if pattern in req_lower:
                common_cols.append(col_type)
                break

    return list(set(quoted + snake_case + common_cols))

def generate_rule(check_type, column, rule_index):
    '''Generate a single DQ rule definition.'''
    rule = {{
        "name": f"{{check_type}}_{{column}}_check",
        "criticality": "warn",
        "check": {{
            "function": check_type,
            "arguments": {{"col_name": column}}
        }},
        "filter": None
    }}

    # Add specific arguments based on check type
    if check_type == "is_greater_than":
        rule["check"]["arguments"]["limit"] = 0
    elif check_type == "is_in_range":
        rule["check"]["arguments"]["min_val"] = 0
        rule["check"]["arguments"]["max_val"] = 100
    elif check_type == "matches_regex" and "email" in column.lower():
        rule["check"]["arguments"]["regex"] = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    elif check_type == "matches_regex" and "phone" in column.lower():
        rule["check"]["arguments"]["regex"] = r"^\\+?[0-9]{{10,15}}$"
    elif check_type == "is_in_list":
        rule["check"]["arguments"]["allowed_values"] = ["value1", "value2", "value3"]
    elif check_type == "has_length":
        rule["check"]["arguments"]["min_length"] = 1
        rule["check"]["arguments"]["max_length"] = 255

    return rule

# Main execution
try:
    # Parse requirements
    suggested_checks = parse_requirements(user_requirements)
    columns = extract_columns_from_requirements(user_requirements)

    # If no columns detected, use generic placeholders
    if not columns:
        columns = ["column_name"]

    # Generate rules
    rules = []
    rule_index = 1

    for check_type, description in suggested_checks:
        for column in columns:
            rule = generate_rule(check_type, column, rule_index)
            rule["_description"] = description
            rules.append(rule)
            rule_index += 1

    # If no rules generated, provide a template
    if not rules:
        rules = [
            {{
                "name": "example_not_null_check",
                "criticality": "warn",
                "check": {{
                    "function": "is_not_null_and_not_empty",
                    "arguments": {{"col_name": "your_column_name"}}
                }},
                "filter": None,
                "_description": "Template rule - replace column name and adjust as needed"
            }}
        ]

    # Build result
    result = {{
        "table_name": table_name,
        "user_requirements": user_requirements,
        "generated_rules": rules,
        "available_check_functions": DQX_CHECK_FUNCTIONS,
        "instructions": "Review and edit the generated rules. Replace placeholder values with actual column names and appropriate values for your data."
    }}

    return json.dumps(result, indent=2)

except Exception as e:
    return json.dumps({{
        "error": str(e),
        "table_name": table_name,
        "user_requirements": user_requirements
    }})
$$
""")

print(f"Function created: {CATALOG}.{SCHEMA}.generate_dq_rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Function

# COMMAND ----------

# Test with sample requirements
result = spark.sql(f"""
SELECT {CATALOG}.{SCHEMA}.generate_dq_rules(
    'main.sales.customers',
    'Ensure email is valid, customer_id is not null and unique, and amount is positive'
) as generated_rules
""").collect()[0][0]

import json
parsed_result = json.loads(result)
print("Generated Rules:")
print(json.dumps(parsed_result["generated_rules"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Details for AgentBricks Registration
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Type** | Unity Catalog Function |
# MAGIC | **Unity Catalog function** | `main.dq_tools.generate_dq_rules` |
# MAGIC | **Agent Name** | DQ Rule Generator |
# MAGIC | **Description** | Generates data quality rules based on natural language requirements. Takes a table name and description of desired checks. Returns DQX-compatible rule definitions. Use this when users want to create new DQ rules for their data. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Function Exists

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{SCHEMA}.generate_dq_rules"))
