# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Summarizer - Unity Catalog Function
# MAGIC
# MAGIC This notebook creates a Unity Catalog Function that can be used as a tool
# MAGIC in Databricks AgentBricks Multi-Agent Supervisor.
# MAGIC
# MAGIC The function analyzes generated DQ rules and provides:
# MAGIC 1. Human-readable summary of the rules
# MAGIC 2. JSON rule definition for editing
# MAGIC 3. Recommendations for improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your catalog and schema where the function will be created

# COMMAND ----------

# Configuration - Update these values for your environment
CATALOG = "main"  # Your Unity Catalog name
SCHEMA = "dq_tools"  # Schema for DQ tools

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the DQ Rule Summarizer Function

# COMMAND ----------

# Drop existing function if it exists
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.summarize_dq_rules")

# COMMAND ----------

# Create the Unity Catalog Function
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.summarize_dq_rules(
    rules_json STRING COMMENT 'JSON array of DQ rule definitions',
    table_name STRING COMMENT 'Name of the table the rules apply to'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Analyzes data quality rules and provides a comprehensive summary including human-readable explanations, affected columns, criticality breakdown, and improvement recommendations. Use this tool when you need to understand or explain generated DQ rules to users.'
AS $$
import json

def analyze_rules(rules):
    '''Analyze DQ rules and extract metadata.'''
    affected_columns = set()
    criticality_breakdown = {{"error": 0, "warn": 0, "info": 0}}
    check_types = {{}}

    for rule in rules:
        # Extract affected columns
        check = rule.get("check", {{}})
        args = check.get("arguments", {{}})
        if "col_name" in args:
            affected_columns.add(args["col_name"])
        if "col_names" in args:
            affected_columns.update(args["col_names"])

        # Count criticality levels
        criticality = rule.get("criticality", "warn")
        if criticality in criticality_breakdown:
            criticality_breakdown[criticality] += 1

        # Track check types
        check_function = check.get("function", "unknown")
        check_types[check_function] = check_types.get(check_function, 0) + 1

    return {{
        "affected_columns": list(affected_columns),
        "criticality_breakdown": criticality_breakdown,
        "check_types": check_types,
        "total_rules": len(rules)
    }}

def generate_summary(rules, table_name, analysis):
    '''Generate a human-readable summary.'''
    summary_parts = [
        f"## Data Quality Rules Summary for `{{table_name}}`\\n\\n",
        f"**Total Rules:** {{analysis['total_rules']}}\\n\\n",
        f"**Affected Columns:** {{', '.join(analysis['affected_columns']) if analysis['affected_columns'] else 'None specified'}}\\n\\n",
        "### Criticality Breakdown\\n"
    ]

    for level, count in analysis["criticality_breakdown"].items():
        if count > 0:
            emoji = {{"error": "🔴", "warn": "🟡", "info": "🔵"}}.get(level, "⚪")
            summary_parts.append(f"- {{emoji}} **{{level.upper()}}:** {{count}} rule(s)\\n")

    summary_parts.append("\\n### Check Types Used\\n")
    for check_type, count in analysis["check_types"].items():
        summary_parts.append(f"- `{{check_type}}`: {{count}} rule(s)\\n")

    summary_parts.append("\\n### Rule Details\\n")
    for i, rule in enumerate(rules, 1):
        name = rule.get("name", f"Rule {{i}}")
        criticality = rule.get("criticality", "warn")
        check_fn = rule.get("check", {{}}).get("function", "unknown")
        args = rule.get("check", {{}}).get("arguments", {{}})
        col = args.get("col_name", "N/A")
        summary_parts.append(
            f"{{i}}. **{{name}}** [{{criticality}}]\\n"
            f"   - Check: `{{check_fn}}`\\n"
            f"   - Column: `{{col}}`\\n"
        )

    return "".join(summary_parts)

def generate_recommendations(rules, analysis):
    '''Generate improvement recommendations.'''
    recommendations = []

    if analysis["criticality_breakdown"]["error"] == 0:
        recommendations.append(
            "Consider adding error-level rules for critical data integrity checks"
        )

    if analysis["total_rules"] < 3:
        recommendations.append(
            "Consider adding more rules to ensure comprehensive data quality coverage"
        )

    if len(analysis["affected_columns"]) < 3:
        recommendations.append(
            "Consider adding rules for additional columns to improve coverage"
        )

    null_checks = ["is_not_null", "is_not_null_and_not_empty"]
    has_null_check = any(
        rule.get("check", {{}}).get("function") in null_checks
        for rule in rules
    )
    if not has_null_check:
        recommendations.append(
            "Consider adding null value checks for important columns"
        )

    return recommendations

# Main execution
try:
    rules = json.loads(rules_json)
    if not isinstance(rules, list):
        rules = [rules]
except json.JSONDecodeError as e:
    return json.dumps({{
        "error": f"Invalid JSON: {{str(e)}}",
        "input_received": rules_json[:500]
    }})

# Analyze rules
analysis = analyze_rules(rules)

# Generate summary
summary = generate_summary(rules, table_name, analysis)

# Generate recommendations
recommendations = generate_recommendations(rules, analysis)

# Build result
result = {{
    "summary": summary,
    "rules": rules,
    "recommendations": recommendations,
    "metadata": {{
        "table_name": table_name,
        "total_rules": analysis["total_rules"],
        "affected_columns": analysis["affected_columns"],
        "criticality_breakdown": analysis["criticality_breakdown"],
        "check_types": analysis["check_types"]
    }}
}}

return json.dumps(result, indent=2)
$$
""")

print(f"Function created: {CATALOG}.{SCHEMA}.summarize_dq_rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Function

# COMMAND ----------

# Test with sample DQ rules
test_rules = """
[
  {
    "name": "valid_email_check",
    "criticality": "error",
    "check": {
      "function": "matches_regex",
      "arguments": {
        "col_name": "email",
        "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"
      }
    }
  },
  {
    "name": "positive_amount",
    "criticality": "warn",
    "check": {
      "function": "is_greater_than",
      "arguments": {
        "col_name": "amount",
        "limit": 0
      }
    }
  },
  {
    "name": "not_null_customer_id",
    "criticality": "error",
    "check": {
      "function": "is_not_null",
      "arguments": {
        "col_name": "customer_id"
      }
    }
  }
]
"""

result = spark.sql(f"""
SELECT {CATALOG}.{SCHEMA}.summarize_dq_rules(
    '{test_rules}',
    'main.sales.transactions'
) as summary
""").collect()[0][0]

import json
parsed_result = json.loads(result)
print(parsed_result["summary"])
print("\nRecommendations:")
for rec in parsed_result["recommendations"]:
    print(f"  • {rec}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Details for AgentBricks Registration
# MAGIC
# MAGIC Use the following details when registering this function in the Multi-Agent Supervisor UI:
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Type** | Unity Catalog Function |
# MAGIC | **Unity Catalog function** | `main.dq_tools.summarize_dq_rules` |
# MAGIC | **Agent Name** | DQ Rule Summarizer |
# MAGIC | **Description** | Analyzes data quality rules and provides comprehensive summaries. Takes a JSON array of DQ rule definitions and a table name. Returns human-readable summaries, affected columns, criticality breakdown, and improvement recommendations. Use this when users need to understand or review generated DQ rules. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions (if needed)
# MAGIC
# MAGIC Run the following commands to grant access to the function:

# COMMAND ----------

# Uncomment and modify as needed for your environment
# spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.summarize_dq_rules TO `users`")
# spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.summarize_dq_rules TO `data-engineers`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Function Exists

# COMMAND ----------

# List the function to verify it was created
display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CATALOG}.{SCHEMA}.summarize_dq_rules"))
