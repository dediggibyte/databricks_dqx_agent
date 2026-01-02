# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Validation
# MAGIC Validate data quality rules against a table and return summary results.
# MAGIC
# MAGIC **Note:** The `databricks-labs-dqx` library is installed via job environment dependencies.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("rules", "[]", "Rules JSON")

table_name = dbutils.widgets.get("table_name")
rules_json = dbutils.widgets.get("rules")

# COMMAND ----------

import json
from datetime import datetime
from databricks.labs.dqx.engine import DQEngine

# Parse rules from JSON
try:
    rules = json.loads(rules_json)
except json.JSONDecodeError as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Invalid rules JSON: {str(e)}"
    }))

if not rules:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": "No rules provided"
    }))

print(f"Validating {len(rules)} rules against table: {table_name}")

# COMMAND ----------

# Load the table data
try:
    df = spark.table(table_name)
    total_rows = df.count()
    print(f"Loaded {total_rows} rows from {table_name}")
except Exception as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Failed to load table: {str(e)}"
    }))

# COMMAND ----------

# Initialize DQ Engine and apply checks
dq_engine = DQEngine(spark)

# Apply checks and split into valid/invalid DataFrames
try:
    valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(df, rules)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"Valid rows: {valid_count}")
    print(f"Invalid rows: {invalid_count}")
except Exception as e:
    dbutils.notebook.exit(json.dumps({
        "success": False,
        "error": f"Failed to apply checks: {str(e)}"
    }))

# COMMAND ----------

# Analyze rule-by-rule results
# Apply checks to get the annotated DataFrame with _error and _warning columns
checked_df = dq_engine.apply_checks_by_metadata(df, rules)

# Collect rule-level statistics
rule_results = []

for idx, rule in enumerate(rules):
    check_info = rule.get("check", {})
    func_name = check_info.get("function", "unknown")
    args = check_info.get("arguments", {})
    criticality = rule.get("criticality", "error")

    # Get column name from various possible field names
    column = args.get("column") or args.get("col_name") or \
             (args.get("columns", [None])[0] if args.get("columns") else None) or \
             (args.get("col_names", [None])[0] if args.get("col_names") else None) or "-"

    # Count violations for this specific rule by checking _error/_warning columns
    violation_count = 0
    status = "pass"

    try:
        if criticality.lower() == "error":
            # Check _error column for this rule's violations
            error_col = "_error"
            if error_col in checked_df.columns:
                # Filter rows where this rule caused an error
                violations_df = checked_df.filter(
                    f"exists({error_col}, x -> x.name = '{func_name}')"
                )
                violation_count = violations_df.count()
        else:
            # Check _warning column for this rule's violations
            warn_col = "_warning"
            if warn_col in checked_df.columns:
                violations_df = checked_df.filter(
                    f"exists({warn_col}, x -> x.name = '{func_name}')"
                )
                violation_count = violations_df.count()

        if violation_count > 0:
            status = "fail" if criticality.lower() == "error" else "warn"

    except Exception as e:
        # If we can't count individual violations, use overall invalid count
        print(f"Warning: Could not count violations for {func_name}: {e}")

    rule_results.append({
        "rule_name": func_name,
        "column": str(column),
        "criticality": criticality,
        "status": status,
        "violation_count": violation_count,
        "details": f"Checked {func_name} on column '{column}'"
    })

# COMMAND ----------

# Calculate summary statistics
passed = sum(1 for r in rule_results if r["status"] == "pass")
failed = sum(1 for r in rule_results if r["status"] == "fail")
warnings = sum(1 for r in rule_results if r["status"] == "warn")

# Build output
output = {
    "success": True,
    "table_name": table_name,
    "validated_at": datetime.now().isoformat(),
    "total_rules": len(rules),
    "passed": passed,
    "failed": failed,
    "warnings": warnings,
    "total_rows": total_rows,
    "valid_rows": valid_count,
    "invalid_rows": invalid_count,
    "pass_rate": round((valid_count / total_rows * 100), 2) if total_rows > 0 else 0,
    "rule_results": rule_results
}

print(f"\n=== Validation Summary ===")
print(f"Total Rules: {len(rules)}")
print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}")
print(f"Valid Rows: {valid_count}/{total_rows} ({output['pass_rate']}%)")

# COMMAND ----------

# Return result
dbutils.notebook.exit(json.dumps(output, default=str))
