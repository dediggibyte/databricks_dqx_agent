# DQX Data Quality Rule Generator - Team Runbook

This runbook provides operational guidance for deploying, configuring, and using the DQX Data Quality Rule Generator application.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Configuration Reference](#configuration-reference)
3. [Deployment Guide](#deployment-guide)
4. [Using the Application](#using-the-application)
5. [Troubleshooting](#troubleshooting)
6. [Maintenance](#maintenance)

---

## Prerequisites

Before deploying or using this application, ensure you have:

### Databricks Workspace Requirements

| Requirement | Description |
|-------------|-------------|
| Unity Catalog | Must be enabled on your workspace |
| SQL Warehouse | Serverless recommended for best performance |
| Databricks Apps | Enabled on your workspace |
| Model Serving | Endpoint with Claude model for AI analysis |

### User Permissions

Users need the following permissions:

- **Unity Catalog**: `USE CATALOG`, `USE SCHEMA`, `SELECT` on tables they want to analyze
- **SQL Warehouse**: `CAN USE` permission
- **Jobs**: `CAN MANAGE RUN` on the DQ generation job
- **Lakebase**: Access to the Lakebase instance (optional, for saving rules)

---

## Configuration Reference

### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID that runs the DQ rule generation notebook | `123456789` |

### Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEBASE_HOST` | Lakebase PostgreSQL host for saving rules | None (Lakebase features disabled) |
| `LAKEBASE_DATABASE` | Database name in Lakebase | `databricks_postgres` |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint for rule analysis | `databricks-claude-sonnet-4-5` |
| `SAMPLE_DATA_LIMIT` | Maximum rows to preview | `100` |

### app.yaml Configuration

```yaml
command:
  - gunicorn
  - app:app
  - --bind
  - 0.0.0.0:8000
  - --workers
  - "4"

env:
  - name: DQ_GENERATION_JOB_ID
    value: "YOUR_JOB_ID_HERE"

  - name: LAKEBASE_HOST
    value: "your-lakebase-host.database.us-east-1.cloud.databricks.com"

  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"
```

---

## Deployment Guide

### Step 1: Upload the Notebook

1. Navigate to your Databricks workspace
2. Go to **Workspace** > **Users** > `<your-email>`
3. Create a folder called `dqx_agent`
4. Upload `notebooks/generate_dq_rules_fast.py` to this folder
5. Note the full path: `/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast`

### Step 2: Create the Databricks Job

1. Go to **Workflows** > **Jobs** > **Create Job**
2. Configure the job:
   - **Name**: `DQX Rule Generator`
   - **Task type**: Notebook
   - **Source**: Workspace
   - **Path**: `/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast`
   - **Cluster**: Serverless or existing cluster
3. Add job parameters:
   - `table_name` (string)
   - `user_prompt` (string)
4. Save and note the **Job ID** from the URL

### Step 3: Configure app.yaml

Update the `app.yaml` file with your configuration:

```yaml
env:
  - name: DQ_GENERATION_JOB_ID
    value: "YOUR_JOB_ID"  # From Step 2

  - name: LAKEBASE_HOST
    value: "YOUR_LAKEBASE_HOST"  # From Lakebase console
```

### Step 4: Deploy to Databricks Apps

```bash
# Using Databricks CLI
databricks apps deploy . --app-name dq-rule-generator

# Or specify workspace
databricks apps deploy . --app-name dq-rule-generator --profile your-profile
```

### Step 5: Access the Application

After deployment, access the app at:
```
https://<workspace-host>/apps/dq-rule-generator
```

---

## Using the Application

### Step-by-Step Workflow

#### Step 1: Select Your Table

1. Open the application in your browser
2. In the **Select Table** section:
   - Choose a **Catalog** from the dropdown
   - Choose a **Schema** from the dropdown
   - Choose a **Table** from the dropdown
3. Click **Preview Sample Data** to see a sample of your data

#### Step 2: Generate DQ Rules

1. In the **Generate DQ Rules** section:
   - Enter a natural language prompt describing the rules you need
   - Example: "Ensure customer_id is unique, email is valid format, and age is between 0 and 120"
2. Click **Generate Rules**
3. Wait for the job to complete (typically 30-60 seconds)

#### Step 3: Review and Edit Rules

1. In the **Review & Edit Rules** section:
   - Review the generated JSON rules
   - Edit rules directly in the editor
   - Use **Format JSON** to prettify the code
   - Use **Validate** to check JSON syntax
2. The rules follow DQX format:
   ```json
   {
     "check": "is_not_null",
     "column": "customer_id",
     "name": "customer_id_not_null"
   }
   ```

#### Step 4: AI Analysis & Save

1. Click **Analyze with AI** to get:
   - Coverage analysis
   - Recommendations for improvements
   - Rule-by-rule assessment
2. Review the AI analysis
3. Click **Confirm & Save to Lakebase** to persist the rules

### Available DQX Check Functions

See [DQX Quality Checks Reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/) for full details.

#### Row-Level Checks (Applied to Individual Rows)

| Function | Description | Key Parameters |
|----------|-------------|----------------|
| `is_not_null` | Value is not null | column |
| `is_not_empty` | Value is not empty string | column |
| `is_not_null_and_not_empty` | Not null and not empty | column, trim_strings |
| `is_in_list` | Value in allowed list | column, allowed, case_sensitive |
| `is_not_in_list` | Value not in forbidden list | column, forbidden, case_sensitive |
| `is_not_null_and_is_in_list` | Not null AND in allowed values | column, allowed |
| `is_not_null_and_not_empty_array` | Array non-null and non-empty | column |
| `is_in_range` | Value within boundaries | column, min_limit, max_limit |
| `is_not_in_range` | Value outside boundaries | column, min_limit, max_limit |
| `is_equal_to` | Value matches specific value | column, value |
| `is_not_equal_to` | Value differs from value | column, value |
| `is_not_less_than` | Value >= minimum | column, limit |
| `is_not_greater_than` | Value <= maximum | column, limit |
| `is_valid_date` | Valid date format | column, date_format |
| `is_valid_timestamp` | Valid timestamp format | column, timestamp_format |
| `is_valid_json` | Valid JSON string | column |
| `has_json_keys` | JSON has required keys | column, keys, require_all |
| `has_valid_json_schema` | JSON conforms to schema | column, schema |
| `is_not_in_future` | Timestamp not in future | column, offset |
| `is_not_in_near_future` | Timestamp within future window | column, offset |
| `is_older_than_n_days` | Date older than N days | column, days |
| `is_older_than_col2_for_n_days` | Col1 older than col2 by N days | column1, column2, days |
| `regex_match` | Value matches regex | column, regex, negate |
| `is_valid_ipv4_address` | Valid IPv4 format | column |
| `is_ipv4_address_in_cidr` | IPv4 within CIDR block | column, cidr_block |
| `is_valid_ipv6_address` | Valid IPv6 format | column |
| `is_ipv6_address_in_cidr` | IPv6 within CIDR block | column, cidr_block |
| `sql_expression` | Custom SQL condition | expression, msg, name |
| `is_data_fresh` | Data not stale | column, max_age_minutes |
| `does_not_contain_pii` | No PII detected | column, threshold |
| `is_latitude` | Value between -90 and 90 | column |
| `is_longitude` | Value between -180 and 180 | column |
| `is_geometry` | Valid geometry | column |
| `is_geography` | Valid geography | column |
| `is_point` | Geometry is point | column |
| `is_linestring` | Geometry is linestring | column |
| `is_polygon` | Geometry is polygon | column |
| `is_multipoint` | Geometry is multipoint | column |
| `is_multilinestring` | Geometry is multilinestring | column |
| `is_multipolygon` | Geometry is multipolygon | column |
| `is_geometrycollection` | Geometry is collection | column |
| `is_ogc_valid` | Geometry valid per OGC | column |
| `is_non_empty_geometry` | Geometry has coordinates | column |
| `is_not_null_island` | Not at null island (0,0) | column |

#### Dataset-Level Checks (Applied to Row Groups or Entire Dataset)

| Function | Description | Key Parameters |
|----------|-------------|----------------|
| `is_unique` | No duplicate values | columns, nulls_distinct |
| `is_aggr_not_greater_than` | Aggregated value <= max | column, limit, aggr_type |
| `is_aggr_not_less_than` | Aggregated value >= min | column, limit, aggr_type |
| `is_aggr_equal` | Aggregated value = target | column, limit, aggr_type |
| `is_aggr_not_equal` | Aggregated value != target | column, limit, aggr_type |
| `foreign_key` | Values exist in reference | columns, ref_columns, ref_table |
| `sql_query` | Custom SQL validation | query, condition_column |
| `compare_datasets` | Compare with reference | columns, ref_table |
| `is_data_fresh_per_time_window` | Records in time windows | column, window_minutes |
| `has_valid_schema` | Schema matches expected | expected_schema, strict |
| `has_no_outliers` | No statistical outliers | column |

---

## Troubleshooting

### Common Issues

#### "No catalogs available"

**Cause**: User doesn't have access to any catalogs or SQL Warehouse is unavailable.

**Solution**:
1. Verify the user has `USE CATALOG` permission on at least one catalog
2. Check that the SQL Warehouse is running
3. Verify workspace has Unity Catalog enabled

#### "Job failed to start"

**Cause**: Job ID is incorrect or user lacks permissions.

**Solution**:
1. Verify `DQ_GENERATION_JOB_ID` is correct in app.yaml
2. Ensure user has `CAN MANAGE RUN` permission on the job
3. Check the job exists and is not deleted

#### "Lakebase connection failed"

**Cause**: Lakebase host is incorrect or service is unavailable.

**Solution**:
1. Verify `LAKEBASE_HOST` in app.yaml
2. Check Lakebase instance is running in Databricks console
3. Ensure user has OAuth access to Lakebase

#### "AI Analysis unavailable"

**Cause**: Model Serving endpoint is not configured or unavailable.

**Solution**:
1. Verify `MODEL_SERVING_ENDPOINT` in app.yaml
2. Check the endpoint exists and is serving
3. Ensure user has access to query the endpoint

#### White/invisible text in UI

**Cause**: CSS theme issue.

**Solution**: Clear browser cache and reload the page.

### Checking Logs

View application logs in Databricks:

1. Go to **Compute** > **Apps**
2. Find your app (`dq-rule-generator`)
3. Click on the app name
4. View **Logs** tab

### Health Check

Access the health endpoint to verify the app is running:

```bash
curl https://<workspace-host>/apps/dq-rule-generator/health
```

Expected response:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00.000000"
}
```

---

## Maintenance

### Updating the Application

1. Make changes to the code
2. Redeploy:
   ```bash
   databricks apps deploy . --app-name dq-rule-generator
   ```

### Updating the Notebook

1. Upload the new notebook to Databricks
2. The job will automatically use the updated notebook

### Monitoring Usage

- Check **Workflows** > **Job Runs** for rule generation activity
- Review Lakebase tables for saved rules history
- Monitor Model Serving endpoint usage for AI analysis calls

### Backup Procedures

Rules saved to Lakebase can be queried:

```sql
SELECT * FROM dq_rules
WHERE table_name = 'catalog.schema.table'
ORDER BY created_at DESC;
```

---

## Support

For issues or questions:

1. Check this runbook's Troubleshooting section
2. Review application logs in Databricks Apps console
3. Contact the data engineering team

---

## Quick Reference Card

| Action | How To |
|--------|--------|
| Deploy app | `databricks apps deploy . --app-name dq-rule-generator` |
| Check health | `GET /health` |
| View logs | Databricks Console > Compute > Apps > dq-rule-generator > Logs |
| Update config | Edit `app.yaml`, redeploy |
| Check Lakebase | `GET /api/lakebase/status` |
