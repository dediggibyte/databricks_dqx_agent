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

See [configuration.md](configuration.md) for detailed environment variable reference.

### Quick Reference

| Variable | Description | Required |
|----------|-------------|----------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID | Yes |
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | No |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint | No |

---

## Deployment Guide

See the main [README.md](../README.md) for quick deployment steps.

### Manual Job Creation (Alternative)

If not using DAB deployment:

1. Navigate to **Workflows** > **Jobs** > **Create Job**
2. Configure:
   - **Name**: `DQX Rule Generator`
   - **Task type**: Notebook
   - **Path**: `/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast`
   - **Cluster**: Serverless
3. Add parameters: `table_name`, `user_prompt`
4. Note the **Job ID**

---

## Using the Application

### Step-by-Step Workflow

#### Step 1: Select Your Table

1. Open the application in your browser
2. Choose **Catalog** > **Schema** > **Table**
3. Click **Preview Sample Data**

#### Step 2: Generate DQ Rules

1. Enter a natural language prompt
2. Example: "Ensure customer_id is unique, email is valid format, and age is between 0 and 120"
3. Click **Generate Rules**
4. Wait 30-60 seconds

#### Step 3: Review and Edit Rules

1. Review generated JSON rules
2. Edit directly in the editor
3. Use **Format JSON** / **Validate**

Example rule format:
```json
{
  "check": "is_not_null",
  "column": "customer_id",
  "name": "customer_id_not_null"
}
```

#### Step 4: AI Analysis & Save

1. Click **Analyze with AI** for recommendations
2. Click **Confirm & Save to Lakebase**

### DQX Check Functions

See [dqx-checks.md](dqx-checks.md) for full reference.

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "No catalogs available" | No permissions or SQL Warehouse down | Check `USE CATALOG` permission, verify warehouse |
| "Job failed to start" | Wrong Job ID or no permissions | Verify `DQ_GENERATION_JOB_ID`, check `CAN MANAGE RUN` |
| "Lakebase connection failed" | Wrong host or service down | Verify `LAKEBASE_HOST`, check Lakebase status |
| "AI Analysis unavailable" | Endpoint not configured | Verify `MODEL_SERVING_ENDPOINT` |

### Checking Logs

1. Go to **Compute** > **Apps**
2. Find your app
3. View **Logs** tab

### Health Check

```bash
curl https://<workspace-host>/apps/dq-rule-generator/health
```

---

## Maintenance

### Updating the Application

```bash
databricks bundle deploy -t dev
```

### Monitoring

- **Workflows** > **Job Runs** - rule generation activity
- **Lakebase** - saved rules history
- **Model Serving** - AI analysis usage

### Backup

Query saved rules:
```sql
SELECT * FROM dq_rules
WHERE table_name = 'catalog.schema.table'
ORDER BY created_at DESC;
```

---

## Quick Reference Card

| Action | How To |
|--------|--------|
| Deploy app | `databricks bundle deploy -t dev` |
| Check health | `GET /health` |
| View logs | Console > Compute > Apps > Logs |
| Update config | Edit `app.yaml`, redeploy |
| Check Lakebase | `GET /api/lakebase/status` |
