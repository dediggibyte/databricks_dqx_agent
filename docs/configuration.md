# Configuration

This document describes all configuration options for DQX Data Quality Manager.

---

## Overview

Configuration is managed at three levels:

| Level | Location | Purpose |
|-------|----------|---------|
| **App Runtime** | `src/app.yaml` | Environment variables for the Flask app |
| **Bundle Variables** | `environments/<env>/variables.yml` | DAB deployment variables |
| **Workspace** | Environment variable | Databricks host URL |

---

## App Runtime Configuration

### src/app.yaml

This file configures the Databricks App runtime environment:

```yaml
command:
  - gunicorn
  - --bind
  - 0.0.0.0:8000
  - --workers
  - "2"
  - --timeout
  - "300"
  - wsgi:app

env:
  # === Required ===
  - name: DQ_GENERATION_JOB_ID
    value: "<generation-job-id>"

  - name: DQ_VALIDATION_JOB_ID
    value: "<validation-job-id>"

  - name: SQL_WAREHOUSE_ID
    value: "<sql-warehouse-id>"

  # === Optional: Lakebase ===
  - name: LAKEBASE_HOST
    value: "<lakebase-host>.database.us-east-1.cloud.databricks.com"

  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"

  - name: LAKEBASE_PORT
    value: "5432"

  # === Optional: AI Analysis ===
  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"

  # === Optional: Data Sampling ===
  - name: SAMPLE_DATA_LIMIT
    value: "100"
```

### Environment Variables Reference

#### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID for rule generation | `123456789` |
| `DQ_VALIDATION_JOB_ID` | Databricks Job ID for rule validation | `987654321` |
| `SQL_WAREHOUSE_ID` | SQL Warehouse ID for queries | `abc123def456` |

!!! tip "Getting Job IDs"
    After deploying the bundle, get job IDs with:
    ```bash
    databricks jobs list --output json | grep -A2 "DQ Rule"
    ```

#### Lakebase Variables (Optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEBASE_HOST` | Lakebase PostgreSQL hostname | - |
| `LAKEBASE_DATABASE` | Database name | `databricks_postgres` |
| `LAKEBASE_PORT` | PostgreSQL port | `5432` |

!!! info "Lakebase Authentication"
    Lakebase uses OAuth authentication. The user's forwarded token is used as the password. See [Authentication](authentication.md#lakebase-authentication-oauth).

#### AI Analysis Variables (Optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `MODEL_SERVING_ENDPOINT` | Model serving endpoint for AI analysis | `databricks-claude-sonnet-4-5` |

#### Data Sampling Variables (Optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `SAMPLE_DATA_LIMIT` | Maximum rows to display in sample preview | `100` |

---

## Bundle Variables

### databricks.yml (Declaration)

Variables are declared in the main bundle configuration:

```yaml
# databricks.yml
variables:
  app_name:
    description: "Name of the Databricks App"
  app_description:
    description: "Description of the Databricks App"
  job_name:
    description: "Name of the DQ rule generation job"
  notebook_path:
    description: "Path to the DQ rule generation notebook"
  validation_job_name:
    description: "Name of the DQ rule validation job"
  validation_notebook_path:
    description: "Path to the DQ rule validation notebook"
  sql_warehouse_id:
    description: "SQL Warehouse ID for app to execute queries"
    default: ""  # Overridden by CI/CD --var flag
```

### Environment Variables Files

Values are set per environment in `environments/<env>/variables.yml`:

=== "Development"

    ```yaml
    # environments/dev/variables.yml
    targets:
      dev:
        variables:
          app_name: "dqx-rule-generator-dev"
          app_description: "DQX Data Quality Manager - Dev Environment"

          job_name: "DQ Rule Generation - Dev"
          # Notebooks deployed with bundle to workspace.root_path
          notebook_path: "${workspace.root_path}/notebooks/generate_dq_rules_fast"

          validation_job_name: "DQ Rule Validation - Dev"
          validation_notebook_path: "${workspace.root_path}/notebooks/validate_dq_rules"
    ```

=== "Staging"

    ```yaml
    # environments/stage/variables.yml
    targets:
      stage:
        variables:
          app_name: "dqx-rule-generator-stage"
          app_description: "DQX Data Quality Manager - Stage Environment"

          job_name: "DQ Rule Generation - Stage"
          notebook_path: "${workspace.root_path}/notebooks/generate_dq_rules_fast"

          validation_job_name: "DQ Rule Validation - Stage"
          validation_notebook_path: "${workspace.root_path}/notebooks/validate_dq_rules"
    ```

=== "Production"

    ```yaml
    # environments/prod/variables.yml
    targets:
      prod:
        variables:
          app_name: "dqx-rule-generator"
          app_description: "DQX Data Quality Manager - Production"

          job_name: "DQ Rule Generation"
          notebook_path: "${workspace.root_path}/notebooks/generate_dq_rules_fast"

          validation_job_name: "DQ Rule Validation"
          validation_notebook_path: "${workspace.root_path}/notebooks/validate_dq_rules"
    ```

### Bundle Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `app_name` | Databricks App name | `dqx-rule-generator-dev` |
| `app_description` | App description shown in UI | `DQX Data Quality Manager` |
| `job_name` | Generation job name | `DQ Rule Generation - Dev` |
| `notebook_path` | Path to generation notebook | `${workspace.root_path}/notebooks/...` |
| `validation_job_name` | Validation job name | `DQ Rule Validation - Dev` |
| `validation_notebook_path` | Path to validation notebook | `${workspace.root_path}/notebooks/...` |
| `sql_warehouse_id` | SQL Warehouse ID | Passed via `--var` in CI/CD |

!!! warning "Notebook Path"
    Use `${workspace.root_path}/notebooks/...` to reference notebooks deployed with the bundle. This ensures the service principal running jobs can access the notebooks.

---

## Workspace Configuration

### Host URL

Set the Databricks workspace URL via environment variable:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

This is used for:
- Local CLI deployment
- CI/CD workflows (from GitHub secrets)
- SDK initialization fallback

### Target Configuration

Each environment has target settings in `environments/<env>/targets.yml`:

```yaml
# environments/dev/targets.yml
targets:
  dev:
    mode: production
    default: true

    workspace:
      # Bundle files deployed to this path
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
```

---

## CI/CD Secrets

For automated deployments via GitHub Actions, configure these secrets per environment:

### Required Secrets

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_CLIENT_ID` | Service Principal Application ID |
| `SQL_WAREHOUSE_ID` | SQL Warehouse ID |

### Optional Secrets

| Secret | Description | Default |
|--------|-------------|---------|
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | - |
| `LAKEBASE_DATABASE` | Lakebase database name | `databricks_postgres` |
| `MODEL_SERVING_ENDPOINT` | Model serving endpoint | `databricks-claude-sonnet-4-5` |
| `SAMPLE_DATA_LIMIT` | Sample data row limit | `100` |

### Setting Up Secrets

1. Go to GitHub repository → Settings → Secrets and variables → Actions
2. Click "New repository secret" for each secret
3. For environment-specific secrets, create GitHub environments first

```bash
# Example: Setting secrets via GitHub CLI
gh secret set DATABRICKS_HOST --env dev --body "https://your-workspace.cloud.databricks.com"
gh secret set SQL_WAREHOUSE_ID --env dev --body "abc123def456"
```

---

## Local Development

For local development without Databricks Apps:

```bash
# Required
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DQ_GENERATION_JOB_ID="your-generation-job-id"
export DQ_VALIDATION_JOB_ID="your-validation-job-id"
export SQL_WAREHOUSE_ID="your-warehouse-id"

# Optional
export LAKEBASE_HOST="your-lakebase-host"
export MODEL_SERVING_ENDPOINT="databricks-claude-sonnet-4-5"

# Run the app
cd src
python wsgi.py
```

!!! note "Token Fallback"
    When running locally, the app uses `DATABRICKS_TOKEN` as fallback when no `x-forwarded-access-token` header is present.

---

## Configuration Hierarchy

Configuration values are resolved in this order:

1. **Environment variables** in `src/app.yaml` (highest priority)
2. **Bundle variables** from `environments/<env>/variables.yml`
3. **Default values** in `databricks.yml`
4. **Runtime fallbacks** in `src/app/config.py`

```python
# src/app/config.py
class Config:
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DQ_GENERATION_JOB_ID = os.getenv("DQ_GENERATION_JOB_ID")
    DQ_VALIDATION_JOB_ID = os.getenv("DQ_VALIDATION_JOB_ID")
    SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
    LAKEBASE_HOST = os.getenv("LAKEBASE_HOST")
    LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", "5432"))
    MODEL_SERVING_ENDPOINT = os.getenv("MODEL_SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")
    SAMPLE_DATA_LIMIT = int(os.getenv("SAMPLE_DATA_LIMIT", "100"))
```

---

## Validation

### Validate Bundle Configuration

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Check resolved variables
databricks bundle validate -t dev --output json | jq '.variables'
```

### Check App Configuration

After deployment, verify app environment:

```bash
# Get app details
databricks apps get dqx-rule-generator-dev
```

Or check via the app's debug endpoint:

```bash
curl https://<workspace>/apps/dqx-rule-generator-dev/api/debug
```

---

## Related Documentation

- [Quick Start](runbook.md) - Deployment guide with configuration steps
- [Authentication](authentication.md) - Auth configuration details
- [CI/CD Pipeline](ci-cd.md) - GitHub secrets setup
