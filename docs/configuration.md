# Configuration

## Environment Variables

Configure these in `src/app.yaml`:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID for rule generation | Yes | - |
| `DQ_VALIDATION_JOB_ID` | Databricks Job ID for rule validation | Yes | - |
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | No | - |
| `LAKEBASE_DATABASE` | Lakebase database name | No | `databricks_postgres` |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint | No | `databricks-claude-sonnet-4-5` |

## src/app.yaml Example

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
  - name: DQ_GENERATION_JOB_ID
    value: "<generation-job-id>"

  - name: DQ_VALIDATION_JOB_ID
    value: "<validation-job-id>"

  - name: LAKEBASE_HOST
    value: "<your-lakebase-host>"

  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"

  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"
```

> **Note:** In CI/CD deployments, `src/app.yaml` is automatically generated with job IDs and secrets from GitHub environment secrets.

## Bundle Variables

Variables are declared in `databricks.yml` and values are set per-target in `environments/<env>/variables.yml`.

| Variable | Description |
|----------|-------------|
| `app_name` | Name of the Databricks App |
| `app_description` | Description of the app |
| `job_name` | Name of the DQ generation job |
| `notebook_path` | Path to the generation notebook in workspace |
| `validation_job_name` | Name of the DQ validation job |
| `validation_notebook_path` | Path to the validation notebook in workspace |

### Example (dev)

```yaml
# environments/dev/variables.yml
targets:
  dev:
    variables:
      app_name: "dqx-rule-generator-dev"
      app_description: "DQX Data Quality Manager - Dev Environment"
      job_name: "DQ Rule Generation - Dev"
      notebook_path: "/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast"
      validation_job_name: "DQ Rule Validation - Dev"
      validation_notebook_path: "/Workspace/Users/<your-email>/dqx_agent/validate_dq_rules"
```

## Workspace Configuration

The workspace host is configured via the `DATABRICKS_HOST` environment variable:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

This enables:
- Local deployment with personal workspaces
- CI/CD deployment with GitHub secrets per environment
- No hardcoded workspace URLs in the repository
