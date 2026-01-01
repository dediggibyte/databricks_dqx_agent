# Configuration

## Environment Variables

Configure these in `app.yaml`:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID for rule generation | Yes | - |
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | No | - |
| `LAKEBASE_DATABASE` | Lakebase database name | No | `databricks_postgres` |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint | No | `databricks-claude-sonnet-4-5` |
| `SAMPLE_DATA_LIMIT` | Max rows for preview | No | `100` |

## app.yaml Example

```yaml
command: [
  "gunicorn",
  "--bind", "0.0.0.0:8000",
  "--workers", "2",
  "--timeout", "300",
  "wsgi:app"
]

env:
  - name: SAMPLE_DATA_LIMIT
    value: "100"

  - name: DQ_GENERATION_JOB_ID
    value: "<your-job-id>"

  - name: LAKEBASE_HOST
    value: "<your-lakebase-host>"

  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"

  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"
```

## Bundle Variables

Configure in `environments/<env>/variables.yml`:

| Variable | Description |
|----------|-------------|
| `app_name` | Name of the Databricks App |
| `app_description` | Description of the app |
| `job_name` | Name of the DQ generation job |
| `notebook_path` | Path to the notebook in workspace |

### Example (development)

```yaml
variables:
  app_name:
    default: "dqx-rule-generator-dev"

  app_description:
    default: "DQX Rule Generator - Development Environment"

  job_name:
    default: "DQ Rule Generation - Dev"

  notebook_path:
    default: "/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast"
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
