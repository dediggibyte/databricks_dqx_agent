# DQX Data Quality Rule Generator - Runbook

Complete guide for deploying, configuring, and operating the DQX Rule Generator application.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Prerequisites](#prerequisites)
3. [Repository Structure](#repository-structure)
4. [Configuration](#configuration)
5. [Deployment](#deployment)
6. [Using the Application](#using-the-application)
7. [CI/CD Pipeline](#cicd-pipeline)
8. [Troubleshooting](#troubleshooting)
9. [Maintenance](#maintenance)

---

## Quick Start

```bash
# 1. Clone and configure
git clone <repository-url>
cd databricks_dqx_agent
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# 2. Upload notebook
databricks workspace import notebooks/generate_dq_rules_fast.py \
  /Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast \
  --language PYTHON --overwrite

# 3. Update notebook path in environments/dev/variables.yml
# 4. Deploy
databricks bundle deploy -t dev

# 5. Get Job ID and update app.yaml with DQ_GENERATION_JOB_ID
databricks jobs list --output json | grep -A2 "DQ Rule Generation"

# 6. Redeploy
databricks bundle deploy -t dev
```

Access: `https://your-workspace.cloud.databricks.com/apps/dqx-rule-generator-dev`

---

## Prerequisites

### Tools Required

| Tool | Installation |
|------|--------------|
| Databricks CLI | `pip install databricks-cli` or [Install Guide](https://docs.databricks.com/dev-tools/cli/install.html) |
| Python 3.11+ | For local development |

### Databricks Workspace Requirements

| Requirement | Description |
|-------------|-------------|
| Unity Catalog | Must be enabled |
| SQL Warehouse | Any warehouse (Serverless recommended) |
| Databricks Apps | Must be enabled on workspace |
| Model Serving (optional) | For AI analysis feature |
| Lakebase (optional) | For saving rules with versioning |

### User Permissions

| Resource | Permission |
|----------|------------|
| Unity Catalog | `USE CATALOG`, `USE SCHEMA`, `SELECT` on target tables |
| SQL Warehouse | `CAN USE` |
| Jobs | `CAN MANAGE RUN` (granted automatically via DAB deployment) |
| Lakebase | OAuth access (if using save feature) |

---

## Repository Structure

```
databricks_dqx_agent/
├── README.md                 # Quick deployment guide
├── wsgi.py                   # WSGI entry point (gunicorn)
├── app.yaml                  # App runtime config (env vars)
├── databricks.yml            # DAB main configuration
├── requirements.txt          # Python dependencies
│
├── docs/                     # Documentation
│   ├── runbook.md            # This file
│   ├── architecture.md       # System design
│   ├── configuration.md      # Config reference
│   ├── api-reference.md      # API endpoints
│   ├── ci-cd.md              # CI/CD pipeline
│   └── dqx-checks.md         # DQX check functions
│
├── resources/                # DAB resource definitions
│   ├── apps.yml              # Databricks App definition
│   ├── generation_job.yml    # DQ rule generation job (Serverless)
│   └── validation_job.yml    # DQ rule validation job (Serverless)
│
├── environments/             # Per-environment configs
│   ├── dev/
│   │   ├── targets.yml       # Dev target (mode, workspace)
│   │   ├── variables.yml     # Dev variables (app_name, notebook_path)
│   │   └── permissions.yml   # Dev permissions
│   ├── stage/
│   │   ├── targets.yml
│   │   ├── variables.yml
│   │   └── permissions.yml
│   └── prod/
│       ├── targets.yml
│       ├── variables.yml
│       └── permissions.yml
│
├── app/                      # Flask application
│   ├── __init__.py           # App factory (create_app)
│   ├── config.py             # Configuration class
│   ├── routes/               # API endpoints
│   └── services/             # Business logic
│
├── notebooks/                # Databricks notebooks
│   ├── generate_dq_rules_fast.py  # DQ rule generation
│   └── validate_dq_rules.py       # DQ rule validation
│
├── templates/                # HTML templates
│   ├── base.html             # Base template with navigation
│   ├── generator.html        # DQ rule generator page
│   └── validator.html        # DQ rule validator page
│
└── .github/                  # CI/CD workflows
    ├── workflows/
    └── actions/
```

---

## Configuration

### Files to Configure

| File | What to Update | When |
|------|----------------|------|
| `environments/<env>/variables.yml` | `notebook_path`, `validation_notebook_path` | Before first deployment |
| `app.yaml` | `DQ_GENERATION_JOB_ID`, `DQ_VALIDATION_JOB_ID` | After first deployment |
| `app.yaml` | `LAKEBASE_HOST` (optional) | If using Lakebase |
| `app.yaml` | `MODEL_SERVING_ENDPOINT` (optional) | If using AI analysis |

### Environment Variables (app.yaml)

```yaml
env:
  # Required - Get from: databricks jobs list
  - name: DQ_GENERATION_JOB_ID
    value: "<generation-job-id>"

  - name: DQ_VALIDATION_JOB_ID
    value: "<validation-job-id>"

  # Optional - Preview row limit
  - name: SAMPLE_DATA_LIMIT
    value: "100"

  # Optional - Lakebase for saving rules
  - name: LAKEBASE_HOST
    value: "<lakebase-host>.database.us-east-1.cloud.databricks.com"
  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"

  # Optional - AI analysis
  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"
```

### Bundle Variables (environments/dev/variables.yml)

```yaml
variables:
  app_name:
    default: "dqx-rule-generator-dev"

  job_name:
    default: "DQ Rule Generation - Dev"

  notebook_path:
    default: "/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast"
```

### Workspace Host

Set via environment variable (not in YAML):
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

---

## Deployment

### Step-by-Step Deployment

#### 1. Set Workspace

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

#### 2. Upload Notebook

```bash
databricks workspace import notebooks/generate_dq_rules_fast.py \
  /Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast \
  --language PYTHON --overwrite
```

#### 3. Update Notebook Path

Edit `environments/dev/variables.yml`:
```yaml
notebook_path:
  default: "/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast"
```

#### 4. Validate and Deploy

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy (creates Job + App)
databricks bundle deploy -t dev
```

#### 5. Get Job ID

```bash
databricks jobs list --output json | grep -A2 "DQ Rule Generation"
```

#### 6. Update app.yaml

```yaml
- name: DQ_GENERATION_JOB_ID
  value: "<job-id-from-step-5>"
```

#### 7. Redeploy

```bash
databricks bundle deploy -t dev
```

### Multi-Environment Deployment

| Target | Command | App Name |
|--------|---------|----------|
| Development | `databricks bundle deploy -t dev` | dqx-rule-generator-dev |
| Staging | `databricks bundle deploy -t stage` | dqx-rule-generator-stage |
| Production | `databricks bundle deploy -t prod` | dqx-rule-generator |

---

## Using the Application

### Workflow

1. **Select Table**: Choose Catalog → Schema → Table → Preview Data
2. **Generate Rules**: Enter natural language prompt → Click Generate
3. **Review**: Edit JSON rules → Format → Validate
4. **Save**: Analyze with AI (optional) → Confirm & Save to Lakebase

### Example Prompts

```
"Ensure customer_id is not null and unique"
"Validate email format and check age is between 0 and 120"
"Check order_date is not in the future and amount is positive"
```

### Rule Format

```json
{
  "check": "is_not_null",
  "column": "customer_id",
  "name": "customer_id_not_null",
  "criticality": "error"
}
```

See [dqx-checks.md](dqx-checks.md) for all available check functions.

---

## CI/CD Pipeline

### Automated Deployment

| Environment | Trigger | Workflow |
|-------------|---------|----------|
| `dev` | Push to `main`, PR | `.github/workflows/ci-cd-dev.yml` |
| `stage` | Manual | `.github/workflows/ci-cd-stage.yml` |
| `prod` | Manual | `.github/workflows/ci-cd-prod.yml` |

### GitHub Secrets Required

Configure per environment in GitHub Settings → Secrets:

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_CLIENT_ID` | Service Principal Client ID |

### GitHub OIDC Setup

1. Create Service Principal in Databricks Account Console
2. Create Federation Policy:
   ```bash
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["<DATABRICKS_ACCOUNT_ID>"],
       "subject": "repo:<GITHUB_ORG>/<REPO_NAME>:environment:<ENV>"
     }
   }'
   ```
3. Grant workspace access to the service principal

See [ci-cd.md](ci-cd.md) for detailed setup.

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "No catalogs available" | No permissions or warehouse down | Check `USE CATALOG` permission, verify SQL Warehouse is running |
| "Job failed to start" | Wrong Job ID or no permissions | Verify `DQ_GENERATION_JOB_ID` in app.yaml |
| "Lakebase connection failed" | Wrong host or service down | Verify `LAKEBASE_HOST`, check Lakebase status |
| "AI Analysis unavailable" | Endpoint not configured | Verify `MODEL_SERVING_ENDPOINT` exists |
| Bundle validation fails | Missing DATABRICKS_HOST | Run `export DATABRICKS_HOST="..."` |

### Check Logs

**App Logs:**
1. Databricks Console → Compute → Apps
2. Select your app → Logs tab

**Job Logs:**
1. Databricks Console → Workflows → Job Runs
2. Select run → View logs

### Health Check

```bash
curl https://<workspace>/apps/dqx-rule-generator-dev/health
```

Expected response:
```json
{"status": "healthy", "timestamp": "..."}
```

### API Endpoints for Debugging

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | App health status |
| `GET /api/lakebase/status` | Lakebase connection |
| `GET /api/catalogs` | Test Unity Catalog access |

---

## Maintenance

### Update Application

```bash
# Make code changes, then:
databricks bundle deploy -t dev
```

### Update Notebook

```bash
databricks workspace import notebooks/generate_dq_rules_fast.py \
  /Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast \
  --language PYTHON --overwrite
```

### View Saved Rules

```sql
SELECT * FROM dq_rules
WHERE table_name = 'catalog.schema.table'
ORDER BY created_at DESC;
```

### Destroy Deployment

```bash
databricks bundle destroy -t dev
```

---

## Quick Reference

| Action | Command |
|--------|---------|
| Set workspace | `export DATABRICKS_HOST="https://..."` |
| Validate | `databricks bundle validate -t dev` |
| Deploy | `databricks bundle deploy -t dev` |
| List jobs | `databricks jobs list` |
| Check health | `curl <app-url>/health` |
| View app logs | Console → Compute → Apps → Logs |
| Destroy | `databricks bundle destroy -t dev` |

---

## Related Documentation

- [Architecture](architecture.md) - System design
- [Configuration](configuration.md) - Detailed config reference
- [API Reference](api-reference.md) - REST endpoints
- [CI/CD](ci-cd.md) - GitHub Actions setup
- [DQX Checks](dqx-checks.md) - Available check functions
