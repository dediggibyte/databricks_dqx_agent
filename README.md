# DQX - Data Quality Rule Generator

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Databricks](https://img.shields.io/badge/Databricks-Apps-orange.svg)](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
[![DQX](https://img.shields.io/badge/DQX-Data%20Quality-green.svg)](https://databrickslabs.github.io/dqx/)
[![CI/CD Dev](https://github.com/dediggibyte/databricks_dqx_agent/actions/workflows/ci-cd-dev.yml/badge.svg)](https://github.com/dediggibyte/databricks_dqx_agent/actions/workflows/ci-cd-dev.yml)

A Databricks App for generating data quality rules using AI assistance with [Databricks DQX](https://databrickslabs.github.io/dqx/).

---

## Deployment Guide

### Prerequisites

| Requirement | Description |
|-------------|-------------|
| Databricks CLI | [Install here](https://docs.databricks.com/dev-tools/cli/install.html) |
| AWS Databricks Workspace | With Unity Catalog enabled |
| SQL Warehouse | Any warehouse (Serverless recommended) |
| Lakebase (optional) | For saving rules with versioning |
| Model Serving (optional) | For AI analysis (`databricks-claude-sonnet-4-5`) |

### Step 1: Clone & Configure

```bash
git clone <repository-url>
cd databricks_dqx_agent

export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

### Step 2: Upload Notebook

```bash
databricks workspace import notebooks/generate_dq_rules_fast.py \
  /Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast \
  --language PYTHON --overwrite
```

### Step 3: Update Configuration

Edit `environments/dev/variables.yml`:
```yaml
notebook_path:
  default: "/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast"
```

### Step 4: Deploy Bundle

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

### Step 5: Get Job IDs & Update App

```bash
databricks jobs list --output json | grep -A2 "DQ Rule"
```

Edit `app.yaml`:
```yaml
env:
  - name: DQ_GENERATION_JOB_ID
    value: "<generation-job-id>"

  - name: DQ_VALIDATION_JOB_ID
    value: "<validation-job-id>"

  - name: LAKEBASE_HOST              # Optional
    value: "<your-lakebase-host>"
```

### Step 6: Redeploy

```bash
databricks bundle deploy -t dev
```

### Done!

Access: `https://your-workspace.cloud.databricks.com/apps/dqx-rule-generator-dev`

---

## Documentation

**Start here:** [Runbook](docs/runbook.md) - Complete guide for deployment, configuration, and operations

| Document | Description |
|----------|-------------|
| [Runbook](docs/runbook.md) | **Complete getting started guide** |
| [Architecture](docs/architecture.md) | System design and project structure |
| [Configuration](docs/configuration.md) | Environment variables and app.yaml |
| [API Reference](docs/api-reference.md) | REST API endpoints |
| [CI/CD Pipeline](docs/ci-cd.md) | GitHub Actions deployment |
| [DQX Checks](docs/dqx-checks.md) | Available DQX check functions |

---

## Local Development

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DQ_GENERATION_JOB_ID="your-generation-job-id"
export DQ_VALIDATION_JOB_ID="your-validation-job-id"

python wsgi.py
```

---

## Resources

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Guide](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
