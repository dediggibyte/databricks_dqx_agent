# DQX Data Quality Manager

[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Databricks](https://img.shields.io/badge/Databricks-Apps-FF3621.svg)](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
[![DQX](https://img.shields.io/badge/DQX-Data%20Quality-green.svg)](https://databrickslabs.github.io/dqx/)
[![CI/CD Dev](https://github.com/dediggibyte/databricks_dqx_agent/actions/workflows/ci-cd-dev.yml/badge.svg)](https://github.com/dediggibyte/databricks_dqx_agent/actions/workflows/ci-cd-dev.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://dediggibyte.github.io/databricks_dqx_agent/)

**AI-powered data quality rule generation and validation for Databricks**

A Databricks App for generating, validating, and managing data quality rules using AI assistance with [Databricks DQX](https://databrickslabs.github.io/dqx/).

---

## Features

- **AI-Powered Generation** - Generate DQX rules from natural language prompts
- **Rule Validation** - Validate rules against actual data with pass/fail statistics
- **Version Control** - Store rules in Lakebase with full audit history
- **AI Analysis** - Get coverage insights and recommendations from AI
- **OBO Authentication** - Users only see data they have permission to access

---

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/dediggibyte/databricks_dqx_agent.git
cd databricks_dqx_agent
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# 2. Deploy
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Access: `https://your-workspace.cloud.databricks.com/apps/dqx-rule-generator-dev`

> **Note:** DAB automatically deploys notebooks and configures permissions. No manual setup required.

---

## Architecture

![DQX Architecture](docs/images/architecture.png)

### Authentication Model

| Component | Auth Method | Description |
|-----------|-------------|-------------|
| **Unity Catalog** | User Token (OBO) | Access data with user's permissions |
| **Jobs** | App Service Principal | Trigger generation/validation jobs |
| **Lakebase** | User OAuth | Store rules with user identity |

---

## Project Structure

```
databricks_dqx_agent/
├── src/                      # Flask app (deployed to Databricks Apps)
│   ├── app/                  # Application code
│   │   ├── routes/           # API endpoints
│   │   └── services/         # Business logic (databricks, ai, lakebase)
│   ├── templates/            # HTML templates
│   └── static/               # CSS and JavaScript
├── notebooks/                # Databricks notebooks (serverless jobs)
├── resources/                # DAB resource definitions
├── environments/             # Per-environment configs (dev, stage, prod)
├── docs/                     # Documentation (MkDocs)
└── .github/                  # CI/CD workflows
```

---

## Configuration

### Required Environment Variables

Set in `src/app.yaml`:

| Variable | Description |
|----------|-------------|
| `SQL_WAREHOUSE_ID` | SQL Warehouse ID for queries |
| `DQ_GENERATION_JOB_ID` | Auto-set by DAB |
| `DQ_VALIDATION_JOB_ID` | Auto-set by DAB |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | - |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint | `databricks-claude-sonnet-4-5` |

---

## Local Development

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DQ_GENERATION_JOB_ID="your-job-id"
export DQ_VALIDATION_JOB_ID="your-job-id"
export SQL_WAREHOUSE_ID="your-warehouse-id"

cd src
pip install -r requirements.txt
python wsgi.py
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](docs/runbook.md) | Deployment guide |
| [Configuration](docs/configuration.md) | Environment variables |
| [Authentication](docs/authentication.md) | OBO and security |
| [Architecture](docs/architecture.md) | System design |
| [API Reference](docs/api-reference.md) | REST endpoints |
| [DQX Checks](docs/dqx-checks.md) | Available check functions |
| [CI/CD](docs/ci-cd.md) | GitHub Actions setup |

**Full Documentation:** [https://dediggibyte.github.io/databricks_dqx_agent/](https://dediggibyte.github.io/databricks_dqx_agent/)

---

## CI/CD

| Environment | Trigger | Workflow |
|-------------|---------|----------|
| `dev` | Push to main, PR | `ci-cd-dev.yml` |
| `stage` | Manual | `ci-cd-stage.yml` |
| `prod` | Manual | `ci-cd-prod.yml` |

---

## Resources

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Guide](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)

---

## License

MIT License - see [LICENSE](LICENSE) for details.
