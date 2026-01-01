# DQX Data Quality Manager

A Databricks App for generating and validating data quality rules using AI assistance with [Databricks DQX](https://databrickslabs.github.io/dqx/).

---

## Overview

DQX Data Quality Manager provides a web-based interface to:

- **Generate** data quality rules using AI assistance
- **Validate** generated rules against your data
- **Store** rules with version control in Lakebase
- **Analyze** rule coverage and quality with AI insights

## Features

<div class="grid cards" markdown>

-   :material-robot:{ .lg .middle } __AI-Powered Rule Generation__

    ---

    Generate DQX-compatible data quality rules using natural language prompts and AI assistance.

-   :material-check-decagram:{ .lg .middle } __Rule Validation__

    ---

    Validate generated rules against your actual data using Databricks serverless jobs.

-   :material-history:{ .lg .middle } __Version Control__

    ---

    Store and track rule versions in Lakebase (PostgreSQL) with full audit history.

-   :material-chart-box:{ .lg .middle } __AI Analysis__

    ---

    Get AI-powered insights on rule coverage, quality scores, and recommendations.

</div>

## Quick Start

### Prerequisites

| Requirement | Description |
|-------------|-------------|
| Databricks CLI | [Install here](https://docs.databricks.com/dev-tools/cli/install.html) |
| AWS Databricks Workspace | With Unity Catalog enabled |
| SQL Warehouse | Any warehouse (Serverless recommended) |
| Lakebase (optional) | For saving rules with versioning |

### Deploy in 5 Minutes

```bash
# 1. Clone the repository
git clone https://github.com/dediggibyte/databricks_dqx_agent.git
cd databricks_dqx_agent

# 2. Configure your workspace
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# 3. Upload the notebook
databricks workspace import notebooks/generate_dq_rules_fast.py \
  /Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast \
  --language PYTHON --overwrite

# 4. Deploy the bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

See the [Runbook](runbook.md) for detailed deployment instructions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Databricks App (Flask)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Generator  │  │  Validator  │  │      REST API           │  │
│  │    Page     │  │    Page     │  │  /api/generate          │  │
│  │             │  │             │  │  /api/validate          │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ Unity Catalog │    │  Serverless   │    │   Lakebase    │
│    Tables     │    │     Jobs      │    │  (PostgreSQL) │
└───────────────┘    └───────────────┘    └───────────────┘
```

## Resources

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Guide](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/dediggibyte/databricks_dqx_agent/blob/main/LICENSE) file for details.
