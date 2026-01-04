# DQX Data Quality Manager

A Databricks App for generating and validating data quality rules using AI assistance with [Databricks DQX](https://databrickslabs.github.io/dqx/).

---

## What is DQX Data Quality Manager?

DQX Data Quality Manager provides an intuitive web interface for:

- **Generating** data quality rules using AI and natural language prompts
- **Validating** rules against your actual data with pass/fail statistics
- **Storing** rules with version control in Lakebase (PostgreSQL)
- **Analyzing** rule coverage and quality with AI-powered insights

Built on the Databricks platform, it leverages Unity Catalog for data access, serverless compute for rule generation/validation, and Lakebase for persistent storage.

---

## Key Features

<div class="grid cards" markdown>

-   **AI-Powered Generation**

    ---

    Generate comprehensive DQX-compatible data quality rules using natural language prompts. Simply describe what you want to check, and AI creates the rules.

-   **Rule Validation**

    ---

    Validate generated rules against your actual data using serverless Databricks jobs. Get detailed pass/fail statistics for each rule.

-   **Version Control**

    ---

    Store and track rule versions in Lakebase (PostgreSQL) with full audit history. Roll back to previous versions when needed.

-   **AI Analysis**

    ---

    Get AI-powered insights on rule coverage, quality scores, and recommendations for improving your data quality checks.

</div>

---

## How It Works

1. **Select Data** - Browse Unity Catalog and select your target table
2. **Generate Rules** - Describe your requirements in natural language
3. **Review & Edit** - Review AI-generated rules, edit as needed, validate against data
4. **Save** - Get AI analysis and save rules with version control

---

## Architecture

<figure class="arch-diagram">
  <img src="./images/architecture.png" alt="DQX Architecture">
  <figcaption>DQX Data Quality Manager architecture showing Flask app with Unity Catalog, Serverless Jobs, and Lakebase</figcaption>
</figure>

---

## Authentication Model

DQX uses a **dual authentication model** for security:

| Component | Auth Method | Description |
|-----------|-------------|-------------|
| **Unity Catalog** | User Token (OBO) | Access data with user's permissions |
| **AI Analysis** | User Token (OBO) | Execute queries as the user |
| **Jobs** | App Service Principal | Trigger generation/validation jobs |
| **Lakebase** | User OAuth | Store rules with user identity |

!!! info "On-Behalf-Of (OBO)"
    The app acts on behalf of the logged-in user for data access, ensuring users only see data they have permission to access. See [Authentication](authentication.md) for details.

---

## Quick Start

### Prerequisites

| Requirement | Description |
|-------------|-------------|
| Databricks CLI | [Install here](https://docs.databricks.com/dev-tools/cli/install.html) |
| AWS Databricks Workspace | With Unity Catalog enabled |
| SQL Warehouse | Any warehouse (Serverless recommended) |

### Deploy in 5 Minutes

```bash
# 1. Clone the repository
git clone https://github.com/dediggibyte/databricks_dqx_agent.git
cd databricks_dqx_agent

# 2. Configure your workspace
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# 3. Deploy the bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Access your app at: `https://your-workspace.cloud.databricks.com/apps/dqx-rule-generator-dev`

[Start using DQX Data Quality Manager](runbook.md){ .md-button .md-button--primary }

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| **Web Framework** | Flask 3.0 with Gunicorn |
| **Data Quality** | Databricks Labs DQX |
| **Compute** | Databricks Serverless Jobs |
| **Data Catalog** | Unity Catalog |
| **Storage** | Lakebase (PostgreSQL) |
| **AI** | Claude Sonnet via Model Serving |
| **Deployment** | Databricks Asset Bundles (DAB) |
| **CI/CD** | GitHub Actions with OIDC |

---

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](runbook.md) | Complete deployment guide |
| [Configuration](configuration.md) | Environment variables and settings |
| [Authentication](authentication.md) | OBO and security details |
| [Architecture](architecture.md) | System design and structure |
| [API Reference](api-reference.md) | REST API endpoints |
| [DQX Checks](dqx-checks.md) | Available check functions |
| [CI/CD Pipeline](ci-cd.md) | GitHub Actions setup |

---

## Resources

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Guide](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [GitHub Repository](https://github.com/dediggibyte/databricks_dqx_agent)

---

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/dediggibyte/databricks_dqx_agent/blob/main/LICENSE) file for details.
