# CI/CD Pipeline

This project uses **GitHub Actions** with **Databricks Asset Bundles (DABs)** for automated deployment.

## Pipeline Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   PR/Push   │────▶│   Validate  │────▶│   Deploy    │
│   to main   │     │   & Lint    │     │   to Dev    │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
                    ┌─────────────┐           │
                    │   Manual    │◀──────────┘
                    │   Trigger   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       ┌─────────────┐          ┌─────────────┐
       │   Deploy    │          │   Deploy    │
       │   to Stage  │          │   to Prod   │
       └─────────────┘          └─────────────┘
```

## Environments

| Environment | Trigger | Purpose |
|-------------|---------|---------|
| `dev` | Push to `main`, PR | Development testing |
| `stage` | Manual | Pre-production validation |
| `prod` | Manual | Production deployment |

## GitHub Secrets Required

Configure these secrets in your GitHub repository for each environment:

| Secret | Description | Required | Default |
|--------|-------------|----------|---------|
| `DATABRICKS_HOST` | Databricks workspace URL (e.g., `https://dbc-xxx.cloud.databricks.com`) | Yes | - |
| `DATABRICKS_CLIENT_ID` | Service Principal Application ID | Yes | - |
| `LAKEBASE_HOST` | Lakebase PostgreSQL host for DQ rule storage | No | - |
| `LAKEBASE_DATABASE` | Lakebase database name | No | `databricks_postgres` |
| `MODEL_SERVING_ENDPOINT` | Model serving endpoint for AI analysis | No | `databricks-claude-sonnet-4-5` |

## Prerequisites: GitHub OIDC Federation

Before CI/CD will work, configure workload identity federation in Databricks:

### 1. Create a Service Principal

In Databricks Account Console, create a service principal.

### 2. Create Federation Policy

```bash
databricks account service-principal-federation-policy create <SP_ID> --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["<DATABRICKS_ACCOUNT_ID>"],
    "subject": "repo:<GITHUB_ORG>/<REPO_NAME>:environment:<ENV>"
  }
}'
```

### 3. Grant Workspace Access

Grant the service principal access to your workspace.

See [Enable workload identity federation for GitHub Actions](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github) for details.

## Manual Deployment

Deploy using Databricks CLI:

```bash
# Install Databricks CLI
pip install databricks-cli

# Set workspace
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Validate bundle
databricks bundle validate -t dev

# Deploy to environment
databricks bundle deploy -t dev    # Development
databricks bundle deploy -t stage  # Staging
databricks bundle deploy -t prod   # Production
```

## Bundle Configuration

The bundle uses a modular structure with **Serverless compute**:

```
databricks.yml                    # Main config (includes other files)
resources/
├── apps.yml                      # Databricks App definition
├── generation_job.yml            # DQ rule generation job (Serverless)
└── validation_job.yml            # DQ rule validation job (Serverless)
environments/
├── dev/
│   ├── targets.yml               # Dev target (mode: production)
│   ├── variables.yml             # Dev variables
│   └── permissions.yml           # Dev permissions
├── stage/
│   ├── targets.yml               # Stage target (mode: production)
│   ├── variables.yml             # Stage variables
│   └── permissions.yml           # Stage permissions
└── prod/
    ├── targets.yml               # Prod target (mode: production)
    ├── variables.yml             # Prod variables
    └── permissions.yml           # Prod permissions
```

## Environment Differences

| Setting | Development | Staging | Production |
|---------|-------------|---------|------------|
| App Name | dqx-rule-generator-dev | dqx-rule-generator-stage | dqx-rule-generator |
| Job Name | DQ Rule Generation - Dev | DQ Rule Generation - Stage | DQ Rule Generation |
| Compute | Serverless | Serverless | Serverless |
