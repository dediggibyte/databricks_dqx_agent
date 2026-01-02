# Architecture

## Overview

This application provides a user-friendly interface for:
1. **Browsing Unity Catalog** - Select catalogs, schemas, and tables
2. **Generating DQ Rules** - Use AI to generate data quality rules based on natural language requirements
3. **Reviewing & Editing** - Review generated rules in JSON format with full editing capabilities
4. **AI Analysis** - Get AI-powered insights on rule coverage and recommendations
5. **Saving to Lakebase** - Store rules with versioning in Databricks Lakebase (PostgreSQL)

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Databricks App (Flask)                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ Step 1:         │  │ Step 2:         │  │ Step 3:     │  │ Step 4:     │ │
│  │ - Select Catalog│  │ - Enter prompt  │  │ - View rules│  │ - AI analyze│ │
│  │ - Select Schema │  │ - Trigger job   │  │ - Edit JSON │  │ - Confirm   │ │
│  │ - Select Table  │  │ - Wait for      │  │ - Validate  │  │ - Save to   │ │
│  │ - Preview data  │  │   completion    │  │ - Download  │  │   Lakebase  │ │
│  └────────┬────────┘  └────────┬────────┘  └─────────────┘  └──────┬──────┘ │
└───────────┼────────────────────┼────────────────────────────────────┼───────┘
            │                    │                                    │
            ▼                    ▼                                    ▼
   ┌─────────────────┐  ┌─────────────────┐                 ┌─────────────────┐
   │  Unity Catalog  │  │ Databricks Job  │                 │    Lakebase     │
   │  (SQL Warehouse)│  │  (Serverless)   │                 │   (PostgreSQL)  │
   │                 │  │                 │                 │                 │
   │  - List catalogs│  │  1. Profile data│                 │  - Store rules  │
   │  - List schemas │  │  2. Generate DQ │                 │  - Versioning   │
   │  - List tables  │  │     rules w/ AI │                 │  - History      │
   │  - Sample data  │  │  3. Return JSON │                 │  - OAuth auth   │
   └─────────────────┘  └─────────────────┘                 └─────────────────┘
```

## Project Structure

```
databricks_dqx_agent/
├── wsgi.py                   # WSGI entry point (gunicorn)
├── app.yaml                  # Databricks App runtime configuration
├── databricks.yml            # DAB bundle configuration (main)
├── requirements.txt          # Python dependencies
├── README.md                 # Quick start guide
│
├── docs/                     # Documentation
│   ├── architecture.md       # This file
│   ├── api-reference.md      # API endpoints
│   ├── configuration.md      # Environment variables
│   ├── ci-cd.md              # CI/CD pipeline
│   ├── dqx-checks.md         # DQX check functions
│   └── runbook.md            # Operational guide
│
├── resources/                # DAB resource definitions
│   ├── apps.yml              # App definitions
│   ├── generation_job.yml    # DQ rule generation job (Serverless)
│   └── validation_job.yml    # DQ rule validation job (Serverless)
│
├── environments/             # Per-environment configurations
│   ├── dev/
│   │   ├── targets.yml       # Dev target config
│   │   ├── variables.yml     # Dev variables
│   │   └── permissions.yml   # Dev permissions
│   ├── stage/
│   │   ├── targets.yml       # Stage target config
│   │   ├── variables.yml     # Stage variables
│   │   └── permissions.yml   # Stage permissions
│   └── prod/
│       ├── targets.yml       # Prod target config
│       ├── variables.yml     # Prod variables
│       └── permissions.yml   # Prod permissions
│
├── .github/                  # CI/CD workflows
│   ├── workflows/
│   │   ├── ci-cd-dev.yml     # Dev pipeline (auto on push/PR)
│   │   ├── ci-cd-stage.yml   # Stage pipeline (manual)
│   │   ├── ci-cd-prod.yml    # Prod pipeline (manual)
│   │   ├── common-ci-steps.yml
│   │   └── common-cd-steps.yml
│   └── actions/
│       ├── databricks-setup/ # GitHub OIDC + Databricks CLI
│       └── deploy-dab/       # Bundle deployment
│
├── app/                      # Application package
│   ├── __init__.py           # Flask app factory
│   ├── config.py             # Configuration management
│   ├── routes/               # API endpoints
│   │   ├── catalog.py        # Unity Catalog routes
│   │   ├── rules.py          # DQ Rules routes
│   │   └── lakebase.py       # Lakebase routes
│   └── services/             # Business logic
│       ├── databricks.py     # Databricks SDK service
│       ├── lakebase.py       # Lakebase service
│       └── ai.py             # AI analysis service
│
├── notebooks/                # Databricks notebooks
│   ├── generate_dq_rules_fast.py  # DQ rule generation notebook
│   └── validate_dq_rules.py       # DQ rule validation notebook
│
└── templates/                # HTML templates
    ├── base.html             # Base template with navigation
    ├── generator.html        # DQ rule generator page
    └── validator.html        # DQ rule validator page
```

## Authentication

This app uses **OAuth User Authorization** for all connections:
- **Unity Catalog**: Uses the logged-in user's Databricks credentials
- **Lakebase**: Uses OAuth token from `x-forwarded-access-token` header
- **No passwords stored** in configuration
