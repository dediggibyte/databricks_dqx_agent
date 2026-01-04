# Architecture

This document describes the system architecture and design of DQX Data Quality Manager.

---

## System Overview

DQX Data Quality Manager is a multi-tier application built on the Databricks platform:

<figure class="arch-diagram arch-diagram--hero">
  <img src="../images/system-overview.png" alt="DQX System Overview">
  <figcaption>Complete system architecture showing users, authentication, compute, and data layers</figcaption>
</figure>

---

## Application Flow

### Rule Generation Flow

<figure class="arch-diagram">
  <img src="../images/rule-generation-flow.png" alt="Rule Generation Flow">
  <figcaption>DQ rule generation sequence: User selects table, enters prompt, and AI generates rules</figcaption>
</figure>

### Rule Validation Flow

<figure class="arch-diagram">
  <img src="../images/rule-validation-flow.png" alt="Rule Validation Flow">
  <figcaption>DQ rule validation sequence: Rules are applied against actual data using DQX Engine</figcaption>
</figure>

---

## Component Architecture

### Flask Application (`src/app/`)

The web application handles UI rendering and REST API endpoints:

```
src/app/
├── __init__.py          # Flask app factory
├── config.py            # Configuration management
├── routes/
│   ├── catalog.py       # Unity Catalog browsing endpoints
│   ├── rules.py         # DQ rule generation/validation endpoints
│   └── lakebase.py      # Lakebase connection endpoints
└── services/
    ├── databricks.py    # Databricks SDK wrapper (SQL + Jobs)
    ├── ai.py            # AI analysis service
    └── lakebase.py      # PostgreSQL operations
```

### Services Layer

| Service | Responsibility | Authentication |
|---------|---------------|----------------|
| `DatabricksService` | SQL queries, job triggering | OBO for SQL, SP for jobs |
| `AIAnalysisService` | AI-powered rule analysis | OBO via Statement Execution |
| `LakebaseService` | Rule storage and versioning | User OAuth |

### Notebooks (`notebooks/`)

Long-running compute tasks executed as serverless jobs:

| Notebook | Purpose | Key Libraries |
|----------|---------|---------------|
| `generate_dq_rules_fast.py` | Profile data and generate DQ rules | DQX Profiler, DQX Generator |
| `validate_dq_rules.py` | Apply rules and return results | DQX Engine |

---

## Project Structure

```
databricks_dqx_agent/
├── databricks.yml                    # DAB bundle configuration (main)
├── README.md                         # Quick start guide
│
├── src/                              # App source code (deployed to Databricks Apps)
│   ├── app.yaml                      # Databricks App runtime configuration
│   ├── wsgi.py                       # WSGI entry point (gunicorn)
│   ├── requirements.txt              # Python dependencies
│   │
│   ├── app/                          # Flask application package
│   │   ├── __init__.py               # Flask app factory
│   │   ├── config.py                 # Configuration management
│   │   ├── routes/                   # API endpoints
│   │   │   ├── catalog.py            # Unity Catalog routes
│   │   │   ├── rules.py              # DQ Rules routes
│   │   │   └── lakebase.py           # Lakebase routes
│   │   └── services/                 # Business logic
│   │       ├── databricks.py         # Databricks SDK service
│   │       ├── lakebase.py           # Lakebase service
│   │       └── ai.py                 # AI analysis service
│   │
│   ├── templates/                    # HTML templates
│   │   ├── base.html                 # Base template with navigation
│   │   ├── generator.html            # DQ rule generator page
│   │   └── validator.html            # DQ rule validator page
│   │
│   └── static/                       # Static assets
│       ├── css/main.css              # Styles
│       └── js/                       # JavaScript files
│           ├── common.js             # Shared utilities
│           ├── generator.js          # Generator page logic
│           └── validator.js          # Validator page logic
│
├── notebooks/                        # Databricks notebooks
│   ├── generate_dq_rules_fast.py     # DQ rule generation notebook
│   └── validate_dq_rules.py          # DQ rule validation notebook
│
├── resources/                        # DAB resource definitions
│   ├── apps.yml                      # App definition + permissions
│   ├── generation_job.yml            # Generation job (Serverless)
│   └── validation_job.yml            # Validation job (Serverless)
│
├── environments/                     # Per-environment configurations
│   ├── dev/
│   │   ├── targets.yml               # Dev target config
│   │   ├── variables.yml             # Dev variables
│   │   └── permissions.yml           # Dev permissions
│   ├── stage/
│   │   └── ...
│   └── prod/
│       └── ...
│
├── .github/                          # CI/CD workflows
│   ├── workflows/
│   │   ├── ci-cd-dev.yml             # Dev pipeline
│   │   ├── ci-cd-stage.yml           # Stage pipeline
│   │   ├── ci-cd-prod.yml            # Prod pipeline
│   │   └── docs.yml                  # Documentation deployment
│   └── actions/
│       ├── databricks-setup/         # GitHub OIDC setup
│       └── deploy-dab/               # Bundle deployment
│
└── docs/                             # Documentation (MkDocs)
    ├── index.md                      # Home page
    ├── runbook.md                    # Deployment guide
    ├── authentication.md             # Auth documentation
    ├── architecture.md               # This file
    ├── configuration.md              # Config reference
    ├── api-reference.md              # API endpoints
    ├── dqx-checks.md                 # DQX check functions
    └── ci-cd.md                      # CI/CD pipeline
```

---

## Authentication Architecture

DQX uses a **dual authentication model**:

### User Token (OBO) Path

Used for operations that should respect user permissions:

<figure class="arch-diagram">
  <img src="../images/auth-obo-path.png" alt="OBO Authentication Path">
</figure>

**Operations:**

- `SHOW CATALOGS/SCHEMAS/TABLES`
- `SELECT * FROM table`
- `SELECT ai_query(...)`

### Service Principal Path

Used for operations without user scope support:

<figure class="arch-diagram">
  <img src="../images/auth-sp-path.png" alt="Service Principal Authentication Path">
</figure>

**Operations:**

- `jobs.run_now()`
- `jobs.get_run()`
- `jobs.get_run_output()`

### OAuth Path (Lakebase)

Used for PostgreSQL connections:

<figure class="arch-diagram">
  <img src="../images/auth-oauth-path.png" alt="OAuth Authentication Path for Lakebase">
</figure>

For detailed authentication documentation, see [Authentication](authentication.md).

---

## Data Flow

### Rule Storage Schema

Rules are stored in Lakebase with versioning:

```sql
CREATE TABLE dq_rules_events (
    id UUID PRIMARY KEY,
    table_name VARCHAR(500) NOT NULL,
    version INTEGER NOT NULL,
    rules JSONB NOT NULL,
    user_prompt TEXT,
    ai_summary JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB,
    UNIQUE(table_name, version)
);
```

### Rule JSON Format

```json
{
  "criticality": "error",
  "check": {
    "function": "is_not_null",
    "arguments": {
      "col_name": "customer_id"
    }
  },
  "name": "customer_id_not_null"
}
```

---

## Deployment Architecture

### Databricks Asset Bundles (DAB)

The application is deployed using DAB with modular configuration:

```yaml
# databricks.yml
bundle:
  name: dqx-rule-generator

include:
  - ./resources/*.yml           # App + Job definitions
  - ./environments/dev/*.yml    # Dev config
  - ./environments/stage/*.yml  # Stage config
  - ./environments/prod/*.yml   # Prod config
```

### Resource Bindings

```yaml
# resources/apps.yml
resources:
  apps:
    dqx_app:
      user_api_scopes:
        - sql                   # Enable OBO for SQL

      resources:
        - name: "sql-warehouse"
          sql_warehouse:
            id: ${var.sql_warehouse_id}
            permission: "CAN_USE"

        - name: "generation-job"
          job:
            id: ${resources.jobs.dq_rule_generation.id}
            permission: "CAN_MANAGE_RUN"
```

### Environment Isolation

| Environment | App Name | Workspace Path |
|-------------|----------|----------------|
| Development | `dqx-rule-generator-dev` | `/Users/<user>/.bundle/.../dev` |
| Staging | `dqx-rule-generator-stage` | `/Users/<user>/.bundle/.../stage` |
| Production | `dqx-rule-generator` | `/Users/<user>/.bundle/.../prod` |

---

## Security Architecture

### Defense in Depth

1. **Network Layer**: All traffic over HTTPS/TLS
2. **Authentication**: OAuth tokens with limited lifetime
3. **Authorization**: User permissions enforced via OBO
4. **Data Access**: Unity Catalog access controls
5. **Audit**: All operations logged with user identity

### Token Flow

<figure class="arch-diagram">
  <img src="../images/token-flow.png" alt="Security Token Flow">
  <figcaption>OAuth token lifecycle from user login to resource access</figcaption>
</figure>

### No Stored Credentials

- No passwords in configuration files
- OAuth tokens from request headers only
- Service principal via managed identity

---

## Scalability Considerations

| Component | Scaling Strategy |
|-----------|------------------|
| **Flask App** | Horizontal (multiple workers via Gunicorn) |
| **SQL Warehouse** | Serverless auto-scaling |
| **Jobs** | Serverless compute (auto-provisioned) |
| **Lakebase** | Managed PostgreSQL scaling |

---

## Related Documentation

- [Configuration](configuration.md) - Environment variables and settings
- [Authentication](authentication.md) - Detailed auth documentation
- [API Reference](api-reference.md) - REST API endpoints
- [CI/CD Pipeline](ci-cd.md) - Deployment automation
