# Databricks DQX Agent

Generate data quality rules using AI assistance with Databricks DQX library.

## Overview

This solution provides:
- **Step 1**: Databricks App with table selection and sample data preview
- **Step 2**: AI-powered DQ rule generation via Databricks notebook job
- **Step 3**: Review and edit generated rules in JSON format
- **Step 4**: AI analysis of rules and storage to Lakebase with versioning

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Databricks App (Flask)                             │
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
                                                                    │
                                                                    ▼
                                                          ┌─────────────────┐
                                                          │ Model Serving   │
                                                          │ Endpoint (LLM)  │
                                                          │                 │
                                                          │ - Analyze rules │
                                                          │ - Provide       │
                                                          │   reasoning     │
                                                          └─────────────────┘
```

## Components

### 1. Databricks App (`app.py`)
Flask-based web application served via Gunicorn:
- **Cascading dropdowns**: Catalog → Schema → Table selection via Unity Catalog API
- **Sample data preview**: View table data via Statement Execution API
- **Prompt input**: Natural language DQ requirements
- **Job trigger**: Submits parameters to notebook job
- **Results display**: Shows generated rules in editable JSON format
- **AI Analysis**: Analyzes rules using Model Serving endpoint (LLM)
- **Lakebase Storage**: Saves confirmed rules with version tracking

### 2. DQ Rule Generation Notebook (`notebooks/generate_dq_rules.py`)
Databricks notebook for serverless job execution:
- Profiles input data using DQX Profiler
- Generates DQ rules using AI (DQX + Databricks LLM endpoints)
- Validates rules against DQX schema
- Returns structured JSON output

### 3. Lakebase Database
PostgreSQL-compatible database for storing DQ rules:
- `dq_rules_events` table with versioning
- Automatic version incrementing
- Previous versions archived (is_active=false)
- AI analysis summary stored with each version
- **OAuth User Authorization** - connects using logged-in user's identity

## Project Structure

```
databricks_dqx_agent/
├── app.py                    # Flask application
├── app.yaml                  # Databricks App config
├── requirements.txt          # Python dependencies
├── templates/
│   └── index.html            # Web UI template
├── notebooks/
│   └── generate_dq_rules.py  # DQ generation notebook
└── README.md
```

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Model serving endpoint (e.g., `databricks-claude-sonnet-4-5`)
- SQL Warehouse (for sample data preview)
- Lakebase instance (for storing DQ rules) - *optional but recommended*

### Step 1: Upload the Notebook

1. Upload `notebooks/generate_dq_rules.py` to your Databricks workspace
2. Recommended path: `/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules`

### Step 2: Create a Databricks Job

1. Go to **Workflows → Jobs → Create Job**
2. Configure:
   - **Task name**: `generate_dq_rules`
   - **Type**: Notebook
   - **Source**: Select the uploaded notebook
   - **Cluster**: Serverless
   - **Parameters**: Add `table_name`, `user_prompt`, `timestamp` (as job trigger time)
3. Save and note the **Job ID**

### Step 3: Deploy the Databricks App

1. Go to **Compute → Apps → Create App**
2. Set **Source code path** to the root of this repository
3. The app will use `app.yaml` configuration automatically

### Step 4: Configure Environment Variables

Update `app.yaml` with your configuration:

```yaml
env:
  # Required: Job ID from Step 2
  - name: DQ_GENERATION_JOB_ID
    value: "your-job-id"

  # Optional: Lakebase configuration for Step 4 (Confirm & Save)
  # Authentication uses OAuth (User Authorization) - no password needed
  - name: LAKEBASE_HOST
    value: "your-lakebase-instance.cloud.databricks.com"

  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"

  # Optional: Model Serving endpoint for AI analysis
  - name: MODEL_SERVING_ENDPOINT
    value: "databricks-claude-sonnet-4-5"
```

### Step 5: Create Lakebase Instance (Optional)

If you want to use the Confirm & Save feature:

1. Go to **SQL → Lakebase → Create**
2. Create a new Lakebase instance
3. Note the **Host** (e.g., `ep-abc-123.databricks.com`)
4. Update `app.yaml` with the `LAKEBASE_HOST` value
5. **Grant user access to Lakebase** - Each user of the app must have a Postgres role in Lakebase:
   - Use the `databricks_auth` extension to create roles
   - Users authenticate via their Databricks OAuth token automatically
6. The app will automatically create the `dq_rules_events` table on first save

#### OAuth Authentication (User Authorization)

This app uses **OAuth User Authorization** for Lakebase connections:
- No passwords stored in configuration
- Users authenticate automatically via Databricks Apps
- The app receives the user's OAuth token via `x-forwarded-access-token` header
- Each user connects to Lakebase with their own identity
- Provides per-user audit trail for data governance

## Usage

1. **Open the Databricks App URL**

2. **Step 1 - Select a Table**:
   - Choose Catalog from dropdown
   - Choose Schema from dropdown
   - Choose Table from dropdown
   - Review sample data preview

3. **Step 2 - Generate DQ Rules**:
   - Enter natural language prompt describing your DQ requirements
   - Example prompts:
     - "Ensure email is valid and customer_id is not null"
     - "Check that amounts are positive and dates are not in the future"
     - "Validate phone numbers and ensure status is in ['active', 'inactive']"
   - Click "Generate DQ Rules"
   - Wait for job completion

4. **Step 3 - Review & Edit Rules**:
   - Review the generated rules in JSON format
   - Edit rules directly in the JSON editor
   - Use "Format JSON" to prettify
   - Use "Validate" to check JSON structure
   - Download rules as JSON if needed

5. **Step 4 - Confirm & Save** (requires Lakebase):
   - Click "Analyze Rules with AI" to get insights:
     - Summary of what the rules check
     - Quality score (1-10)
     - Rule-by-rule analysis with reasoning
     - Coverage assessment
     - Recommendations for additional rules
   - Click "Confirm & Save Rules" to store in Lakebase
   - View version history of saved rules

## DQ Rule Format

Generated rules follow DQX-compatible format:

```json
{
  "table_name": "catalog.schema.table",
  "summary": "Generated 3 data quality rules...",
  "rules": [
    {
      "name": "valid_email_check",
      "criticality": "error",
      "check": {
        "function": "matches_regex",
        "arguments": {
          "col_name": "email",
          "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
        }
      }
    }
  ],
  "metadata": {
    "row_count": 1000,
    "column_count": 10,
    "rules_generated": 3
  }
}
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main UI |
| `/api/catalogs` | GET | List Unity Catalog catalogs |
| `/api/schemas/<catalog>` | GET | List schemas in a catalog |
| `/api/tables/<catalog>/<schema>` | GET | List tables in a schema |
| `/api/sample/<catalog>/<schema>/<table>` | GET | Get sample data |
| `/api/generate` | POST | Trigger DQ generation job |
| `/api/status/<run_id>` | GET | Get job run status |
| `/api/analyze` | POST | Analyze DQ rules with AI |
| `/api/confirm` | POST | Save rules to Lakebase |
| `/api/history/<table_name>` | GET | Get version history for a table |
| `/api/lakebase/status` | GET | Check Lakebase connection status |
| `/health` | GET | Health check |

## Lakebase Schema

The app automatically creates the following table in Lakebase:

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

## References

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/)
- [AI-assisted Quality Checks Generation](https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/)
- [Databricks Lakebase Documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-database.html)
