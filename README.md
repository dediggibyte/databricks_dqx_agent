# Databricks DQX Agent

Generate data quality rules using AI assistance with Databricks DQX library.

## Overview

This solution provides:
- **Step 1**: Databricks App with table selection and sample data preview
- **Step 2**: AI-powered DQ rule generation via Databricks notebook job

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Databricks App (Flask)                      │
│  ┌─────────────────────┐    ┌─────────────────────────────┐ │
│  │ Step 1:             │    │ Step 2:                     │ │
│  │ - Catalog dropdown  │    │ - Prompt input              │ │
│  │ - Schema dropdown   │    │ - Trigger job               │ │
│  │ - Table dropdown    │    │ - Display generated rules   │ │
│  │ - Sample data view  │    │ - Download JSON             │ │
│  └─────────────────────┘    └──────────────┬──────────────┘ │
└────────────────────────────────────────────┼────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────┐
                              │   Databricks Job             │
                              │   (Serverless Cluster)       │
                              │                              │
                              │   1. Profile table with DQX  │
                              │   2. Generate rules with AI  │
                              │   3. Return JSON output      │
                              └──────────────────────────────┘
```

## Components

### 1. Databricks App (`app.py`)
Flask-based web application served via Gunicorn:
- **Cascading dropdowns**: Catalog → Schema → Table selection via Unity Catalog API
- **Sample data preview**: View table data via Statement Execution API
- **Prompt input**: Natural language DQ requirements
- **Job trigger**: Submits parameters to notebook job
- **Results display**: Shows generated rules with download option

### 2. DQ Rule Generation Notebook (`notebooks/generate_dq_rules.py`)
Databricks notebook for serverless job execution:
- Profiles input data using DQX Profiler
- Generates DQ rules using AI (DQX + Databricks LLM endpoints)
- Validates rules against DQX schema
- Returns structured JSON output

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

Update `app.yaml` with your Job ID:
```yaml
env:
  - name: DQ_GENERATION_JOB_ID
    value: "your-job-id"
```

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
   - Review generated rules
   - Download as JSON if needed

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
| `/health` | GET | Health check |

## References

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/)
- [AI-assisted Quality Checks Generation](https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/)
