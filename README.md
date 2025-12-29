# Databricks DQX Agent

This repository provides a complete solution for creating data quality configurations using the Databricks DQX library with AI assistance from Databricks hosted model serving endpoints.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Databricks App (Streamlit)                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐   │
│  │ Table Dropdown   │  │ Prompt Input     │  │ Rule Editor              │   │
│  │ + Sample Data    │  │ + Job Trigger    │  │ + Confirmation           │   │
│  └────────┬─────────┘  └────────┬─────────┘  └───────────┬──────────────┘   │
└───────────┼─────────────────────┼────────────────────────┼──────────────────┘
            │                     │                        │
            │                     ▼                        │
            │         ┌──────────────────────┐             │
            │         │ Databricks Job       │             │
            │         │ (Serverless Cluster) │             │
            │         │                      │             │
            │         │ ┌──────────────────┐ │             │
            │         │ │ DQX Profiler     │ │             │
            │         │ │ + AI Generation  │ │             │
            │         │ └──────────────────┘ │             │
            │         └──────────┬───────────┘             │
            │                    │                         │
            │                    ▼                         │
            │         ┌──────────────────────┐             │
            │         │ AgentBricks Tool     │             │
            │         │ (DQ Rule Summarizer) │             │
            │         └──────────┬───────────┘             │
            │                    │                         │
            └────────────────────┼─────────────────────────┘
                                 │
                                 ▼
                      ┌──────────────────────┐
                      │ Lakebase (PostgreSQL)│
                      │ - Versioned Rules    │
                      │ - Event Tracking     │
                      └──────────────────────┘
```

## Components

### 1. Databricks App (`app/app.py`)
A Streamlit-based web application that provides:
- **Table Selection**: Dropdown to browse catalogs, schemas, and tables
- **Sample Data Preview**: View up to 100 rows of selected table data
- **Prompt Input**: Natural language interface for describing DQ rules
- **Rule Editor**: JSON editor for reviewing and modifying generated rules
- **Confirmation & Save**: Store validated rules to Lakebase with versioning

### 2. DQ Rule Generation Notebook (`notebooks/generate_dq_rules.py`)
A Databricks notebook designed to run as a serverless job:
- Profiles input data using DQX Profiler
- Generates DQ rules using AI (DSPy + Databricks LLM endpoints)
- Validates rules against DQX schema
- Returns structured JSON output

### 3. AgentBricks Tool (`tools/dq_rule_summarizer.py`)
An AI-powered tool for understanding and summarizing DQ rules:
- Provides human-readable rule summaries
- Returns editable JSON rule definitions
- Offers improvement recommendations
- Tracks affected columns and criticality levels

### 4. Lakebase Client (`lakebase/client.py`)
PostgreSQL client for rule persistence:
- OAuth token-based authentication
- Automatic version management
- Rule event tracking
- History and rollback support

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Lakebase instance provisioned
- Model serving endpoint configured
- Python 3.10+

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd databricks_dqx_agent
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DQ_GENERATION_JOB_ID="your-job-id"
export LAKEBASE_HOST="your-lakebase-host"
export LAKEBASE_DATABASE="dqx_rules_db"
export MODEL_SERVING_ENDPOINT="databricks-meta-llama-3-1-70b-instruct"
```

### Databricks App Deployment

1. Upload the repository to your Databricks workspace
2. Create a new Databricks App using `app.yaml`
3. Configure the required environment variables
4. Add Lakebase as a resource

### Job Configuration

1. Import `notebooks/generate_dq_rules.py` to your workspace
2. Create a new job with serverless compute
3. Configure notebook parameters
4. Note the Job ID for the app configuration

### Lakebase Setup

Initialize the database schema:
```python
from lakebase import get_lakebase_client

client = get_lakebase_client()
client.initialize_schema()
```

## Usage

### Using the App

1. Open the Databricks App URL
2. Select a catalog, schema, and table from the dropdowns
3. Review the sample data preview
4. Enter a natural language prompt describing your DQ requirements
5. Click "Generate DQ Rules" to trigger the AI generation
6. Review and edit the generated rules in the JSON editor
7. Click "Confirm and Save Rule" to persist to Lakebase

### Using the AgentBricks Tool

```python
from tools import execute_tool

# Example DQ rules
rules = [
    {
        "name": "valid_email",
        "criticality": "error",
        "check": {
            "function": "matches_regex",
            "arguments": {
                "col_name": "email",
                "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
            }
        }
    }
]

# Get summary
result = execute_tool(
    rules=rules,
    table_name="main.sales.customers",
    use_llm=True
)

print(result["summary"])
print(result["recommendations"])
```

### Using the Lakebase Client

```python
from lakebase import get_lakebase_client

client = get_lakebase_client()

# Save a rule
event = client.save_rule_event(
    table_name="main.sales.transactions",
    rule_definition={"rules": [...]},
    created_by="user@example.com",
    metadata={"source": "ai_generated"}
)

print(f"Saved version {event.version}")

# Get latest rule
latest = client.get_latest_rule("main.sales.transactions")

# Get history
history = client.get_rule_history("main.sales.transactions", limit=5)
```

## DQ Rule Format

Generated rules follow the DQX-compatible format:

```json
[
  {
    "name": "rule_name",
    "criticality": "error|warn|info",
    "check": {
      "function": "check_function_name",
      "arguments": {
        "col_name": "column_name",
        ...
      }
    },
    "filter": "optional SQL filter expression"
  }
]
```

### Available Check Functions
- `is_not_null` - Column is not null
- `is_not_null_and_not_empty` - Column is not null and not empty string
- `is_greater_than` - Numeric comparison
- `is_less_than` - Numeric comparison
- `is_in_list` - Value is in allowed list
- `matches_regex` - Value matches pattern
- `is_unique` - Column values are unique
- And many more from DQX library

## Project Structure

```
databricks_dqx_agent/
├── app/
│   ├── __init__.py
│   └── app.py              # Streamlit application
├── config/
│   ├── __init__.py
│   └── settings.py         # Configuration management
├── lakebase/
│   ├── __init__.py
│   └── client.py           # Lakebase client
├── notebooks/
│   ├── __init__.py
│   └── generate_dq_rules.py  # DQ generation notebook
├── tools/
│   ├── __init__.py
│   ├── agentbricks_registration.py  # AgentBricks integration
│   └── dq_rule_summarizer.py        # Summarizer tool
├── utils/
│   ├── __init__.py
│   ├── databricks_client.py  # Databricks SDK utilities
│   └── spark_utils.py        # Spark utilities
├── app.yaml                  # Databricks App config
├── requirements.txt
└── README.md
```

## References

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Lakebase Documentation](https://docs.databricks.com/aws/en/oltp/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/)
- [DSPy Framework](https://dspy-docs.vercel.app/)
