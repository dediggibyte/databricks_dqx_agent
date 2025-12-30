# Databricks DQX Agent

Generate data quality rules using AI assistance with Databricks DQX library.

## Overview

This solution provides:
- **Step 1**: Databricks App with table dropdown and sample data preview
- **Step 2**: AI-powered DQ rule generation via Databricks notebook job

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Databricks App (Streamlit)                  │
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

### 1. Databricks App (`app/app.py`)
Streamlit-based web application:
- **Cascading dropdowns**: Catalog → Schema → Table selection
- **Sample data preview**: View table data with configurable row limit
- **Column information**: Data types and null counts
- **Prompt input**: Natural language DQ requirements
- **Job trigger**: Submits prompt to notebook job
- **Results display**: Shows generated rules with download option

### 2. DQ Rule Generation Notebook (`notebooks/generate_dq_rules.py`)
Databricks notebook for serverless job execution:
- Profiles input data using DQX Profiler
- Generates DQ rules using AI (DSPy + Databricks LLM endpoints)
- Validates rules against DQX schema
- Returns structured JSON output

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Model serving endpoint (e.g., `databricks-meta-llama-3-1-70b-instruct`)
- Python 3.10+

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
3. Save and note the **Job ID**

### Step 3: Deploy the Databricks App

Option A - Using Databricks CLI:
```bash
databricks apps deploy dq-rule-generator --source-path /path/to/databricks_dqx_agent
```

Option B - Manual deployment:
1. Go to **Compute → Apps → Create App**
2. Upload the `app/` folder
3. Set entry point to `app/app.py`

### Step 4: Configure the App

In the app sidebar, enter your **DQ Generation Job ID**

Or set environment variable:
```bash
export DQ_GENERATION_JOB_ID="your-job-id"
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
[
  {
    "name": "valid_email_check",
    "criticality": "error",
    "check": {
      "function": "matches_regex",
      "arguments": {
        "col_name": "email",
        "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      }
    },
    "filter": null
  },
  {
    "name": "positive_amount",
    "criticality": "warn",
    "check": {
      "function": "is_greater_than",
      "arguments": {
        "col_name": "amount",
        "limit": 0
      }
    },
    "filter": null
  }
]
```

## Project Structure

```
databricks_dqx_agent/
├── app/
│   ├── __init__.py
│   └── app.py              # Streamlit application
├── notebooks/
│   ├── __init__.py
│   └── generate_dq_rules.py  # DQ generation notebook
├── app.yaml                  # Databricks App config
├── requirements.txt
└── README.md
```

## References

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/)
- [AI-assisted Quality Checks Generation](https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation/)
