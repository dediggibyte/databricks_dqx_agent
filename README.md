# DQX - Data Quality Rule Generator

A Databricks App for generating data quality rules using AI assistance with [Databricks DQX](https://databrickslabs.github.io/dqx/).

## Overview

This application provides a user-friendly interface for:
1. **Browsing Unity Catalog** - Select catalogs, schemas, and tables
2. **Generating DQ Rules** - Use AI to generate data quality rules based on natural language requirements
3. **Reviewing & Editing** - Review generated rules in JSON format with full editing capabilities
4. **AI Analysis** - Get AI-powered insights on rule coverage and recommendations
5. **Saving to Lakebase** - Store rules with versioning in Databricks Lakebase (PostgreSQL)

## Architecture

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
├── app.yaml                  # Databricks App configuration
├── requirements.txt          # Python dependencies
├── README.md                 # This file
├── RUNBOOK.md               # Operational guide for teams
│
├── app/                      # Application package
│   ├── __init__.py          # Flask app factory
│   ├── config.py            # Configuration management
│   │
│   ├── routes/              # API endpoints
│   │   ├── __init__.py
│   │   ├── catalog.py       # Unity Catalog routes
│   │   ├── rules.py         # DQ Rules routes
│   │   └── lakebase.py      # Lakebase routes
│   │
│   └── services/            # Business logic
│       ├── __init__.py
│       ├── databricks.py    # Databricks SDK service
│       ├── lakebase.py      # Lakebase service
│       └── ai.py            # AI analysis service
│
├── notebooks/               # Databricks notebooks
│   ├── generate_dq_rules.py      # Full notebook (detailed output)
│   └── generate_dq_rules_fast.py # Optimized notebook (faster)
│
├── templates/               # HTML templates
│   └── index.html           # Main UI template
│
└── docs/                    # Documentation
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless recommended)
- Lakebase instance (optional, for saving rules)
- Model Serving endpoint with Claude (for AI analysis)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd databricks_dqx_agent
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Upload the notebook to Databricks**
   - Upload `notebooks/generate_dq_rules_fast.py` to your Databricks workspace
   - Location: `/Workspace/Users/<your-email>/dqx_agent/generate_dq_rules_fast`

4. **Create a Databricks Job**
   - Create a job that runs the notebook
   - Add parameters: `table_name`, `user_prompt`
   - Note the Job ID

5. **Configure app.yaml**
   ```yaml
   env:
     - name: DQ_GENERATION_JOB_ID
       value: "<your-job-id>"

     - name: LAKEBASE_HOST
       value: "<your-lakebase-host>"

     - name: MODEL_SERVING_ENDPOINT
       value: "databricks-claude-sonnet-4-5"
   ```

6. **Deploy to Databricks Apps**
   ```bash
   databricks apps deploy . --app-name dq-rule-generator
   ```

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `DQ_GENERATION_JOB_ID` | Databricks Job ID for rule generation | Yes |
| `LAKEBASE_HOST` | Lakebase PostgreSQL host | No |
| `LAKEBASE_DATABASE` | Lakebase database name | No (default: `databricks_postgres`) |
| `MODEL_SERVING_ENDPOINT` | AI model endpoint | No (default: `databricks-claude-sonnet-4-5`) |
| `SAMPLE_DATA_LIMIT` | Max rows for preview | No (default: `100`) |

## Authentication

This app uses **OAuth User Authorization** for all connections:
- **Unity Catalog**: Uses the logged-in user's Databricks credentials
- **Lakebase**: Uses OAuth token from `x-forwarded-access-token` header
- **No passwords stored** in configuration

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main UI |
| `/health` | GET | Health check |
| `/api/catalogs` | GET | List catalogs |
| `/api/schemas/<catalog>` | GET | List schemas |
| `/api/tables/<catalog>/<schema>` | GET | List tables |
| `/api/sample/<catalog>/<schema>/<table>` | GET | Get sample data |
| `/api/generate` | POST | Trigger rule generation |
| `/api/status/<run_id>` | GET | Get job status |
| `/api/analyze` | POST | AI analysis of rules |
| `/api/confirm` | POST | Save rules to Lakebase |
| `/api/history/<table_name>` | GET | Get rule history |
| `/api/lakebase/status` | GET | Check Lakebase connection |

## DQX Check Functions

The generated rules use DQX's built-in check functions. See [DQX Quality Checks Reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/) for full details.

### Row-Level Checks

| Function | Description |
|----------|-------------|
| `is_not_null` | Value is not null |
| `is_not_empty` | Value is not empty string |
| `is_not_null_and_not_empty` | Not null and not empty string |
| `is_in_list` | Value is in allowed list |
| `is_not_in_list` | Value is not in forbidden list |
| `is_not_null_and_is_in_list` | Not null AND in allowed values |
| `is_not_null_and_not_empty_array` | Array is non-null and non-empty |
| `is_in_range` | Value within min/max boundaries |
| `is_not_in_range` | Value outside min/max boundaries |
| `is_equal_to` | Value matches specific value |
| `is_not_equal_to` | Value differs from specific value |
| `is_not_less_than` | Value meets minimum threshold |
| `is_not_greater_than` | Value respects maximum threshold |
| `is_valid_date` | Valid date format |
| `is_valid_timestamp` | Valid timestamp format |
| `is_valid_json` | Valid JSON string |
| `has_json_keys` | JSON has required keys |
| `has_valid_json_schema` | JSON conforms to schema |
| `is_not_in_future` | Timestamp not in future |
| `is_not_in_near_future` | Timestamp within acceptable future window |
| `is_older_than_n_days` | Date precedes reference by N days |
| `is_older_than_col2_for_n_days` | Column1 older than column2 by N days |
| `regex_match` | Value matches regex pattern |
| `is_valid_ipv4_address` | Valid IPv4 format |
| `is_ipv4_address_in_cidr` | IPv4 within CIDR block |
| `is_valid_ipv6_address` | Valid IPv6 format |
| `is_ipv6_address_in_cidr` | IPv6 within CIDR block |
| `sql_expression` | Custom SQL-based condition |
| `is_data_fresh` | Data not stale beyond max age |
| `does_not_contain_pii` | No personally identifiable info |
| `is_latitude` | Value between -90 and 90 |
| `is_longitude` | Value between -180 and 180 |
| `is_geometry` | Valid geometry value |
| `is_geography` | Valid geography value |
| `is_point` | Geometry type is point |
| `is_linestring` | Geometry type is linestring |
| `is_polygon` | Geometry type is polygon |
| `is_multipoint` | Geometry type is multipoint |
| `is_multilinestring` | Geometry type is multilinestring |
| `is_multipolygon` | Geometry type is multipolygon |
| `is_geometrycollection` | Geometry type is collection |
| `is_ogc_valid` | Geometry valid per OGC standard |
| `is_non_empty_geometry` | Geometry contains coordinates |
| `is_not_null_island` | Not at null island (0,0) |
| `has_dimension` | Geometry has specified dimension |
| `has_x_coordinate_between` | X coordinates within range |
| `has_y_coordinate_between` | Y coordinates within range |
| `is_area_not_less_than` | Geometry area meets minimum |
| `is_area_not_greater_than` | Geometry area respects maximum |
| `is_num_points_not_less_than` | Coordinate count meets minimum |
| `is_num_points_not_greater_than` | Coordinate count respects maximum |

### Dataset-Level Checks

| Function | Description |
|----------|-------------|
| `is_unique` | Values/composite keys have no duplicates |
| `is_aggr_not_greater_than` | Aggregated value respects maximum |
| `is_aggr_not_less_than` | Aggregated value meets minimum |
| `is_aggr_equal` | Aggregated value matches target |
| `is_aggr_not_equal` | Aggregated value differs from target |
| `foreign_key` | Values exist in reference dataset |
| `sql_query` | Custom SQL validates condition |
| `compare_datasets` | Compare source against reference |
| `is_data_fresh_per_time_window` | Records arrive in time windows |
| `has_valid_schema` | DataFrame schema matches expected |
| `has_no_outliers` | Detects statistical outliers |

## Development

### Local Development

```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DQ_GENERATION_JOB_ID="your-job-id"

# Run locally
python wsgi.py
```

## Resources

- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Databricks Apps Guide](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Lakebase Documentation](https://docs.databricks.com/aws/en/oltp/index.html)
