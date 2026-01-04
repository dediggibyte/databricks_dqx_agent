# API Reference

This document describes all REST API endpoints provided by DQX Data Quality Manager.

---

## Overview

| Category | Base Path | Authentication |
|----------|-----------|----------------|
| Health | `/health` | None |
| Catalog | `/api/catalogs`, `/api/schemas`, `/api/tables` | OBO |
| Generation | `/api/generate`, `/api/status` | SP (jobs) |
| Validation | `/api/validate` | SP (jobs) |
| Analysis | `/api/analyze` | OBO |
| Storage | `/api/confirm`, `/api/history` | OAuth |
| Lakebase | `/api/lakebase` | OAuth |

---

## Health Endpoints

### GET /health

Health check endpoint for monitoring.

**Authentication:** None

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00Z"
}
```

---

## Catalog Endpoints

These endpoints use **OBO authentication** - results are filtered by the user's Unity Catalog permissions.

### GET /api/catalogs

List all accessible catalogs.

**Response:**
```json
{
  "catalogs": ["main", "hive_metastore", "samples"]
}
```

**Errors:**
```json
{
  "error": "Unable to list catalogs: [error message]"
}
```

---

### GET /api/schemas/{catalog}

List schemas in a catalog.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `catalog` | path | Catalog name |

**Response:**
```json
{
  "schemas": ["default", "bronze", "silver", "gold"]
}
```

---

### GET /api/tables/{catalog}/{schema}

List tables in a schema.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `catalog` | path | Catalog name |
| `schema` | path | Schema name |

**Response:**
```json
{
  "tables": ["customers", "orders", "products"]
}
```

---

### GET /api/sample/{catalog}/{schema}/{table}

Get sample data from a table.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `catalog` | path | Catalog name |
| `schema` | path | Schema name |
| `table` | path | Table name |

**Query Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | integer | 100 | Maximum rows to return |

**Response:**
```json
{
  "columns": ["id", "name", "email", "created_at"],
  "rows": [
    {"id": 1, "name": "John", "email": "john@example.com", "created_at": "2025-01-01"},
    {"id": 2, "name": "Jane", "email": "jane@example.com", "created_at": "2025-01-02"}
  ],
  "row_count": 2
}
```

---

## Generation Endpoints

### POST /api/generate

Trigger DQ rule generation job.

**Authentication:** App Service Principal (for job triggering)

**Request Body:**
```json
{
  "table_name": "catalog.schema.table",
  "user_prompt": "Ensure all required fields are not null and email is valid format",
  "sample_limit": 1000
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table_name` | string | Yes | Fully qualified table name |
| `user_prompt` | string | Yes | Natural language requirements |
| `sample_limit` | integer | No | Rows to sample for profiling |

**Response:**
```json
{
  "run_id": "12345678901234567"
}
```

**Errors:**
```json
{
  "error": "DQ_GENERATION_JOB_ID not configured"
}
```

---

### GET /api/status/{run_id}

Get job run status and results.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `run_id` | path | Job run ID from `/api/generate` |

**Response (Running):**
```json
{
  "status": "running",
  "state": "RUNNING"
}
```

**Response (Completed):**
```json
{
  "status": "completed",
  "result": {
    "rules": [
      {
        "criticality": "error",
        "check": {
          "function": "is_not_null",
          "arguments": {"col_name": "customer_id"}
        },
        "name": "customer_id_not_null"
      }
    ],
    "column_profiles": {
      "customer_id": {"null_count": 0, "distinct_count": 1000}
    },
    "metadata": {
      "table_name": "catalog.schema.customers",
      "row_count": 10000
    }
  }
}
```

**Response (Failed):**
```json
{
  "status": "failed",
  "message": "Job failed: [error details]"
}
```

---

## Validation Endpoints

### POST /api/validate

Trigger DQ rule validation job.

**Request Body:**
```json
{
  "table_name": "catalog.schema.table",
  "rules": [
    {
      "criticality": "error",
      "check": {
        "function": "is_not_null",
        "arguments": {"col_name": "customer_id"}
      },
      "name": "customer_id_not_null"
    }
  ]
}
```

**Response:**
```json
{
  "run_id": "98765432109876543"
}
```

---

### GET /api/validate/status/{run_id}

Get validation job status and results.

**Response (Completed):**
```json
{
  "status": "completed",
  "result": {
    "total_rules": 5,
    "passed": 4,
    "failed": 1,
    "warnings": 0,
    "rule_results": [
      {
        "rule_name": "customer_id_not_null",
        "column": "customer_id",
        "status": "pass",
        "violation_count": 0,
        "details": "All values are non-null"
      },
      {
        "rule_name": "email_format",
        "column": "email",
        "status": "fail",
        "violation_count": 15,
        "details": "15 rows have invalid email format"
      }
    ]
  }
}
```

---

## Analysis Endpoints

### POST /api/analyze

AI analysis of generated rules.

**Authentication:** OBO (uses user token for ai_query)

**Request Body:**
```json
{
  "rules": [...],
  "table_name": "catalog.schema.table",
  "user_prompt": "Original user requirements"
}
```

**Response:**
```json
{
  "success": true,
  "analysis": {
    "summary": "Generated 8 data quality rules covering key aspects...",
    "coverage_score": 85,
    "strengths": [
      "Good null checks on required fields",
      "Email format validation present"
    ],
    "recommendations": [
      "Consider adding range checks for numeric fields",
      "Add foreign key validation for order_id"
    ],
    "rule_assessment": [
      {
        "rule": "customer_id_not_null",
        "assessment": "Essential - correctly validates primary key"
      }
    ]
  }
}
```

**Errors:**
```json
{
  "success": false,
  "error": "AI analysis failed: [error message]"
}
```

---

## Storage Endpoints

### POST /api/confirm

Save rules to Lakebase with versioning.

**Authentication:** User OAuth (Lakebase)

**Request Body:**
```json
{
  "table_name": "catalog.schema.table",
  "rules": [...],
  "user_prompt": "Original user requirements",
  "ai_summary": {
    "coverage_score": 85,
    "summary": "..."
  }
}
```

**Response:**
```json
{
  "success": true,
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "version": 3,
  "created_at": "2025-01-15T10:30:00Z"
}
```

**Errors:**
```json
{
  "success": false,
  "error": "Lakebase connection not configured"
}
```

---

### GET /api/history/{table_name}

Get rule history for a table.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `table_name` | path | URL-encoded fully qualified table name |

**Query Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | integer | 10 | Maximum versions to return |

**Response:**
```json
{
  "success": true,
  "history": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "version": 3,
      "rules": [...],
      "user_prompt": "Ensure all required fields...",
      "ai_summary": {...},
      "created_at": "2025-01-15T10:30:00Z",
      "is_active": true
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "version": 2,
      "rules": [...],
      "user_prompt": "Check for nulls...",
      "ai_summary": {...},
      "created_at": "2025-01-10T14:20:00Z",
      "is_active": false
    }
  ]
}
```

---

## Lakebase Endpoints

### GET /api/lakebase/status

Check Lakebase connection status.

**Response (Connected):**
```json
{
  "connected": true,
  "configured": true,
  "host": "ep-xxx.database.us-east-1.cloud.databricks.com",
  "database": "databricks_postgres",
  "auth_type": "oauth",
  "user": "user@company.com"
}
```

**Response (Not Configured):**
```json
{
  "connected": false,
  "configured": false,
  "message": "Lakebase host not configured"
}
```

**Response (Auth Error):**
```json
{
  "connected": false,
  "configured": true,
  "message": "No OAuth token - user must be authenticated via Databricks Apps"
}
```

---

## Debug Endpoints

### GET /api/debug

Debug endpoint showing configuration status.

!!! warning "Development Only"
    This endpoint should be disabled in production.

**Response:**
```json
{
  "databricks_host": "https://your-workspace.cloud.databricks.com",
  "sql_warehouse_id": "abc123...",
  "generation_job_id": "12345...",
  "validation_job_id": "67890...",
  "lakebase_configured": true,
  "model_endpoint": "databricks-claude-sonnet-4-5"
}
```

---

## Error Handling

All endpoints return errors in a consistent format:

```json
{
  "error": "Error message describing what went wrong"
}
```

Or for endpoints with success flag:

```json
{
  "success": false,
  "error": "Error message describing what went wrong"
}
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad Request - Invalid input |
| 401 | Unauthorized - Missing or invalid token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource doesn't exist |
| 500 | Internal Server Error |

---

## Rate Limiting

The API does not implement rate limiting at the application level. Rate limits are enforced by underlying Databricks services:

- SQL Warehouse: Concurrent query limits
- Jobs API: Job submission limits
- Model Serving: Request rate limits

---

## Related Documentation

- [Authentication](authentication.md) - How API authentication works
- [Configuration](configuration.md) - Configuring API endpoints
- [Architecture](architecture.md) - System design overview
