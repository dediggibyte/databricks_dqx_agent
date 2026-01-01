# API Reference

## Endpoints

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

## Details

### GET /api/catalogs
Returns list of available Unity Catalog catalogs.

**Response:**
```json
{
  "catalogs": ["catalog1", "catalog2"]
}
```

### GET /api/schemas/{catalog}
Returns list of schemas in a catalog.

**Response:**
```json
{
  "schemas": ["schema1", "schema2"]
}
```

### GET /api/tables/{catalog}/{schema}
Returns list of tables in a schema.

**Response:**
```json
{
  "tables": ["table1", "table2"]
}
```

### GET /api/sample/{catalog}/{schema}/{table}
Returns sample data from a table.

**Response:**
```json
{
  "columns": ["col1", "col2"],
  "data": [["val1", "val2"], ...]
}
```

### POST /api/generate
Triggers DQ rule generation job.

**Request:**
```json
{
  "table_name": "catalog.schema.table",
  "user_prompt": "Generate rules for..."
}
```

**Response:**
```json
{
  "run_id": "12345"
}
```

### GET /api/status/{run_id}
Returns job run status and results.

**Response:**
```json
{
  "status": "TERMINATED",
  "result": {...}
}
```

### POST /api/analyze
AI analysis of generated rules.

**Request:**
```json
{
  "rules": [...],
  "table_name": "catalog.schema.table"
}
```

**Response:**
```json
{
  "analysis": "..."
}
```

### POST /api/confirm
Saves rules to Lakebase.

**Request:**
```json
{
  "table_name": "catalog.schema.table",
  "rules": [...],
  "user_prompt": "..."
}
```

**Response:**
```json
{
  "success": true,
  "version": 1
}
```

### GET /api/history/{table_name}
Returns rule history for a table.

**Response:**
```json
{
  "history": [
    {"version": 1, "created_at": "...", "rules": [...]}
  ]
}
```

### GET /api/lakebase/status
Checks Lakebase connection status.

**Response:**
```json
{
  "connected": true,
  "host": "..."
}
```
