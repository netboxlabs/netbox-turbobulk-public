# TurboBulk API Reference

## Authentication

All API endpoints require authentication via NetBox API token:

```
Authorization: Bearer <your-token>
```

NetBox v2 tokens (starting with `nbt_`) use `Bearer` authorization. Legacy tokens may use `Token` instead.

## Models Endpoints

### List Available Models

**GET** `/api/plugins/turbobulk/models/`

Returns a list of all available models with summary information.

**Response:**
```json
[
  {
    "app_label": "dcim",
    "model_name": "device",
    "full_name": "dcim.device",
    "db_table": "dcim_device",
    "verbose_name": "device",
    "supports_custom_fields": true,
    "supports_tags": true
  },
  ...
]
```

### Get Model Schema

**GET** `/api/plugins/turbobulk/models/{app_label}.{model_name}/`

Returns detailed schema information for a specific model.

**Parameters:**
- `{app_label}` - Django app label (e.g., `dcim`)
- `{model_name}` - Model name (e.g., `device`)

**Response:**
```json
{
  "app_label": "dcim",
  "model_name": "device",
  "db_table": "dcim_device",
  "verbose_name": "device",
  "fields": [
    {
      "name": "id",
      "type": "BigAutoField",
      "db_type": "bigint",
      "arrow_type": "int64",
      "nullable": false,
      "primary_key": true,
      "unique": true,
      "foreign_key": null,
      "choices": null,
      "max_length": null,
      "default": null
    },
    {
      "name": "name",
      "type": "CharField",
      "db_type": "varchar(64)",
      "arrow_type": "string",
      "nullable": false,
      "primary_key": false,
      "unique": false,
      "foreign_key": null,
      "choices": null,
      "max_length": 64,
      "default": null
    },
    {
      "name": "site",
      "type": "ForeignKey",
      "db_type": "bigint",
      "arrow_type": "int64",
      "nullable": false,
      "primary_key": false,
      "unique": false,
      "foreign_key": "dcim.site",
      "choices": null,
      "max_length": null,
      "default": null
    }
  ],
  "primary_key_field": "id",
  "unique_constraints": [
    ["name", "tenant"]
  ],
  "supports_custom_fields": true,
  "supports_tags": true
}
```

## Bulk Load Endpoint

### Submit Bulk Load Job

**POST** `/api/plugins/turbobulk/load/`

Submits a bulk insert or upsert job.

**Content-Type:** `multipart/form-data`

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | string | Yes | Model identifier (e.g., `dcim.device`) |
| `mode` | string | No | `insert` (default) or `upsert` |
| `file` | file | Yes | Data file (JSONL, JSONL.gz, or Parquet) |
| `format` | string | No | File format: `auto` (default), `jsonl`, or `parquet` |
| `conflict_fields` | array | No | Fields for upsert conflict detection (simple columns) |
| `conflict_constraint` | string | No | Named constraint for expression-based conflicts (overrides `conflict_fields`) |
| `dry_run` | boolean | No | Validate without committing (default: false) |
| `create_changelogs` | boolean | No | Generate ObjectChange records (default: true) |
| `branch` | string | No | Target branch name (requires netbox-branching) |
| `post_hooks` | object | No | Hook configuration |
| `dispatch_events` | boolean | No | Override global event dispatch setting. `true`=dispatch events, `false`=skip events, `null`=use global config (default: null) |

**Example Request:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=upsert" \
  -F "conflict_fields=name" \
  -F "conflict_fields=site_id" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

**Dry-Run Example:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "dry_run=true" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

**Load to Branch Example:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "branch=my-feature-branch" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

**Load without Changelogs (Performance Mode):**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "create_changelogs=false" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

**Upsert with Expression-Based Constraint:**

For models with expression-based unique constraints (e.g., case-insensitive name matching), use `conflict_constraint` instead of `conflict_fields`:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=upsert" \
  -F "conflict_constraint=dcim_device_unique_name_site" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

This enables upserts on constraints that include expressions like `LOWER(name)` or partial indexes. Contact NetBox Labs support if you need assistance identifying the correct constraint name for your use case.

> **Note:** When `conflict_constraint` is specified, `conflict_fields` is ignored. The constraint must exist on the target table.

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "status_url": "/api/plugins/turbobulk/jobs/550e8400-e29b-41d4-a716-446655440000/",
  "message": "Bulk insert job submitted for dcim.device",
  "dry_run": false
}
```

**Dry-Run Response (Completed):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "data": {
    "model": "dcim.device",
    "mode": "insert",
    "dry_run": true,
    "valid": true,
    "rows": 1000,
    "errors": [],
    "warnings": []
  }
}
```

## Bulk Delete Endpoint

### Submit Bulk Delete Job

**POST** `/api/plugins/turbobulk/delete/`

Submits a bulk delete job.

**Content-Type:** `multipart/form-data`

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | string | Yes | Model identifier |
| `file` | file | Yes | Data file with keys to delete (JSONL, JSONL.gz, or Parquet) |
| `format` | string | No | File format: `auto` (default), `jsonl`, or `parquet` |
| `key_fields` | array | No | Fields identifying rows (default: PK) |
| `cascade_nullable_fks` | boolean | No | Nullify nullable FK refs (default: true) |
| `dry_run` | boolean | No | Count rows without deleting (default: false) |
| `create_changelogs` | boolean | No | Generate ObjectChange records (default: true) |
| `branch` | string | No | Target branch name (requires netbox-branching) |
| `post_hooks` | object | No | Hook configuration |
| `dispatch_events` | boolean | No | Override global event dispatch setting. `true`=dispatch events, `false`=skip events, `null`=use global config (default: null) |

**Example Request:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "file=@delete_keys.parquet" \
  -F "cascade_nullable_fks=true" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/delete/"
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440001",
  "status": "pending",
  "status_url": "/api/plugins/turbobulk/jobs/550e8400-e29b-41d4-a716-446655440001/",
  "message": "Bulk delete job submitted for dcim.device",
  "dry_run": false
}
```

**Dry-Run Response (Completed):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440001",
  "status": "completed",
  "data": {
    "model": "dcim.device",
    "dry_run": true,
    "valid": true,
    "rows": 500,
    "fks_would_nullify": 25
  }
}
```

## Bulk Export Endpoint

### Submit Bulk Export Job

**POST** `/api/plugins/turbobulk/export/`

Submits a bulk export job. Supports caching: if an identical export was recently completed and data hasn't changed, returns the cached file immediately (HTTP 200) instead of creating a new job (HTTP 202).

**Content-Type:** `application/json`

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | string | Yes | Model identifier |
| `filters` | object | No | Django-style filter parameters |
| `fields` | array | No | Specific fields to export |
| `include_custom_fields` | boolean | No | Include custom_field_data (default: true) |
| `include_tags` | boolean | No | Include _tags column (default: true) |
| `format` | string | No | Export format: `jsonl` (default) or `parquet` |
| `force_refresh` | boolean | No | Bypass cache, generate fresh export (default: false) |
| `check_cache_only` | boolean | No | Only check cache status, don't create job on miss (default: false) |
| `client_cache_key` | string | No | Client's cached version key for 304 response |

**Example Request:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "dcim.device",
    "filters": {"site__name": "NYC"},
    "fields": ["id", "name", "site", "status"],
    "include_custom_fields": true,
    "include_tags": true
  }' \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/export/"
```

**Response (Cache Miss - New Job):**
```json
HTTP 202 Accepted
{
  "job_id": "550e8400-e29b-41d4-a716-446655440002",
  "status": "pending",
  "cached": false,
  "status_url": "/api/plugins/turbobulk/jobs/550e8400-e29b-41d4-a716-446655440002/",
  "message": "Bulk export job submitted for dcim.device"
}
```

**Response (Cache Hit):**
```json
HTTP 200 OK
{
  "status": "completed",
  "cached": true,
  "cache_key": "abc123def456...",
  "cache_created_at": "2025-01-24T10:00:00Z",
  "download_url": "/api/plugins/turbobulk/cache/abc123def456.../download/",
  "file_size_bytes": 524288,
  "row_count": 1000,
  "model": "dcim.device"
}
```

**Response (Client Cache Current - 304):**

When `client_cache_key` matches the current cache:
```json
HTTP 304 Not Modified
{
  "message": "Client cache is current"
}
```

**Response (Check Cache Only - Miss):**

When `check_cache_only=true` and no valid cache exists:
```json
HTTP 200 OK
{
  "cached": false,
  "data_changed": true,
  "message": "Cache invalidated or not found, call again to generate"
}
```

**Force Refresh Example:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "dcim.device",
    "force_refresh": true
  }' \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/export/"
```

### Download Cached Export

**GET** `/api/plugins/turbobulk/cache/{cache_key}/download/`

Downloads a cached export file.

**Parameters:**
- `{cache_key}` - Cache key from export response

**Response:**
- For cloud storage: Returns presigned URL
- For local storage: Streams file content

## Job Status Endpoint

### Get Job Status

**GET** `/api/plugins/turbobulk/jobs/{job_id}/`

Returns the status and results of a bulk operation job.

**Parameters:**
- `{job_id}` - UUID of the job

**Response (Pending):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Bulk Load",
  "status": "pending",
  "created": "2024-01-15T10:00:00Z",
  "started": null,
  "completed": null,
  "user": "admin",
  "data": null,
  "error": null,
  "duration_seconds": null
}
```

**Response (Completed - Success):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Bulk Load",
  "status": "completed",
  "created": "2024-01-15T10:00:00Z",
  "started": "2024-01-15T10:00:01Z",
  "completed": "2024-01-15T10:00:15Z",
  "user": "admin",
  "data": {
    "model": "dcim.device",
    "mode": "insert",
    "rows_processed": 10000,
    "rows_inserted": 10000,
    "tags_processed": 5000,
    "post_hooks": {
      "fix_denormalized": {"success": true, "rows_updated": 100},
      "fix_counters": {"success": true, "rows_updated": 50}
    }
  },
  "error": null,
  "duration_seconds": 14.5
}
```

**Response (Failed):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Bulk Load",
  "status": "errored",
  "created": "2024-01-15T10:00:00Z",
  "started": "2024-01-15T10:00:01Z",
  "completed": "2024-01-15T10:00:02Z",
  "user": "admin",
  "data": {
    "model": "dcim.device",
    "mode": "insert",
    "rows_processed": 500,
    "success": false,
    "error": {
      "error_type": "foreign_key",
      "message": "Foreign key violation: site_id=999 references a dcim_site that does not exist",
      "column": "site_id",
      "value": "999",
      "referenced_table": "dcim_site",
      "suggestion": "Ensure referenced site exists before inserting"
    }
  },
  "error": "Foreign key violation",
  "duration_seconds": 1.2
}
```

## Post Hooks Configuration

Post hooks can be configured per-request:

```json
{
  "post_hooks": {
    "fix_denormalized": true,
    "rebuild_search_index": true,
    "fix_counters": true
  }
}
```

| Hook | Default | Description |
|------|---------|-------------|
| `fix_denormalized` | true | Fix denormalized site/location fields |
| `rebuild_search_index` | true | Trigger NetBox search reindex |
| `fix_counters` | true | Update counter cache fields |

## Error Responses

### Validation Error (400)
```json
{
  "model": ["Model must be in format 'app_label.model_name'"]
}
```

### Model Not Found (400)
```json
{
  "error": "Model not found: dcim.nonexistent",
  "error_type": "model_not_found"
}
```

### Permission Denied (403)
```json
{
  "detail": "User lacks permission: dcim.add_device"
}
```

### File Too Large (413)
```json
{
  "error_type": "file_size_exceeded",
  "message": "File size (2000000000 bytes) exceeds maximum allowed size (1000000000 bytes)",
  "file_size": 2000000000,
  "max_size": 1000000000
}
```

### Job Not Found (404)
```json
{
  "detail": "Not found."
}
```

## Data Format Requirements

TurboBulk supports two data formats:

### JSONL Format (Default)

JSONL (JSON Lines) is the default and recommended format:
- One JSON object per line
- Supports gzip compression (`.jsonl.gz`)
- Auto-detected from file extension or content

**Example:**
```json
{"name": "device-001", "site_id": 1, "status": "active"}
{"name": "device-002", "site_id": 1, "status": "active"}
```

### Parquet Format

Parquet is supported for high-performance operations:
- Columnar format with built-in compression
- Requires PyArrow or similar library to create
- 25-50% faster throughput than JSONL, but gzipped JSONL files are 40-60% smaller

### Field Type Mapping

| Django Field | JSONL Type | Parquet/Arrow Type |
|--------------|------------|-------------------|
| BigAutoField, BigIntegerField | number | int64 |
| IntegerField | number | int32 |
| CharField, TextField | string | string |
| BooleanField | boolean | bool |
| DateField | string (ISO) | date32 |
| DateTimeField | string (ISO) | timestamp |
| FloatField | number | float64 |
| DecimalField | string/number | decimal128 |
| ForeignKey | number | int64 (PK of related) |
| JSONField | object | string (serialized JSON) |

### Special Columns

| Column | Type | Description |
|--------|------|-------------|
| `custom_field_data` | string | JSON object with custom field values |
| `_tags` | list<string> | List of tag slugs for post-processing |

### Example Parquet Schema (PyArrow)

> **IMPORTANT:** FK columns must use the `_id` suffix (database column name).

```python
import pyarrow as pa

schema = pa.schema([
    ('name', pa.string()),
    ('site_id', pa.int64()),         # FK uses _id suffix
    ('device_type_id', pa.int64()),  # FK uses _id suffix
    ('role_id', pa.int64()),         # FK uses _id suffix
    ('status', pa.string()),
    ('serial', pa.string()),
    ('asset_tag', pa.string()),
    ('custom_field_data', pa.string()),
    ('_tags', pa.list_(pa.string())),
])
```

## Changelog Support

TurboBulk generates ObjectChange records by default for full audit trail support in NetBox.

### Behavior

| Operation | Action | prechange_data | postchange_data |
|-----------|--------|----------------|-----------------|
| Insert | `create` | null | Full row data |
| Upsert | `update` | Original row data | Updated row data |
| Delete | `delete` | Full row data | null |

### Disabling Changelogs

Set `create_changelogs=false` to skip changelog generation for better performance:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "create_changelogs=false" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

### Performance Impact

Changelog generation adds overhead that scales with the number of rows. At large scale (100K+ rows), enabling changelogs can significantly impact operation time.

Consider disabling changelogs for:
- Initial data migrations where audit trail is not required
- Large batch operations (>100K rows) where performance is critical
- Ephemeral or test data

### Job Result Data

When changelogs are enabled, job results include `changelogs_created`:

```json
{
  "data": {
    "model": "dcim.device",
    "rows_inserted": 1000,
    "changelogs_created": 1000
  }
}
```

## NetBox Branching Support

TurboBulk integrates with the [netbox-branching](https://github.com/netbox-community/netbox-branching) plugin for branch-aware bulk operations.

### Prerequisites

- `netbox-branching` plugin installed and configured
- Branch must exist and be in `READY` state

### Usage

Specify the `branch` parameter to route operations to a branch schema:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "branch=feature-new-datacenter" \
  -F "file=@devices.parquet" \
  "https://your-instance.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

### How It Works

When `branch` is specified:
1. API validates branch exists and is in READY state
2. Operation runs within the branch's PostgreSQL schema
3. ObjectChange records are created (visible in branch changelog)
4. ChangeDiff records are generated for merge conflict detection
5. Data is isolated from main schema until branch is merged

### Response with Branch

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Bulk insert job submitted for dcim.device in branch feature-new-datacenter",
  "dry_run": false
}
```

### Error Responses

**Branch Not Found (400):**
```json
{
  "branch": ["Branch 'nonexistent-branch' not found"]
}
```

**Branch Not Ready (400):**
```json
{
  "branch": ["Branch 'my-branch' is not in READY state (status: provisioning)"]
}
```

**Plugin Not Installed (400):**
```json
{
  "branch": ["netbox-branching plugin not installed"]
}
```

### Workflow Example

1. Create a branch via NetBox UI or API
2. Wait for branch to reach READY state
3. Bulk load data to branch using `branch` parameter
4. Review changes in NetBox Branch UI
5. Merge branch to main when satisfied
6. TurboBulk data is now in main schema

See [Branching Documentation](branching.md) for detailed workflow examples.
