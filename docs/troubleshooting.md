# TurboBulk Troubleshooting Guide

This guide covers common issues, error messages, and solutions when using TurboBulk.

## Common Errors

### Foreign Key Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `FK value=0` | Wrong FK column name | Use `_id` suffix (e.g., `site_id` not `site`) |
| `foreign key violation` | Referenced object doesn't exist | Load parent objects first |
| `site_id=999 references a dcim_site that does not exist` | Invalid FK value | Verify the referenced object exists |

**Example fix for FK column naming:**
```python
# WRONG - causes FK value=0
table = pa.table({
    'name': ['device-1'],
    'site': [123],  # Wrong! Missing _id suffix
})

# CORRECT
table = pa.table({
    'name': ['device-1'],
    'site_id': [123],  # Correct: uses _id suffix
})
```

### Unique Constraint Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `unique constraint violation` | Duplicate key in data | Deduplicate source data |
| `duplicate key value violates unique constraint` | Record already exists | Use `mode='upsert'` instead of `insert` |

**Solutions:**
1. **Deduplicate your source data** before loading
2. **Use upsert mode** for data that may already exist:
   ```bash
   curl -X POST ... -F "mode=upsert" -F "conflict_fields=name" ...
   ```

### Schema Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `schema mismatch` | Parquet columns don't match model | Regenerate schema from `/models/` endpoint |
| `NOT NULL violation` | Required field missing | Include all required fields |
| `string_too_long` | Value exceeds max length | Truncate strings to fit field limits |

**Getting the correct schema:**
```bash
curl -H "Authorization: Bearer $TOKEN" \
  "$NETBOX_URL/api/plugins/turbobulk/models/dcim.device/"
```

### Pre-Validation Errors

TurboBulk runs pre-validation for IP addresses and prefixes (models with inet-based rules):

| Error | Cause | Resolution |
|-------|-------|------------|
| `Rule ipaddress_network_broadcast found N violations` | IP address is network/broadcast address | Use host addresses (e.g., 10.0.0.1/24 not 10.0.0.0/24) |
| `Rule ipaddress_vrf_uniqueness found N violations` | Duplicate IP in VRF with enforce_unique=True | Use unique IP or different VRF |
| `Rule prefix_network_portion found N violations` | Prefix has non-zero host bits | Use network address (e.g., 10.0.0.0/24 not 10.0.0.5/24) |
| `Rule prefix_vrf_uniqueness found N violations` | Duplicate prefix in VRF with enforce_unique=True | Use unique prefix or different VRF |

**Validation modes:**
- `validation_mode=auto` (default): Pre-validation for IP addresses and prefixes
- `validation_mode=full`: Django full_clean() on each row (slower but catches all issues)
- `validation_mode=none`: Skip pre-validation (fastest, use for trusted data only)

**Example:**
```bash
# Force full Django validation for complex models
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.cable" \
  -F "mode=insert" \
  -F "validation_mode=full" \
  -F "file=@cables.parquet" \
  "$NETBOX_URL/api/plugins/turbobulk/load/"

# Skip validation for trusted migration data
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.site" \
  -F "mode=insert" \
  -F "validation_mode=none" \
  -F "file=@sites.parquet" \
  "$NETBOX_URL/api/plugins/turbobulk/load/"
```

### Permission Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `permission denied` | User lacks model permissions | Request permissions from your administrator |
| `User lacks permission: dcim.add_device` | Missing add permission | Request `add_device` permission |

**Required permissions by operation:**
- **Insert**: `add` permission on target model
- **Upsert**: `add` + `change` permissions
- **Delete**: `delete` permission
- **Export**: `view` permission

### File Upload Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `File size exceeded` | Upload too large | Split your data into smaller files |
| `Invalid Parquet file` | Corrupted or wrong format | Verify file with `parquet-tools` or PyArrow |

**Checking your Parquet file:**
```python
import pyarrow.parquet as pq

table = pq.read_table('devices.parquet')
print("Schema:", table.schema)
print("Rows:", table.num_rows)
```

### Branch Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Branch not found` | Branch doesn't exist | Create the branch first via NetBox UI or API |
| `Branch is not in READY state` | Branch is provisioning | Wait for READY status |
| `Branching not available` | Feature not enabled | Contact support |

### Job Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Job timeout` | Operation took too long | Split data into smaller files |
| `Job not found` | Invalid job ID | Verify job ID from submit response |
| `Job errored` | Operation failed | Check job `data.error` for details |

## Debugging Workflow

### 1. Check Job Status

```bash
# Get detailed job status
curl -H "Authorization: Bearer $TOKEN" \
  "$NETBOX_URL/api/plugins/turbobulk/jobs/{job_id}/"
```

The response includes:
- `status`: pending, running, completed, errored
- `data.error`: Detailed error information
- `data.rows_processed`: Progress indicator

### 2. Use Dry-Run Mode

Validate your data before committing:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "dry_run=true" \
  -F "file=@devices.parquet" \
  "$NETBOX_URL/api/plugins/turbobulk/load/"
```

Dry-run performs all validation including:
- Schema validation
- FK constraint checking
- Unique constraint checking
- NOT NULL validation

### 3. Validate Parquet File Locally

Check your Parquet file structure before uploading:

```python
import pyarrow.parquet as pq

# Read and inspect
table = pq.read_table('devices.parquet')
print("Schema:", table.schema)
print("Rows:", table.num_rows)
print("Columns:", table.column_names)

# Check for null values in required fields
for col in table.column_names:
    nulls = table.column(col).null_count
    if nulls > 0:
        print(f"WARNING: {col} has {nulls} null values")
```

### 4. Verify API Connectivity

```bash
# Test API access
curl -H "Authorization: Bearer $TOKEN" \
  "$NETBOX_URL/api/plugins/turbobulk/models/" | head -5
```

## Performance Issues

### Slow Imports

| Symptom | Cause | Solution |
|---------|-------|----------|
| Import slower than expected | Changelogs enabled | Set `create_changelogs=false` for bulk imports |
| Very large files | Processing overhead | Split into files of 100K-500K rows |
| Timeouts | Operation takes too long | Split data into smaller batches |

**Optimizing for speed:**
```bash
# Disable changelogs for large initial imports
curl -X POST ... -F "create_changelogs=false" -F "file=@devices.parquet" ...
```

### Export Cache Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Always getting fresh exports | Data changed since last export | This is expected behavior |
| Want to force fresh export | Using cached data | Use `force_refresh=true` |
| Want to check if cache valid | Client-side optimization | Use `check_cache_only=true` |

## Understanding Error Messages

TurboBulk provides detailed, user-friendly error messages:

```json
{
  "data": {
    "error": {
      "error_type": "foreign_key",
      "message": "Foreign key violation: site_id=999 references a dcim_site that does not exist",
      "column": "site_id",
      "value": "999",
      "suggestion": "Ensure that the referenced site exists before inserting"
    }
  }
}
```

**Error types:**

| Type | Meaning |
|------|---------|
| `foreign_key` | Referenced object doesn't exist |
| `unique` | Duplicate key in your data |
| `not_null` | Required field is missing |
| `check` | Value fails a constraint (e.g., invalid status) |
| `data_type` | Wrong data type for field |
| `string_too_long` | String exceeds field's max_length |

## Transaction Safety

TurboBulk operations are **atomic** - if any part fails, the entire operation rolls back:

- No partial data is committed
- Your NetBox data remains in a consistent state
- You can safely retry after fixing the issue

This means you don't need to worry about cleanup after failures.

## Getting Help

1. **Check job status** - The error details often explain the issue
2. **Use dry-run** - Validate before committing
3. **Check permissions** - Verify user has required permissions
4. **Verify FK values** - Ensure referenced objects exist

### Contact Support

For issues you can't resolve:

- **NetBox Cloud/Enterprise customers:** Contact NetBox Labs support
- Include in your request:
  - Job ID
  - Error message
  - Parquet file schema (not the data)
  - Steps to reproduce
