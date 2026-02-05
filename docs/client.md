# TurboBulk Client

Python client library for the TurboBulk NetBox plugin.

TurboBulk provides high-performance bulk data operations for NetBox. Both JSONL and Parquet formats are supported—JSONL is easier to create (any language), while Parquet offers faster throughput for large datasets.

## Installation

```bash
# Basic installation (JSONL support included)
pip install turbobulk-client

# With Parquet support (for maximum performance)
pip install turbobulk-client[parquet]
```

## Quick Start

```python
from turbobulk_client import TurboBulkClient
import gzip
import json

# Initialize client (uses NETBOX_URL and NETBOX_TOKEN env vars)
client = TurboBulkClient()

# Or with explicit credentials
client = TurboBulkClient(
    base_url='https://your-instance.cloud.netboxapp.com',
    token='nbt_your-api-token'
)

# Validate data before loading
result = client.validate('dcim.site', 'sites.jsonl.gz')
if result.get('data', {}).get('valid'):
    print("Validation passed!")
else:
    print("Errors:", result.get('data', {}).get('errors'))

# Bulk insert (JSONL is the default format)
result = client.load('dcim.site', 'sites.jsonl.gz')
print(f"Inserted {result['data']['rows_inserted']} rows")

# Bulk upsert (insert or update on conflict)
result = client.load('dcim.device', 'devices.jsonl.gz', mode='upsert')

# Bulk delete
result = client.delete('dcim.site', 'site_ids.jsonl.gz')

# Bulk export (returns JSONL by default)
result = client.export('dcim.device', filters={'site_id': 1})
```

## Features

- **Massive Performance**: Tens of thousands of objects/sec vs hundreds with REST API
- **Validation Modes**: Choose between speed (`none`), balanced (`auto`), or thoroughness (`full`)
- **Template Generation**: Get required fields for any model
- **Async Jobs**: Operations run as background jobs with status polling
- **Event Dispatch**: Webhooks and event rules triggered asynchronously (configurable)
- **Changelog Support**: Optional ObjectChange audit trail for bulk operations
- **Full NetBox Support**: Sites, devices, interfaces, cables, IPs, and more

## API Reference

### TurboBulkClient

```python
client = TurboBulkClient(
    base_url=None,      # NetBox URL (or NETBOX_URL env var)
    token=None,         # API token (or NETBOX_TOKEN env var)
    verify_ssl=True,    # Verify SSL certificates
)
```

### Methods

#### get_template(model, include_optional=False)

Get a template dict with required fields for a model:

```python
template = client.get_template('dcim.site')
# {'name': '', 'slug': '', 'status': 'active', ...}
```

#### validate(model, data_path, mode='insert', ...)

Validate a data file without committing. Accepts JSONL (.jsonl, .jsonl.gz) or Parquet (.parquet):

```python
result = client.validate('dcim.site', 'sites.jsonl.gz')
# Returns: {'data': {'valid': True, 'rows': 100, 'errors': [], 'warnings': []}}
```

#### load(model, data_path, mode='insert', ...)

Submit a bulk insert/upsert job. Accepts JSONL or Parquet files (format auto-detected):

```python
result = client.load(
    'dcim.device',
    'devices.jsonl.gz',         # or devices.parquet
    mode='upsert',              # 'insert' or 'upsert'
    conflict_fields=['name'],   # For upsert conflict detection
    validation_mode='auto',     # 'none', 'auto', or 'full'
    post_hooks={                # Post-operation hooks
        'fix_denormalized': True,
        'rebuild_search_index': True,
    },
    create_changelogs=True,     # Generate ObjectChange records
    dispatch_events=None,       # True/False/None (None=use global config)
)
```

#### delete(model, data_path, ...)

Submit a bulk delete job. Accepts JSONL or Parquet files with an 'id' column:

```python
result = client.delete(
    'dcim.site',
    'site_ids.jsonl.gz',        # Must have 'id' column
    cascade_nullable_fks=True,  # Clear nullable FK references
    dispatch_events=None,       # True/False/None (None=use global config)
)
```

#### export(model, filters=None, fields=None, format='jsonl', ...)

Export data to JSONL (default) or Parquet:

```python
# Export as JSONL (default, gzipped)
result = client.export(
    'dcim.device',
    filters={'site_id': 1, 'status': 'active'},
    fields=['id', 'name', 'site_id', 'status'],
)

# Export as Parquet for maximum performance
result = client.export(
    'dcim.device',
    filters={'site_id': 1},
    format='parquet',
)
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NETBOX_URL` | NetBox base URL (e.g., `https://demo.cloud.netboxapp.com`) |
| `NETBOX_TOKEN` | API authentication token (v2 format: `nbt_...`) |

## Creating Data Files

### JSONL (Default - Recommended)

JSONL (JSON Lines) is the easiest format—one JSON object per line:

```python
import gzip
import json

# Row-oriented data
sites = [
    {'name': 'site-1', 'slug': 'site-1', 'status': 'active'},
    {'name': 'site-2', 'slug': 'site-2', 'status': 'active'},
    {'name': 'site-3', 'slug': 'site-3', 'status': 'active'},
]

# Write gzipped JSONL (recommended)
with gzip.open('sites.jsonl.gz', 'wt', encoding='utf-8') as f:
    for site in sites:
        f.write(json.dumps(site) + '\n')
```

### Parquet (High Performance)

For maximum throughput with large datasets (100K+ rows):

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Column-oriented data - FK columns must use _id suffix
data = {
    'name': ['site-1', 'site-2', 'site-3'],
    'slug': ['site-1', 'site-2', 'site-3'],
    'status': ['active', 'active', 'active'],
}

# Write Parquet file
table = pa.table(data)
pq.write_table(table, 'sites.parquet')
```

## Error Handling

```python
from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError

try:
    result = client.load('dcim.site', 'sites.jsonl.gz')
except JobFailedError as e:
    print(f"Job failed: {e}")
    print(f"Error details: {e.job_result}")
except TurboBulkError as e:
    print(f"Client error: {e}")
```

## Export Caching

Repeated exports of unchanged data return cached files immediately:

```python
# First export - creates job, waits for completion
result1 = client.export('dcim.device')

# Second export - returns cached file if data unchanged
result2 = client.export('dcim.device')

# Force fresh export
result3 = client.export('dcim.device', force_refresh=True)

# Check cache status without downloading
result4 = client.export('dcim.device', check_cache_only=True)
```

## Branching Support

Load data into a NetBox branch for review before merging:

```python
result = client.load(
    'dcim.device',
    'devices.jsonl.gz',
    branch='my-feature-branch'
)
```
