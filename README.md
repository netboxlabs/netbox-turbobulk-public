# TurboBulk Client

Python client library for the TurboBulk NetBox plugin—high-performance bulk data operations for NetBox.

**Requires:** NetBox Cloud or NetBox Enterprise with TurboBulk enabled.

## Features

- **Massive Performance Gains**: Tens of thousands of objects/sec vs hundreds with REST API
- **Bulk Operations**: Insert, upsert, delete, and export millions of rows
- **Multiple Formats**: JSONL (default, easy) or Parquet (fastest)
- **Validation Modes**: Choose between speed (`none`), balanced (`auto`), or thoroughness (`full`)
- **Export Caching**: Repeated exports return cached files if data unchanged
- **Changelog Support**: Optional ObjectChange audit trail for bulk operations
- **Async Event Dispatch**: Webhooks and event rules triggered asynchronously after operations
- **Full NetBox Support**: All core models—sites, devices, interfaces, cables, IPs, and more
- **Branching Support**: Works with NetBox Branching for change management

## Installation

```bash
# Basic installation (JSONL support included)
pip install turbobulk-client

# With Parquet support (for maximum performance)
pip install turbobulk-client[parquet]
```

## Quick Start

### Environment Setup

```bash
export NETBOX_URL="https://your-instance-name.cloud.netboxapp.com"
export NETBOX_TOKEN="nbt_your-api-token"
```

### Bulk Insert Sites

```python
from turbobulk_client import TurboBulkClient
import gzip
import json

# Initialize client (uses NETBOX_URL and NETBOX_TOKEN env vars)
client = TurboBulkClient()

# Create site data
sites = [
    {'name': f'site-{i}', 'slug': f'site-{i}', 'status': 'active'}
    for i in range(1000)
]

# Write JSONL file
with gzip.open('sites.jsonl.gz', 'wt') as f:
    for site in sites:
        f.write(json.dumps(site) + '\n')

# Bulk insert
result = client.load('dcim.site', 'sites.jsonl.gz')
print(f"Inserted {result['data']['rows_inserted']} sites")
```

### Bulk Export Devices

```python
# Export all devices to JSONL
result = client.export('dcim.device')
print(f"Exported to: {result['path']}")

# Export with filters
result = client.export(
    'dcim.device',
    filters={'site_id': 1, 'status': 'active'},
    fields=['id', 'name', 'site_id', 'device_type_id'],
)
```

### Validate Before Loading

```python
# Validate data without committing changes
result = client.validate('dcim.site', 'sites.jsonl.gz')
if result['data']['valid']:
    print("Validation passed!")
    client.load('dcim.site', 'sites.jsonl.gz')
else:
    print("Errors:", result['data']['errors'])
```

## API Overview

| Method | Description |
|--------|-------------|
| `client.load(model, path)` | Bulk insert or upsert |
| `client.delete(model, path)` | Bulk delete by ID |
| `client.export(model)` | Export to JSONL/Parquet |
| `client.validate(model, path)` | Validate without committing |
| `client.get_template(model)` | Get required fields |
| `client.get_models()` | List available models |

## Data Formats

### JSONL (Default)

One JSON object per line. Easy to create in any language:

```python
import gzip, json

data = [{'name': 'site-1', 'slug': 'site-1', 'status': 'active'}]

with gzip.open('sites.jsonl.gz', 'wt') as f:
    for row in data:
        f.write(json.dumps(row) + '\n')
```

### Parquet (Fastest)

For maximum throughput with large datasets (100K+ rows):

```python
import pyarrow as pa
import pyarrow.parquet as pq

data = {
    'name': ['site-1', 'site-2'],
    'slug': ['site-1', 'site-2'],
    'status': ['active', 'active'],
}
pq.write_table(pa.table(data), 'sites.parquet')
```

## Validation Modes

Control the trade-off between speed and validation thoroughness:

| Mode | Speed | Coverage | Use When |
|------|-------|----------|----------|
| `none` | Fastest | Database constraints only | Trusted data, migrations |
| `auto` | Fast | DB + IP/prefix validation | Normal bulk operations (default) |
| `full` | Slower | Complete Django validation | Complex models, critical data |

```python
# Use full validation for complex models like cables
result = client.load('dcim.cable', 'cables.jsonl.gz', validation_mode='full')

# Skip validation for trusted migration data
result = client.load('dcim.site', 'sites.jsonl.gz', validation_mode='none')
```

## Foreign Key Columns

**Important**: FK columns must use the `_id` suffix:

```python
# Correct
{'site_id': 1, 'device_type_id': 5}

# Wrong (causes errors)
{'site': 1, 'device_type': 5}
```

## Error Handling

```python
from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError

try:
    result = client.load('dcim.site', 'sites.jsonl.gz')
except JobFailedError as e:
    print(f"Job failed: {e}")
    print(f"Details: {e.job_result}")
except TurboBulkError as e:
    print(f"Client error: {e}")
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NETBOX_URL` | NetBox base URL (e.g., `https://demo.cloud.netboxapp.com`) |
| `NETBOX_TOKEN` | API authentication token (v2 format: `nbt_...`) |

## Documentation

- **[Quick Start Guide](docs/quickstart.md)** - Get running in 5 minutes
- **[User Guide](docs/user-guide.md)** - When to use TurboBulk, operational details
- **[API Reference](docs/api-reference.md)** - Complete endpoint documentation
- **[Client Library](docs/client.md)** - Full Python client reference
- **[Branching Guide](docs/branching.md)** - NetBox Branching integration
- **[Examples](examples/)** - Progressive tutorials from hello-world to 200K cables
- **[Troubleshooting](docs/troubleshooting.md)** - Common errors and solutions
- **[Changelog](CHANGELOG.md)** - Version history

## Examples

The [examples directory](examples/) contains progressive tutorials:

| Example | Description |
|---------|-------------|
| `01_hello_turbobulk.py` | Basic site insert |
| `02_device_inventory.py` | Devices with FK resolution |
| `03_export_transform.py` | ETL workflow |
| `04_interface_bulk.py` | Bulk interface creation |
| `05_cable_connections.py` | Cable terminations |
| `06_gpu_datacenter_cabling.py` | Large-scale cabling (200K cables) |
| `07_post_hooks.py` | Post-operation hooks |
| `08_branching_workflow.py` | NetBox Branching integration |
| `09_cached_exports.py` | Export caching strategies |
| `10_validation_best_practices.py` | Validation patterns |
| `11_event_streams.py` | Server-sent events for job progress |
| `12_format_comparison.py` | JSONL vs Parquet performance |

## When to Use TurboBulk

| Use Case | TurboBulk | REST API |
|----------|-----------|----------|
| Initial data population (>1,000 objects) | Yes | No |
| Regular syncs from external systems | Yes | No |
| Data migration between instances | Yes | No |
| Bulk exports for analytics | Yes | No |
| Interactive single-object changes | No | Yes |
| Operations needing custom validation | No | Yes |

## Requirements

- NetBox Cloud or NetBox Enterprise with TurboBulk enabled
- Python 3.10+
- API token with appropriate permissions

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Support

For issues with the TurboBulk plugin or client library, contact NetBox Labs support.
