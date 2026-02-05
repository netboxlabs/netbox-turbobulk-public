# TurboBulk API Examples

High-performance bulk data operations for NetBox.

These examples demonstrate how to use the TurboBulk API for bulk insert, upsert, delete, and export operations. They progress from simple to complex, culminating in a full GPU datacenter cabling workflow.

## Prerequisites

- **NetBox 4.4.7+** with TurboBulk plugin installed
- **Python 3.10+**
- **API token** with appropriate permissions

## Quick Start

```bash
# Install dependencies
cd examples
pip install -r requirements.txt

# Configure connection (option 1: environment variables)
export NETBOX_URL="http://your-netbox:8080"
export NETBOX_TOKEN="your-api-token"

# Or pass as arguments (option 2)
python 01_hello_turbobulk.py --url http://your-netbox:8080 --token your-token

# Run first example
python 01_hello_turbobulk.py
```

## Verify Installation

Before running examples, verify TurboBulk is properly installed:

```bash
python verify.py
```

This checks API connectivity, schema introspection, and dry-run validation.
Use `--test-data` to also test actual insert/delete operations.

> **Note:** TurboBulk runs pre-validation for IP addresses and prefixes in `auto` mode (the default).
> Use `dry_run=true` combined with `validation_mode=full` to catch all potential issues before committing.

## Example Progression

| Example | Description | Objects | Key Concepts |
|---------|-------------|---------|--------------|
| `01_hello_turbobulk.py` | Basic site insert | 10 sites | API connection, job polling |
| `02_device_inventory.py` | Device bulk insert | 1,000 devices | FK resolution, tags, custom fields |
| `03_export_transform.py` | ETL workflow | 1,000 sites | Export→transform→upsert |
| `04_interface_bulk.py` | Interface creation | 4,000 interfaces | Dependency order |
| `05_cable_connections.py` | Cable creation | 50 cables | CableTermination, ContentType |
| `06_gpu_datacenter_cabling.py` | Full datacenter | ~35K-200K cables | Iterative design workflow |
| `07_post_hooks.py` | Post-operation hooks | Demo | Hook configuration, best practices |
| `09_cached_exports.py` | Export caching | Demo | Cache hit/miss, force refresh, client caching |
| `10_validation_best_practices.py` | Validation patterns | Demo | dry_run, validation_mode, error handling |
| `11_event_streams.py` | Event dispatch | Demo | Event configuration, dispatch_events parameter |
| `12_format_comparison.py` | Format comparison | Demo | JSONL vs Parquet side-by-side comparison |

## Example Descriptions

### 01 - Hello TurboBulk
The simplest possible example. Creates 10 sites to verify your connection works.

```bash
python 01_hello_turbobulk.py --prefix test --count 10
```

### 02 - Device Inventory
Bulk insert devices with proper FK resolution. Demonstrates:
- Fetching reference data (sites, device types, roles)
- Building name→PK lookup maps
- Generating devices with random attributes
- Using tags and custom fields

```bash
python 02_device_inventory.py --prefix inv --count 1000
```

### 03 - Export Transform Reimport
Classic ETL workflow:
1. Export existing sites to Parquet
2. Transform data (add custom fields, update descriptions)
3. Upsert back to NetBox

```bash
python 03_export_transform.py --filter-prefix test
```

### 04 - Interface Bulk Operations
Create interfaces for existing devices. Shows dependency loading order (devices must exist before interfaces).

```bash
python 04_interface_bulk.py --device-prefix inv --interfaces-per-device 8
```

### 05 - Cable Connections
Create cables between interfaces. Demonstrates the two-phase cable loading:
1. Load Cable records, export to get IDs
2. Load CableTermination records with cable FKs

```bash
python 05_cable_connections.py --device-prefix inv --max-cables 50
```

### 06 - GPU Datacenter Cabling
Full iterative cabling design workflow for AI/ML datacenters:

```bash
# Set up infrastructure (site, device types, roles)
python 06_gpu_datacenter_cabling.py setup

# Create devices and interfaces
python 06_gpu_datacenter_cabling.py devices

# Push initial cabling design
python 06_gpu_datacenter_cabling.py push

# Check status
python 06_gpu_datacenter_cabling.py status

# Delete cables for redesign
python 06_gpu_datacenter_cabling.py delete

# Push modified design
python 06_gpu_datacenter_cabling.py push

# Cleanup when done
python 06_gpu_datacenter_cabling.py teardown
```

Topology scaling options:
```bash
# Small topology (~4K cables)
python 06_gpu_datacenter_cabling.py push --pods 2

# Default topology (~35K cables)
python 06_gpu_datacenter_cabling.py push --pods 8

# Large topology (~175K cables)
python 06_gpu_datacenter_cabling.py push --pods 40
```

## Common Patterns

### 1. FK Resolution
TurboBulk requires integer PKs for foreign keys. **Important:** Use the database column name with `_id` suffix (e.g., `site_id`, `device_type_id`, `role_id`), not the Django field name (e.g., `site`, `device_type`, `role`).

```python
# 1. Fetch reference data
sites = client.rest_get_all('/api/dcim/sites/')

# 2. Build lookup map
site_name_to_id = {s['name']: s['id'] for s in sites}

# 3. Use in your data (FK columns use DB column name with _id suffix)
device_data = {
    'name': ['device-1'],
    'site_id': [site_name_to_id['NYC-DC1']],  # FK uses _id suffix
    'device_type_id': [device_type_id],        # FK uses _id suffix
    'role_id': [role_id],                      # FK uses _id suffix
}
```

Common FK column names:
- `site_id`, `device_id`, `device_type_id`, `role_id`, `rack_id`
- `manufacturer_id`, `platform_id`, `tenant_id`, `location_id`
- `cable_id`, `termination_type_id` (for CableTermination)

### 2. Dependency Order
Load objects in FK dependency order:
1. Sites, Manufacturers, Tags
2. Device Types, Device Roles
3. Racks (depends on Site)
4. Devices (depends on Site, DeviceType, Role, Rack)
5. Interfaces (depends on Device)
6. Cables, CableTerminations (depends on Interfaces)

### 3. Job Status Polling
The client handles this automatically, but you can also poll manually:

```python
result = client.load('dcim.device', path, wait=False)
job_id = result['job_id']

# Check status
status = client.get_job_status(job_id)
print(status['status'])  # 'pending', 'completed', 'errored'
```

### 4. Upsert (Insert or Update)
Use upsert mode to update existing records:

```python
result = client.load(
    'dcim.device',
    parquet_path,
    mode='upsert',
    conflict_fields=['name', 'site'],  # Match on these fields
)
```

### 5. Bulk Delete
Delete by exporting IDs first:

```python
# Export IDs to delete
export_path = client.export(
    'dcim.device',
    filters={'name__startswith': 'old-'},
    fields=['id'],
)

# Create delete parquet
ids = read_parquet(export_path)['id']
create_pk_parquet(ids, delete_path)

# Delete
client.delete('dcim.device', delete_path)
```

### 07 - Post-Operation Hooks
Demonstrates configuring post-operation hooks for data consistency:

```bash
python 07_post_hooks.py

# Cleanup test data
python 07_post_hooks.py --cleanup
```

Shows:
- Using hooks with load operations
- Disabling hooks for performance
- Selective hook usage per operation type
- Best practices for large imports

### 09 - Cached Exports
Demonstrates TurboBulk's export caching feature for efficient repeated exports:

```bash
python 09_cached_exports.py
```

Shows:
- Cache HIT vs MISS behavior
- `force_refresh=true` to bypass cache
- `check_cache_only=true` to verify cache status
- `client_cache_key` for 304 Not Modified responses
- Client-side caching workflow for sync jobs

### 10 - Validation Best Practices
Demonstrates TurboBulk's validation system for safe bulk operations:

```bash
python 10_validation_best_practices.py
```

Shows:
- Using `dry_run=true` for pre-flight validation
- `validation_mode` parameter (`auto`, `full`, `none`)
- Handling pre-validation errors
- Best practices for IP addresses, prefixes, and cables

## Utilities

### TurboBulkClient

The client is available in two ways:

**Option 1: Standalone package (recommended for scripts/pipelines)**
```bash
pip install turbobulk-client
```
```python
from turbobulk_client import TurboBulkClient
client = TurboBulkClient()
```

**Option 2: From examples (for development/testing)**
```python
from turbobulk_client import TurboBulkClient
client = TurboBulkClient()
```

Features:
- Connection from args or environment
- Automatic job polling with progress
- `load()`, `delete()`, `export()` methods
- `validate()` for dry-run validation
- `get_template()` for schema discovery
- REST API helpers (`rest_get()`, `rest_get_all()`)
- ContentType lookup for GenericFK fields

### Data File Utilities (`common/parquet_utils.py`)

**JSONL Functions (Recommended):**
- `create_jsonl(rows, path, compress=True)` - Create from list of dicts
- `read_jsonl(path)` - Read to list of dicts
- `create_pk_jsonl(ids, path)` - Create ID-only file for deletes
- `create_jsonl_streaming(iterator, path)` - Stream rows to file

**Parquet Functions (High Performance):**
- `create_parquet(data, path)` - Create from column dict
- `read_parquet(path)` - Read to column dict
- `create_pk_parquet(ids, path)` - Create ID-only file for deletes
- Schema helpers for common NetBox types

### GPU Topology Generator (`common/topology.py`)
- `GPUDatacenterTopology` dataclass for spine-leaf configs
- Device, interface, and cable generation
- Scales from ~5K to ~200K+ cables

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| "No sites found" | Missing prerequisite data | Create sites first |
| "FK constraint violation" | Referenced object doesn't exist | Load in dependency order |
| "FK value=0" | Wrong column name for FK | Use `_id` suffix (e.g., `site_id` not `site`) |
| "Unique constraint violation" | Duplicate key | Use upsert mode or change prefix |
| "Permission denied" | Missing permissions | Grant add/change/delete on model |
| "Connection refused" | NetBox not running | Check NETBOX_URL |

### 11 - Event Streams Integration
Demonstrates TurboBulk's event streams integration for webhooks and event rules:

```bash
python 11_event_streams.py --prefix evt

# Cleanup test data
python 11_event_streams.py --prefix evt --cleanup
```

Shows:
- Event dispatch enabled by default
- Using `dispatch_events=False` for initial data loads
- Configuration options (`dispatch_events`, `events_chunk_size`)
- Recommended workflow for large imports

### 12 - Format Comparison
Compares JSONL and Parquet formats side-by-side:

```bash
python 12_format_comparison.py --count 1000 --prefix fmt

# Cleanup test data
python 12_format_comparison.py --prefix fmt --cleanup
```

Shows:
- Creating data in both formats
- File size comparison
- Load performance comparison
- When to use each format

## Performance Tips

1. **Batch size**: TurboBulk processes in 10K row chunks by default
2. **File format**: JSONL for easy integration, Parquet for maximum throughput
3. **Compression**: JSONL is auto-compressed with gzip
4. **Indexes**: Large imports may be faster with indexes disabled
5. **Post-hooks**: Disable if not needed (search reindex, denormalization)
6. **Event dispatch**: Disable with `dispatch_events=False` for initial loads (10k+ objects)

Expected throughput:
- Insert: 8,000-15,000 rows/sec
- Upsert: 6,000-12,000 rows/sec
- Delete: 4,000-8,000 rows/sec
- Export: 15,000-25,000 rows/sec

