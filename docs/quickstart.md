# TurboBulk Quick Start Guide

Get started with TurboBulk in 5 minutes.

## Prerequisites

- [ ] **NetBox Cloud or NetBox Enterprise** instance with TurboBulk enabled
- [ ] **API token** with appropriate permissions (add/change/delete on target models)
- [ ] **Python 3.10+** with `pip` (for client library and examples)

## Step 1: Get Your API Token

1. Log in to your NetBox instance
2. Navigate to **Admin > API Tokens** (or click your username → API Tokens)
3. Create a new token with appropriate permissions for the models you'll be working with
4. Copy the token value (v2 tokens start with `nbt_`)

## Step 2: Set Up Your Environment

```bash
# Set environment variables
export NETBOX_URL="https://your-instance-name.cloud.netboxapp.com"
export NETBOX_TOKEN="nbt_your-api-token"

# Install the TurboBulk client
pip install turbobulk-client
```

## Step 3: Verify API Access

Test that TurboBulk is accessible:

```bash
curl -H "Authorization: Bearer $NETBOX_TOKEN" \
  "$NETBOX_URL/api/plugins/turbobulk/models/" | head -20
```

You should see a list of available models with their schemas.

## Step 4: Run Your First Example

### Using the Python Client

```python
from turbobulk_client import TurboBulkClient
import gzip
import json

# Initialize client (uses NETBOX_URL and NETBOX_TOKEN env vars)
client = TurboBulkClient()

# Create test data - 10 sites
sites = [
    {'name': f'tb-site-{i}', 'slug': f'tb-site-{i}', 'status': 'active'}
    for i in range(10)
]

# Write JSONL file
with gzip.open('sites.jsonl.gz', 'wt', encoding='utf-8') as f:
    for site in sites:
        f.write(json.dumps(site) + '\n')

# Submit bulk load
result = client.load('dcim.site', 'sites.jsonl.gz')
print(f"Rows inserted: {result['data']['rows_inserted']}")
```

Save this as `hello_turbobulk.py` and run:

```bash
python hello_turbobulk.py
```

Expected output:
```
Rows inserted: 10
```

## Common FK Column Naming Issue

> **IMPORTANT**: Foreign key columns must use the `_id` suffix.
>
> ```python
> # CORRECT:
> data = {'site_id': 1, 'device_type_id': 1}
>
> # WRONG (causes "FK value=0" errors):
> data = {'site': 1, 'device_type': 1}
> ```

## Next Steps

1. **Explore the examples** - See the [examples directory](../examples/) for progressive tutorials:
   - Basic site insert
   - Devices with FK resolution
   - ETL workflow (export → transform → load)
   - Bulk interface creation
   - Cable connections

2. **Read the documentation**:
   - [User Guide](user-guide.md) - When to use, what gets bypassed
   - [API Reference](api-reference.md) - Complete endpoint documentation
   - [Python Client](client.md) - Full client library reference

3. **Understand the tradeoffs**:
   - TurboBulk dispatches webhooks and event rules asynchronously
   - Use for bulk operations (>1,000 objects)
   - Use REST API for interactive changes

4. **Leverage export caching**:
   - Repeated exports return cached files if data hasn't changed
   - Use `force_refresh=True` to bypass cache
   - Use `check_cache_only=True` to verify cache status without creating jobs

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection refused` | Check NETBOX_URL is correct |
| `401 Unauthorized` | Check NETBOX_TOKEN is valid |
| `404 Not Found` on /turbobulk/ | TurboBulk may not be enabled - contact support |
| `FK value=0` errors | Use `_id` suffix for FK columns (e.g., `site_id`) |
| Jobs stuck in `pending` | Contact support |
| `Permission denied` | Request add/change/delete permissions from your administrator |

## Getting Help

- [Troubleshooting Guide](troubleshooting.md) - Detailed error resolution
- [Examples](../examples/) - Working code examples
- **NetBox Labs Support** - Contact support for platform issues
