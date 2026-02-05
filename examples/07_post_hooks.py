#!/usr/bin/env python3
"""
Example 07: Post-Operation Hooks

This example demonstrates how to use TurboBulk's post-operation hooks
to maintain data consistency after bulk operations.

Post-hooks handle tasks that Django signals normally perform:
- Fixing denormalized fields (e.g., component site assignments)
- Updating counter caches (e.g., interface counts)
- Rebuilding search indexes
- Tracing cable paths

Run: python 07_post_hooks.py [--cleanup]
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient


def create_test_site(client: TurboBulkClient, prefix: str) -> dict:
    """Create a test site for the example."""
    schema = pa.schema([
        ('name', pa.string()),
        ('slug', pa.string()),
        ('status', pa.string()),
    ])
    data = {
        'name': [f'{prefix}-hooks-demo'],
        'slug': [f'{prefix}-hooks-demo'],
        'status': ['active'],
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        result = client.load('dcim.site', path, verbose=False)
        return result
    finally:
        path.unlink(missing_ok=True)


def get_site_id(client: TurboBulkClient, name: str) -> int:
    """Get site ID by name."""
    sites = client.rest_get('/api/dcim/sites/', {'name': name})
    results = sites.get('results', [])
    if results:
        return results[0]['id']
    raise ValueError(f"Site not found: {name}")


def example_with_hooks(client: TurboBulkClient, site_id: int, prefix: str):
    """
    Demonstrate bulk load with post-hooks enabled.

    This is the recommended approach - hooks ensure data consistency.
    """
    print("\n--- Example: Bulk Load WITH Hooks (Recommended) ---")

    # Get a device type and role
    device_types = client.rest_get('/api/dcim/device-types/', {'limit': 1})
    if not device_types.get('results'):
        print("  [SKIP] No device types found - cannot create devices")
        return None
    device_type_id = device_types['results'][0]['id']

    roles = client.rest_get('/api/dcim/device-roles/', {'limit': 1})
    if not roles.get('results'):
        print("  [SKIP] No device roles found - cannot create devices")
        return None
    role_id = roles['results'][0]['id']

    # Create devices with all hooks enabled (default)
    schema = pa.schema([
        ('name', pa.string()),
        ('site_id', pa.int64()),
        ('device_type_id', pa.int64()),
        ('role_id', pa.int64()),
        ('status', pa.string()),
    ])

    devices = [f'{prefix}-device-{i}' for i in range(1, 6)]
    data = {
        'name': devices,
        'site_id': [site_id] * 5,
        'device_type_id': [device_type_id] * 5,
        'role_id': [role_id] * 5,
        'status': ['active'] * 5,
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print("  Creating 5 devices with ALL hooks enabled...")
        result = client.load(
            'dcim.device', path,
            # All hooks enabled by default, but being explicit:
            post_hooks={
                'fix_denormalized': True,
                'rebuild_search_index': True,
                'fix_counters': True,
            },
            verbose=False
        )

        # Check the hook results
        job_data = result.get('data', {})
        hooks = job_data.get('post_hooks', {})

        print(f"  Inserted: {job_data.get('rows_inserted', 'N/A')} rows")
        print(f"  Hook results:")
        for hook_name, hook_result in hooks.items():
            status = "OK" if hook_result.get('success') else "FAILED"
            print(f"    - {hook_name}: {status}")

        return devices

    finally:
        path.unlink(missing_ok=True)


def example_without_hooks(client: TurboBulkClient, site_id: int, prefix: str):
    """
    Demonstrate bulk load with hooks disabled.

    Use this for performance during large imports, but be aware that
    you'll need to manually handle consistency afterward.
    """
    print("\n--- Example: Bulk Load WITHOUT Hooks (Performance Mode) ---")
    print("  Use this for large imports where you'll rebuild indexes afterward.")

    # Create a simple test - just sites (no FK dependencies to worry about)
    schema = pa.schema([
        ('name', pa.string()),
        ('slug', pa.string()),
        ('status', pa.string()),
    ])

    sites = [f'{prefix}-perf-site-{i}' for i in range(1, 11)]
    slugs = [s.lower() for s in sites]
    data = {
        'name': sites,
        'slug': slugs,
        'status': ['active'] * 10,
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print("  Creating 10 sites with hooks DISABLED...")
        start = time.time()

        result = client.load(
            'dcim.site', path,
            post_hooks={
                'fix_denormalized': False,  # Skip for sites (not needed anyway)
                'rebuild_search_index': False,  # Will rebuild manually after
                'fix_counters': False,  # Skip counter updates
            },
            verbose=False
        )

        elapsed = time.time() - start
        job_data = result.get('data', {})

        print(f"  Inserted: {job_data.get('rows_inserted', 'N/A')} rows in {elapsed:.2f}s")
        print("  Note: Search index NOT updated - objects won't appear in search")
        print("  Run './manage.py reindex --lazy' to rebuild search index")

        return sites

    finally:
        path.unlink(missing_ok=True)


def example_selective_hooks(client: TurboBulkClient, prefix: str):
    """
    Demonstrate selective hook usage based on the operation type.
    """
    print("\n--- Example: Selective Hooks (Best Practice) ---")
    print("  Enable only the hooks relevant to your operation.")

    print("\n  Scenario 1: Importing sites (no FK relationships)")
    print("  -> Only rebuild_search_index is useful")
    print("  -> fix_denormalized and fix_counters don't apply to sites")
    print("""
    client.load('dcim.site', 'sites.parquet', post_hooks={
        'rebuild_search_index': True,
        'fix_denormalized': False,  # Sites have no denormalized fields
        'fix_counters': False,      # No counter caches on sites
    })
    """)

    print("  Scenario 2: Importing devices (with components)")
    print("  -> fix_denormalized needed to update component site assignments")
    print("  -> fix_counters updates interface/module counts")
    print("""
    client.load('dcim.device', 'devices.parquet', post_hooks={
        'rebuild_search_index': True,
        'fix_denormalized': True,   # Important for components
        'fix_counters': True,       # Update interface counts
    })
    """)

    print("  Scenario 3: Importing cables")
    print("  -> rebuild_cable_paths is critical for connectivity")
    print("""
    client.load('dcim.cable', 'cables.parquet', post_hooks={
        'rebuild_search_index': True,
        'rebuild_cable_paths': True,  # Essential for cable tracing
        'fix_denormalized': False,
        'fix_counters': False,
    })
    """)

    print("  Scenario 4: Large multi-stage import")
    print("  -> Disable search index during import")
    print("  -> Rebuild once at the end")
    print("""
    # Stage 1: Import sites
    client.load('dcim.site', 'sites.parquet',
                post_hooks={'rebuild_search_index': False})

    # Stage 2: Import devices
    client.load('dcim.device', 'devices.parquet',
                post_hooks={'rebuild_search_index': False, 'fix_denormalized': True})

    # Stage 3: Import interfaces
    client.load('dcim.interface', 'interfaces.parquet',
                post_hooks={'rebuild_search_index': False, 'fix_counters': True})

    # Final: Rebuild search index once
    # Run: ./manage.py reindex --lazy
    """)


def cleanup(client: TurboBulkClient, prefix: str):
    """Clean up test data."""
    print("\n--- Cleanup ---")

    # Find all test sites
    sites = client.rest_get('/api/dcim/sites/', {'name__startswith': prefix})
    site_ids = [s['id'] for s in sites.get('results', [])]

    # Find all test devices
    devices = client.rest_get('/api/dcim/devices/', {'name__startswith': prefix})
    device_ids = [d['id'] for d in devices.get('results', [])]

    # Delete devices first (they reference sites)
    if device_ids:
        schema = pa.schema([('id', pa.int64())])
        table = pa.table({'id': device_ids}, schema=schema)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            path = Path(f.name)

        try:
            result = client.delete('dcim.device', path, verbose=False)
            print(f"  Deleted {result.get('data', {}).get('rows_deleted', 0)} devices")
        finally:
            path.unlink(missing_ok=True)

    # Delete sites
    if site_ids:
        schema = pa.schema([('id', pa.int64())])
        table = pa.table({'id': site_ids}, schema=schema)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            path = Path(f.name)

        try:
            result = client.delete('dcim.site', path, verbose=False)
            print(f"  Deleted {result.get('data', {}).get('rows_deleted', 0)} sites")
        finally:
            path.unlink(missing_ok=True)

    if not site_ids and not device_ids:
        print("  No test data to clean up")


def main():
    parser = argparse.ArgumentParser(description='Post-hooks demonstration')
    parser.add_argument('--cleanup', action='store_true', help='Clean up test data and exit')
    parser.add_argument('--prefix', default='tb-hooks', help='Prefix for test objects')
    args = parser.parse_args()

    client = TurboBulkClient()
    print(f"Connected to: {client.base_url}")

    if args.cleanup:
        cleanup(client, args.prefix)
        return

    print("\nTurboBulk Post-Operation Hooks Demo")
    print("=" * 50)

    # Create a test site first
    print("\nCreating test site...")
    create_test_site(client, args.prefix)

    try:
        site_id = get_site_id(client, f'{args.prefix}-hooks-demo')
        print(f"  Created site ID: {site_id}")
    except ValueError as e:
        print(f"  Failed to create site: {e}")
        return

    # Run examples
    example_with_hooks(client, site_id, args.prefix)
    example_without_hooks(client, site_id, args.prefix)
    example_selective_hooks(client, args.prefix)

    print("\n" + "=" * 50)
    print("Demo complete!")
    print(f"\nTo clean up test data, run: python {Path(__file__).name} --cleanup")


if __name__ == '__main__':
    main()
