#!/usr/bin/env python3
"""
Example 10: Validation Best Practices

This example demonstrates TurboBulk's validation system for safe bulk operations.

Key concepts:
1. dry_run=True: Validate data without committing
2. validation_mode: auto (default), full, none
3. Pre-validation for IP addresses and prefixes
4. Handling validation errors

Validation modes:
- auto: Pre-validation for ipam.ipaddress and ipam.prefix (inet-based rules)
- full: Django full_clean() on each row (slower but catches all issues)
- none: Skip pre-validation (fastest, use for trusted data only)

Run: python 10_validation_best_practices.py [--cleanup]
"""

import argparse
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient


def example_dry_run_validation(client: TurboBulkClient, prefix: str):
    """
    Demonstrate using dry_run for pre-flight validation.

    dry_run=True validates all data without committing changes.
    This is the safest way to check data before loading.
    """
    print("\n--- Example: Dry-Run Validation ---")
    print("  Use dry_run=True to validate data before committing.")

    # Create valid site data
    schema = pa.schema([
        ('name', pa.string()),
        ('slug', pa.string()),
        ('status', pa.string()),
    ])
    data = {
        'name': [f'{prefix}-dryrun-site-1', f'{prefix}-dryrun-site-2'],
        'slug': [f'{prefix}-dryrun-site-1', f'{prefix}-dryrun-site-2'],
        'status': ['active', 'planned'],
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print("  Validating 2 sites with dry_run=True...")

        # Use client.validate() which is shorthand for load(..., dry_run=True)
        result = client.validate('dcim.site', path, verbose=False)

        job_status = result.get('status', 'unknown')
        data = result.get('data', {})
        rows = data.get('rows_processed', data.get('rows_validated', 0))
        is_valid = data.get('valid', True)
        errors = data.get('errors', [])

        validation_passed = job_status == 'completed' and is_valid and not errors

        if validation_passed:
            print(f"  Validation PASSED: {rows} rows validated")
            print("  Data is valid - safe to load without dry_run")
        else:
            print(f"  Validation FAILED")
            if errors:
                for err in errors:
                    print(f"    Error: {err.get('message', err)}")
            else:
                error = data.get('error', {})
                print(f"    Error: {error}")

        # Only load if validation passed
        if validation_passed:
            print("\n  Since validation passed, loading for real...")
            result = client.load('dcim.site', path, verbose=False)
            inserted = result.get('data', {}).get('rows_inserted', 0)
            print(f"  Loaded: {inserted} rows")
        else:
            print("\n  Skipping actual load since validation failed.")
            print("  In practice, fix your data and retry.")

    finally:
        path.unlink(missing_ok=True)


def example_validation_modes(client: TurboBulkClient, prefix: str):
    """
    Demonstrate the three validation modes.
    """
    print("\n--- Example: Validation Modes ---")

    print("\n  validation_mode='auto' (default):")
    print("    - Pre-validation runs for ipam.ipaddress and ipam.prefix")
    print("    - Other models rely on database constraints")
    print("    - Best balance of safety and performance")

    print("\n  validation_mode='full':")
    print("    - Django full_clean() on each row")
    print("    - Catches all validation issues")
    print("    - ~30-60x slower than 'auto'")
    print("    - Use for complex models: cables, devices with rack positions")

    print("\n  validation_mode='none':")
    print("    - Skip all pre-validation")
    print("    - Only database constraints apply")
    print("    - Maximum speed for trusted data")
    print("    - Use for migrations from trusted sources")

    # Demonstrate with sites
    schema = pa.schema([
        ('name', pa.string()),
        ('slug', pa.string()),
        ('status', pa.string()),
    ])
    data = {
        'name': [f'{prefix}-mode-test-1'],
        'slug': [f'{prefix}-mode-test-1'],
        'status': ['active'],
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print("\n  Loading site with validation_mode='auto'...")
        result = client.load('dcim.site', path, validation_mode='auto', verbose=False)
        pre_val = result.get('data', {}).get('pre_validation', {})
        if pre_val:
            print(f"    Pre-validation ran: {pre_val.get('rules_run', [])}")
        else:
            print("    Pre-validation skipped (no rules for dcim.site)")
        print(f"    Inserted: {result.get('data', {}).get('rows_inserted', 0)} rows")

    finally:
        path.unlink(missing_ok=True)


def example_handling_validation_errors(client: TurboBulkClient, prefix: str):
    """
    Demonstrate how to handle pre-validation errors.
    """
    print("\n--- Example: Handling Validation Errors ---")
    print("  Pre-validation provides user-friendly error messages.")

    # Get a site to use
    sites = client.rest_get('/api/dcim/sites/', {'limit': 1})
    if not sites.get('results'):
        print("  [SKIP] No sites found - cannot demonstrate device validation")
        return

    site_id = sites['results'][0]['id']

    # Get device type and role
    device_types = client.rest_get('/api/dcim/device-types/', {'limit': 1})
    if not device_types.get('results'):
        print("  [SKIP] No device types found")
        return
    device_type_id = device_types['results'][0]['id']

    roles = client.rest_get('/api/dcim/device-roles/', {'limit': 1})
    if not roles.get('results'):
        print("  [SKIP] No device roles found")
        return
    role_id = roles['results'][0]['id']

    # Create device with invalid data (site_id that doesn't exist)
    schema = pa.schema([
        ('name', pa.string()),
        ('site_id', pa.int64()),
        ('device_type_id', pa.int64()),
        ('role_id', pa.int64()),
        ('status', pa.string()),
    ])
    data = {
        'name': [f'{prefix}-invalid-device'],
        'site_id': [999999],  # Invalid site ID
        'device_type_id': [device_type_id],
        'role_id': [role_id],
        'status': ['active'],
    }
    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print("  Attempting to load device with invalid site_id=999999...")

        result = client.validate('dcim.device', path, verbose=False)
        status = result.get('status', 'unknown')
        data_result = result.get('data', {})

        if status == 'errored' or data_result.get('error'):
            error = data_result.get('error', {})
            print(f"  Validation FAILED (expected):")
            print(f"    Error type: {error.get('error_type', 'unknown')}")
            print(f"    Message: {error.get('message', 'N/A')}")
            print(f"    Suggestion: {error.get('suggestion', 'N/A')}")
        else:
            print(f"  Result: {result}")

    finally:
        path.unlink(missing_ok=True)


def example_complex_models(client: TurboBulkClient, prefix: str):
    """
    Best practices for complex models (Cables, Devices with positions).
    """
    print("\n--- Example: Complex Models Best Practices ---")
    print("  Complex models (Cables, Devices with rack positions) need extra care.")

    print("\n  Recommendations for complex models:")
    print("  1. Always use dry_run=True first")
    print("  2. Use validation_mode='full' for complete Django validation")
    print("  3. Use REST API for operations under 1,000 objects")

    print("\n  Example workflow for IP Address loading:")
    print("""
    # Step 1: Validate with dry_run
    result = client.validate('ipam.ipaddress', 'ips.parquet')
    if result['data'].get('error'):
        print("Validation failed:", result['data']['error'])
        return

    # Step 2: Load - pre-validation catches network/broadcast and VRF issues
    result = client.load('ipam.ipaddress', 'ips.parquet')

    # Step 3: Check pre-validation results
    pre_val = result['data'].get('pre_validation', {})
    if pre_val.get('warnings'):
        print("Warnings:", pre_val['warnings'])
    """)

    print("\n  Example workflow for Cable loading:")
    print("""
    # Cables are complex - always validate first
    result = client.validate('dcim.cable', 'cables.parquet')
    if result['data'].get('error'):
        print("Validation failed:", result['data']['error'])
        return

    # For cables, use full validation to catch profile issues
    result = client.load(
        'dcim.cable',
        'cables.parquet',
        validation_mode='full',  # Complete Django validation
        post_hooks={'rebuild_cable_paths': True}
    )
    """)


def example_migration_workflow(client: TurboBulkClient, prefix: str):
    """
    Best practices for large migrations from trusted sources.
    """
    print("\n--- Example: Migration from Trusted Source ---")
    print("  For migrations from a trusted NetBox instance, you can skip validation.")

    print("\n  Workflow:")
    print("""
    # Step 1: Export from source NetBox
    source_client = TurboBulkClient(url='https://source-netbox.example.com', token='...')
    path = source_client.export('dcim.site')

    # Step 2: Load to destination with validation_mode='none'
    # (Data is trusted - it passed validation on source)
    dest_client = TurboBulkClient(url='https://dest-netbox.example.com', token='...')
    result = dest_client.load(
        'dcim.site',
        path,
        validation_mode='none',  # Skip validation for speed
        create_changelogs=False  # Optional: skip audit for migration
    )
    """)

    print("\n  When to use validation_mode='none':")
    print("    - Data exported from another NetBox")
    print("    - Data from a trusted ETL pipeline with its own validation")
    print("    - Re-loading data that was previously validated")
    print("    - Performance-critical batch jobs with pre-validated data")


def cleanup(client: TurboBulkClient, prefix: str):
    """Clean up test data."""
    print("\n--- Cleanup ---")

    # Find all test sites
    sites = client.rest_get('/api/dcim/sites/', {'name__startswith': prefix})
    site_ids = [s['id'] for s in sites.get('results', [])]

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
    else:
        print("  No test data to clean up")


def main():
    parser = argparse.ArgumentParser(description='Validation best practices demonstration')
    parser.add_argument('--cleanup', action='store_true', help='Clean up test data and exit')
    parser.add_argument('--prefix', default='tb-val', help='Prefix for test objects')
    args = parser.parse_args()

    client = TurboBulkClient()
    print(f"Connected to: {client.base_url}")

    if args.cleanup:
        cleanup(client, args.prefix)
        return

    print("\nTurboBulk Validation Best Practices Demo")
    print("=" * 50)

    # Run examples
    example_dry_run_validation(client, args.prefix)
    example_validation_modes(client, args.prefix)
    example_handling_validation_errors(client, args.prefix)
    example_complex_models(client, args.prefix)
    example_migration_workflow(client, args.prefix)

    print("\n" + "=" * 50)
    print("Demo complete!")
    print(f"\nTo clean up test data, run: python {Path(__file__).name} --cleanup")

    print("\nKey takeaways:")
    print("  1. Use dry_run=True before loading important data")
    print("  2. Use validation_mode='full' for complex models (cables, devices)")
    print("  3. Use validation_mode='none' only for trusted/pre-validated data")
    print("  4. Check pre_validation results in job output for warnings")


if __name__ == '__main__':
    main()
