#!/usr/bin/env python3
"""
Example 08: TurboBulk + NetBox Branching Workflow

This example demonstrates the complete workflow for using TurboBulk with
the netbox-branching plugin for branch-aware bulk operations.

Workflow:
1. Check if netbox-branching plugin is available
2. Create a test branch
3. Bulk insert devices to the branch (isolated from main)
4. Verify data isolation (devices visible in branch, not in main)
5. Bulk upsert to update devices in the branch
6. Bulk delete some devices in the branch
7. Inspect the branch diff/changes
8. (Optional) Merge the branch to main
9. Cleanup

Requirements:
- netbox-branching plugin installed and configured
- TurboBulk plugin installed

Run: python 08_branching_workflow.py [--merge] [--cleanup]
  --merge: Merge the test branch to main at the end
  --cleanup: Clean up test branch and data and exit
"""

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError


BRANCH_NAME = 'turbobulk-demo'
PREFIX = 'tb-branch'


def check_branching_available(client: TurboBulkClient) -> bool:
    """Check if netbox-branching plugin is installed and available."""
    try:
        response = client.session.get(
            f'{client.base_url}/api/plugins/branching/branches/',
            params={'limit': 1}
        )
        if response.status_code == 200:
            return True
        elif response.status_code == 403:
            # 403 means plugin exists but token doesn't have permission - this is OK for our check
            print("  Note: 403 response - plugin exists but token may lack permissions")
            return True
        elif response.status_code == 404:
            return False
        else:
            print(f"  Unexpected response: {response.status_code}")
            return False
    except Exception as e:
        print(f"  Error checking branching: {e}")
        return False


def create_branch(client: TurboBulkClient, name: str) -> Optional[dict]:
    """Create a new branch for testing."""
    print(f"\nCreating branch: {name}")

    # Check if branch already exists
    response = client.session.get(
        f'{client.base_url}/api/plugins/branching/branches/',
        params={'name': name}
    )
    response.raise_for_status()
    existing = response.json().get('results', [])

    if existing:
        print(f"  Branch '{name}' already exists")
        branch = existing[0]
        status = branch['status']
        status_value = status['value'] if isinstance(status, dict) else status
        if status_value != 'ready':
            print(f"  Warning: Branch status is '{status_value}', waiting for 'ready'...")
            branch = wait_for_branch_ready(client, branch['id'])
        return branch

    # Create new branch
    response = client.session.post(
        f'{client.base_url}/api/plugins/branching/branches/',
        json={
            'name': name,
            'description': 'TurboBulk demo branch - safe to delete',
        }
    )
    response.raise_for_status()
    branch = response.json()
    print(f"  Created branch ID: {branch['id']}")

    # Wait for branch to be ready
    branch = wait_for_branch_ready(client, branch['id'])
    return branch


def wait_for_branch_ready(client: TurboBulkClient, branch_id: int, timeout: int = 120) -> dict:
    """Wait for a branch to reach READY status."""
    print("  Waiting for branch to be ready...")
    start = time.time()

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise TurboBulkError(f"Branch did not become ready within {timeout}s")

        response = client.session.get(
            f'{client.base_url}/api/plugins/branching/branches/{branch_id}/'
        )
        response.raise_for_status()
        branch = response.json()

        status = branch['status']
        # Status can be a string or a dict with 'value' key depending on API version
        status_value = status['value'] if isinstance(status, dict) else status
        print(f"  [{elapsed:.0f}s] Branch status: {status_value}")

        if status_value == 'ready':
            return branch

        if status_value in ('failed', 'reverting'):
            raise TurboBulkError(f"Branch entered unexpected status: {status}")

        time.sleep(2)


def delete_branch(client: TurboBulkClient, name: str):
    """Delete a branch."""
    response = client.session.get(
        f'{client.base_url}/api/plugins/branching/branches/',
        params={'name': name}
    )
    response.raise_for_status()
    branches = response.json().get('results', [])

    if not branches:
        print(f"  Branch '{name}' not found")
        return

    branch_id = branches[0]['id']
    response = client.session.delete(
        f'{client.base_url}/api/plugins/branching/branches/{branch_id}/'
    )
    if response.status_code in (200, 204):
        print(f"  Deleted branch '{name}'")
    else:
        print(f"  Failed to delete branch: {response.status_code}")


def get_reference_data(client: TurboBulkClient) -> dict:
    """Get reference data needed for device creation."""
    refs = {}

    # Get a site
    sites = client.rest_get('/api/dcim/sites/', {'limit': 1})
    if not sites.get('results'):
        raise TurboBulkError("No sites found - please create a site first")
    refs['site_id'] = sites['results'][0]['id']
    refs['site_name'] = sites['results'][0]['name']

    # Get a device type
    device_types = client.rest_get('/api/dcim/device-types/', {'limit': 1})
    if not device_types.get('results'):
        raise TurboBulkError("No device types found - please create a device type first")
    refs['device_type_id'] = device_types['results'][0]['id']

    # Get a device role
    roles = client.rest_get('/api/dcim/device-roles/', {'limit': 1})
    if not roles.get('results'):
        raise TurboBulkError("No device roles found - please create a device role first")
    refs['role_id'] = roles['results'][0]['id']

    return refs


def bulk_insert_to_branch(client: TurboBulkClient, branch_name: str, refs: dict) -> list:
    """Bulk insert devices to a branch."""
    print(f"\n--- Bulk Insert to Branch '{branch_name}' ---")

    # Create device data
    device_names = [f'{PREFIX}-device-{i:03d}' for i in range(1, 11)]

    schema = pa.schema([
        ('name', pa.string()),
        ('site_id', pa.int64()),
        ('device_type_id', pa.int64()),
        ('role_id', pa.int64()),
        ('status', pa.string()),
        ('serial', pa.string()),
    ])

    data = {
        'name': device_names,
        'site_id': [refs['site_id']] * 10,
        'device_type_id': [refs['device_type_id']] * 10,
        'role_id': [refs['role_id']] * 10,
        'status': ['planned'] * 10,
        'serial': [f'SN-BRANCH-{i:03d}' for i in range(1, 11)],
    }

    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print(f"  Inserting {len(device_names)} devices to branch...")
        result = client.load(
            'dcim.device',
            path,
            mode='insert',
            branch=branch_name,  # Key parameter: target the branch
            verbose=True,
        )

        job_data = result.get('data', {})
        print(f"  Inserted: {job_data.get('rows_inserted', 'N/A')} rows")
        print(f"  Changelogs created: {job_data.get('changelogs_created', 'N/A')}")

        return device_names

    finally:
        path.unlink(missing_ok=True)


def verify_branch_isolation(client: TurboBulkClient, branch: dict, device_names: list):
    """Verify that devices exist in branch but not in main."""
    print(f"\n--- Verify Branch Isolation ---")

    branch_name = branch['name']
    schema_id = branch['schema_id']

    # Query main schema (no branch header)
    main_devices = client.rest_get(
        '/api/dcim/devices/',
        {'name__startswith': PREFIX}
    )
    main_count = len(main_devices.get('results', []))
    print(f"  Devices in main schema: {main_count}")

    # Query branch schema (with branch header)
    # NetBox branching uses X-NetBox-Branch header with schema_id, not name
    branch_devices = client.session.get(
        f'{client.base_url}/api/dcim/devices/',
        params={'name__startswith': PREFIX},
        headers={'X-NetBox-Branch': schema_id}
    )
    branch_devices.raise_for_status()
    branch_count = len(branch_devices.json().get('results', []))
    print(f"  Devices in branch '{branch_name}': {branch_count}")

    if main_count == 0 and branch_count == len(device_names):
        print("  [OK] Data isolation verified - devices only visible in branch")
    elif main_count > 0:
        print("  [WARNING] Some devices visible in main - may be from previous run")
    else:
        print(f"  [WARNING] Expected {len(device_names)} devices in branch, found {branch_count}")


def bulk_upsert_in_branch(client: TurboBulkClient, branch: dict, refs: dict):
    """Bulk upsert to update devices in the branch."""
    branch_name = branch['name']
    schema_id = branch['schema_id']
    print(f"\n--- Bulk Upsert in Branch '{branch_name}' ---")

    # First, get device IDs from the branch for the devices we want to update
    # NetBox Device has a unique constraint on (lower(name), site_id), not just name
    # So we need to use ID-based upsert instead
    branch_devices = client.session.get(
        f'{client.base_url}/api/dcim/devices/',
        params={'name__startswith': PREFIX, 'limit': 5},
        headers={'X-NetBox-Branch': schema_id}
    )
    branch_devices.raise_for_status()
    devices = branch_devices.json().get('results', [])[:5]

    if not devices:
        print("  No devices found in branch to update")
        return

    device_ids = [d['id'] for d in devices]
    print(f"  Found {len(device_ids)} devices to update (IDs: {device_ids[:3]}...)")

    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('site_id', pa.int64()),
        ('device_type_id', pa.int64()),
        ('role_id', pa.int64()),
        ('status', pa.string()),
        ('serial', pa.string()),
    ])

    data = {
        'id': device_ids,
        'name': [d['name'] for d in devices],
        'site_id': [refs['site_id']] * len(devices),
        'device_type_id': [refs['device_type_id']] * len(devices),
        'role_id': [refs['role_id']] * len(devices),
        'status': ['active'] * len(devices),  # Changed from 'planned'
        'serial': [f'SN-UPDATED-{i:03d}' for i in range(1, len(devices) + 1)],  # Updated
    }

    table = pa.table(data, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        print(f"  Upserting {len(device_ids)} devices in branch...")
        result = client.load(
            'dcim.device',
            path,
            mode='upsert',
            # Match by ID (PK) - this always has a unique constraint
            branch=branch_name,
            verbose=True,
        )

        job_data = result.get('data', {})
        print(f"  Upserted: {job_data.get('rows_inserted', 'N/A')} rows")
        print(f"  Changelogs created: {job_data.get('changelogs_created', 'N/A')}")

    finally:
        path.unlink(missing_ok=True)


def bulk_delete_in_branch(client: TurboBulkClient, branch: dict):
    """Bulk delete some devices in the branch."""
    branch_name = branch['name']
    schema_id = branch['schema_id']
    print(f"\n--- Bulk Delete in Branch '{branch_name}' ---")

    # First get the IDs of devices 6-10 from the branch
    branch_devices = client.session.get(
        f'{client.base_url}/api/dcim/devices/',
        params={'name__startswith': f'{PREFIX}-device-00'},
        headers={'X-NetBox-Branch': schema_id}
    )
    branch_devices.raise_for_status()
    devices = branch_devices.json().get('results', [])

    # Filter to devices 006-010
    to_delete = [d for d in devices if d['name'] >= f'{PREFIX}-device-006']
    if not to_delete:
        print("  No devices to delete")
        return

    device_ids = [d['id'] for d in to_delete]
    print(f"  Deleting {len(device_ids)} devices from branch...")

    schema = pa.schema([('id', pa.int64())])
    table = pa.table({'id': device_ids}, schema=schema)

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        path = Path(f.name)

    try:
        result = client.delete(
            'dcim.device',
            path,
            branch=branch_name,
            verbose=True,
        )

        job_data = result.get('data', {})
        print(f"  Deleted: {job_data.get('rows_deleted', 'N/A')} rows")
        print(f"  Changelogs created: {job_data.get('changelogs_created', 'N/A')}")

    finally:
        path.unlink(missing_ok=True)


def show_branch_changes(client: TurboBulkClient, branch: dict):
    """Show the changes made in the branch."""
    branch_name = branch['name']
    schema_id = branch['schema_id']
    print(f"\n--- Branch Changes Summary ---")

    status = branch['status']
    status_value = status['value'] if isinstance(status, dict) else status
    print(f"  Branch: {branch_name}")
    print(f"  Status: {status_value}")
    print(f"  Created: {branch['created']}")

    # Try to get branch diff/changes
    branch_id = branch['id']
    try:
        response = client.session.get(
            f'{client.base_url}/api/plugins/branching/branches/{branch_id}/changes/'
        )
        if response.status_code == 200:
            changes = response.json()
            print(f"\n  Changes in branch:")
            if isinstance(changes, dict) and 'results' in changes:
                for change in changes.get('results', [])[:10]:
                    print(f"    - {change.get('action', 'unknown')}: {change.get('object_repr', 'object')}")
            elif isinstance(changes, list):
                for change in changes[:10]:
                    print(f"    - {change}")
    except Exception as e:
        print(f"  Could not retrieve changes: {e}")

    # Show final device state in branch
    branch_devices = client.session.get(
        f'{client.base_url}/api/dcim/devices/',
        params={'name__startswith': PREFIX},
        headers={'X-NetBox-Branch': schema_id}
    )
    branch_devices.raise_for_status()
    devices = branch_devices.json().get('results', [])
    print(f"\n  Devices remaining in branch: {len(devices)}")
    for d in devices[:5]:
        print(f"    - {d['name']}: status={d['status']['value']}, serial={d.get('serial', 'N/A')}")
    if len(devices) > 5:
        print(f"    ... and {len(devices) - 5} more")


def merge_branch(client: TurboBulkClient, branch_name: str):
    """Merge the branch to main."""
    print(f"\n--- Merging Branch '{branch_name}' to Main ---")

    # Get branch ID
    response = client.session.get(
        f'{client.base_url}/api/plugins/branching/branches/',
        params={'name': branch_name}
    )
    response.raise_for_status()
    branches = response.json().get('results', [])

    if not branches:
        print(f"  Branch '{branch_name}' not found")
        return

    branch_id = branches[0]['id']

    # Merge
    response = client.session.post(
        f'{client.base_url}/api/plugins/branching/branches/{branch_id}/merge/'
    )

    if response.status_code in (200, 202):
        print("  Merge initiated")
        # Wait for merge to complete
        for _ in range(60):
            time.sleep(2)
            response = client.session.get(
                f'{client.base_url}/api/plugins/branching/branches/{branch_id}/'
            )
            if response.status_code == 404:
                print("  Branch merged and deleted")
                return
            branch = response.json()
            status = branch['status']
            status_value = status['value'] if isinstance(status, dict) else status
            print(f"  Branch status: {status_value}")
            if status_value == 'merged':
                print("  Merge complete!")
                return
            if status_value in ('failed', 'reverting'):
                print(f"  Merge failed with status: {status_value}")
                return
        print("  Merge timed out")
    else:
        print(f"  Merge failed: {response.status_code}")
        print(f"  Response: {response.text[:200]}")


def cleanup(client: TurboBulkClient):
    """Clean up test data and branch."""
    print("\n--- Cleanup ---")

    # Delete branch first (which will discard changes)
    delete_branch(client, BRANCH_NAME)

    # Clean up any devices that might have been merged to main
    devices = client.rest_get('/api/dcim/devices/', {'name__startswith': PREFIX})
    device_ids = [d['id'] for d in devices.get('results', [])]

    if device_ids:
        schema = pa.schema([('id', pa.int64())])
        table = pa.table({'id': device_ids}, schema=schema)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            path = Path(f.name)

        try:
            result = client.delete('dcim.device', path, verbose=False)
            print(f"  Deleted {result.get('data', {}).get('rows_deleted', 0)} devices from main")
        finally:
            path.unlink(missing_ok=True)
    else:
        print("  No devices to clean up from main")


def main():
    parser = argparse.ArgumentParser(description='TurboBulk + NetBox Branching demo')
    parser.add_argument('--merge', action='store_true',
                        help='Merge the test branch to main at the end')
    parser.add_argument('--cleanup', action='store_true',
                        help='Clean up test branch and data and exit')
    args = parser.parse_args()

    client = TurboBulkClient()
    print(f"Connected to: {client.base_url}")

    # Check if branching is available
    print("\nChecking for netbox-branching plugin...")
    if not check_branching_available(client):
        print("ERROR: netbox-branching plugin is not installed or not available.")
        print("This example requires the netbox-branching plugin.")
        print("Install with: pip install netbox-branching")
        sys.exit(1)
    print("  [OK] netbox-branching plugin is available")

    if args.cleanup:
        cleanup(client)
        return

    print("\n" + "=" * 60)
    print("TurboBulk + NetBox Branching Workflow Demo")
    print("=" * 60)

    try:
        # Step 1: Create branch
        branch = create_branch(client, BRANCH_NAME)
        if not branch:
            print("Failed to create/get branch")
            return

        # Get reference data for devices
        refs = get_reference_data(client)
        print(f"\nUsing site: {refs['site_name']} (ID: {refs['site_id']})")

        # Step 2: Bulk insert to branch
        device_names = bulk_insert_to_branch(client, BRANCH_NAME, refs)

        # Step 3: Verify isolation
        verify_branch_isolation(client, branch, device_names)

        # Step 4: Bulk upsert in branch
        bulk_upsert_in_branch(client, branch, refs)

        # Step 5: Bulk delete in branch
        bulk_delete_in_branch(client, branch)

        # Step 6: Show changes
        show_branch_changes(client, branch)

        # Step 7: Optionally merge
        if args.merge:
            merge_branch(client, BRANCH_NAME)

            # Verify data now in main
            print("\n--- Verifying Merged Data in Main ---")
            main_devices = client.rest_get(
                '/api/dcim/devices/',
                {'name__startswith': PREFIX}
            )
            print(f"  Devices now in main: {len(main_devices.get('results', []))}")
        else:
            print("\n" + "-" * 60)
            print("Branch NOT merged (use --merge to merge)")
            print(f"View branch in NetBox UI or merge manually")

    except TurboBulkError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        raise

    print("\n" + "=" * 60)
    print("Demo complete!")
    if not args.merge:
        print(f"\nTo clean up: python {Path(__file__).name} --cleanup")
        print(f"To merge branch: python {Path(__file__).name} --merge")


if __name__ == '__main__':
    main()
