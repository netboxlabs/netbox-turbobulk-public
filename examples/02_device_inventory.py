#!/usr/bin/env python3
"""
TurboBulk Device Inventory - Bulk Insert 1000 Devices

This example demonstrates:
- FK resolution (looking up site, device_type, role IDs)
- Generating device data with realistic attributes
- Using tags with bulk operations
- Custom field data

Usage:
    python 02_device_inventory.py --url https://your-instance.cloud.netboxapp.com --token YOUR_TOKEN

Prerequisites:
    - At least one Site must exist
    - At least one DeviceType must exist
    - At least one DeviceRole must exist
    - API token with dcim.add_device permission
"""

import argparse
import gzip
import json
import random
import sys
from pathlib import Path

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError


def get_reference_data(client: TurboBulkClient) -> dict:
    """
    Fetch reference data (sites, device types, roles) for FK resolution.

    TurboBulk requires integer PKs for foreign keys. This function shows
    the common pattern of exporting reference data first, then mapping
    your source data to PKs.
    """
    print("Fetching reference data for FK resolution...")

    # Get sites
    sites = client.rest_get_all('/api/dcim/sites/')
    if not sites:
        raise TurboBulkError("No sites found. Create at least one site first.")
    print(f"  Found {len(sites)} sites")

    # Get device types
    device_types = client.rest_get_all('/api/dcim/device-types/')
    if not device_types:
        raise TurboBulkError("No device types found. Create at least one device type first.")
    print(f"  Found {len(device_types)} device types")

    # Get device roles
    roles = client.rest_get_all('/api/dcim/device-roles/')
    if not roles:
        raise TurboBulkError("No device roles found. Create at least one device role first.")
    print(f"  Found {len(roles)} device roles")

    # Get tags (optional)
    tags = client.rest_get_all('/api/extras/tags/')
    print(f"  Found {len(tags)} tags")

    return {
        'sites': sites,
        'device_types': device_types,
        'roles': roles,
        'tags': tags,
    }


def generate_devices(ref_data: dict, count: int, prefix: str) -> list:
    """
    Generate device data with resolved FK IDs.

    This shows the recommended pattern:
    1. Get reference data from NetBox
    2. Build lookup maps (name -> ID)
    3. Generate your data using the IDs

    Returns a list of device dicts (row-oriented for JSONL).
    """
    sites = ref_data['sites']
    device_types = ref_data['device_types']
    roles = ref_data['roles']
    tags = ref_data['tags']

    # Build lookup maps
    site_ids = [s['id'] for s in sites]
    device_type_ids = [dt['id'] for dt in device_types]
    role_ids = [r['id'] for r in roles]
    tag_slugs = [t['slug'] for t in tags] if tags else []

    # Status distribution: 80% active, 15% planned, 5% staged
    status_weights = ['active'] * 80 + ['planned'] * 15 + ['staged'] * 5

    devices = []
    for i in range(1, count + 1):
        device = {
            'name': f'{prefix}-{i:05d}',
            'device_type_id': random.choice(device_type_ids),  # FK uses _id suffix
            'role_id': random.choice(role_ids),
            'site_id': random.choice(site_ids),
            'status': random.choice(status_weights),
        }

        # 70% have serial numbers
        if random.random() < 0.7:
            device['serial'] = f'SN-{prefix.upper()}-{i:06d}'

        # 30% have asset tags
        if random.random() < 0.3:
            device['asset_tag'] = f'ASSET-{prefix.upper()}-{i:06d}'

        # Custom fields
        device['custom_field_data'] = {
            'inventory_source': 'turbobulk-example',
            'import_batch': prefix,
        }

        # Tags: 0-3 random tags per device
        if tag_slugs:
            num_tags = random.randint(0, min(3, len(tag_slugs)))
            device['_tags'] = random.sample(tag_slugs, num_tags)

        devices.append(device)

    return devices


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk Device Inventory - Bulk insert devices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--prefix', default='inv', help='Device name prefix (default: inv)')
    parser.add_argument('--count', type=int, default=1000, help='Number of devices (default: 1000)')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # Step 1: Get reference data for FK resolution
        ref_data = get_reference_data(client)

        # Step 2: Generate device data
        print(f"\nGenerating {args.count} devices with prefix '{args.prefix}'...")
        devices = generate_devices(ref_data, args.count, args.prefix)

        # Step 3: Create JSONL file (gzipped)
        jsonl_path = Path('/tmp/devices_inventory.jsonl.gz')
        with gzip.open(jsonl_path, 'wt', encoding='utf-8') as f:
            for device in devices:
                f.write(json.dumps(device) + '\n')
        print(f"Created JSONL file: {jsonl_path}")

        # Step 4: Submit bulk load job
        print(f"\nSubmitting bulk insert job...")
        result = client.load(
            model='dcim.device',
            data_path=jsonl_path,
            mode='insert',
            verbose=True,
        )

        # Print summary
        print(f"\n{'='*50}")
        print("SUCCESS!")
        print(f"{'='*50}")
        print(f"Status:       {result.get('status')}")
        print(f"Rows:         {result.get('data', {}).get('rows_affected', 'N/A')}")
        print(f"Tags:         {result.get('data', {}).get('tags_processed', 'N/A')}")
        print(f"Duration:     {result.get('duration_seconds', 'N/A')}s")
        print(f"\nView your devices at: {client.base_url}/dcim/devices/?q={args.prefix}")

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        print(f"Details: {e.job_result}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
