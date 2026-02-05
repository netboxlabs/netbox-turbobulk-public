#!/usr/bin/env python3
"""
TurboBulk Interface Bulk Operations

This example demonstrates:
- Dependency order: devices must exist before interfaces
- Bulk creating interfaces for existing devices
- Different interface types (1G, 10G, 25G, 100G, 400G)
- Generating interfaces per device

Usage:
    python 04_interface_bulk.py --url http://netbox:8080 --token YOUR_TOKEN

Prerequisites:
    - Devices must exist in NetBox
    - API token with dcim.add_interface permission
"""

import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError
from common.parquet_utils import create_parquet


# Common interface type choices from NetBox
INTERFACE_TYPES = {
    'server': [
        ('1000base-t', 'eth'),       # 1GbE copper
        ('10gbase-t', 'eth'),        # 10GbE copper
        ('25gbase-x-sfp28', 'eth'),  # 25GbE SFP28
    ],
    'switch': [
        ('10gbase-x-sfpp', 'eth'),       # 10GbE SFP+
        ('25gbase-x-sfp28', 'eth'),      # 25GbE SFP28
        ('100gbase-x-qsfp28', 'eth'),    # 100GbE QSFP28
        ('400gbase-x-qsfpdd', 'eth'),    # 400GbE QSFP-DD
    ],
    'gpu_server': [
        ('100gbase-x-qsfp28', 'eth'),    # 100GbE QSFP28
        ('400gbase-x-qsfpdd', 'eth'),    # 400GbE QSFP-DD
        ('800gbase-x-osfp', 'eth'),      # 800GbE OSFP
    ],
}


def get_devices(client: TurboBulkClient, filter_prefix: str = None, limit: int = None) -> list:
    """Fetch devices from NetBox."""
    params = {}
    if filter_prefix:
        params['name__startswith'] = filter_prefix
    if limit:
        params['limit'] = limit

    devices = client.rest_get_all('/api/dcim/devices/', params)
    return devices


def generate_interfaces_for_device(
    device_id: int,
    device_name: str,
    num_interfaces: int,
    interface_type: str = 'server',
) -> list:
    """
    Generate interface records for a single device.

    Returns list of dicts, each representing one interface.
    """
    types = INTERFACE_TYPES.get(interface_type, INTERFACE_TYPES['server'])
    interfaces = []

    for i in range(1, num_interfaces + 1):
        iface_type, prefix = random.choice(types)
        interfaces.append({
            'device': device_id,
            'name': f'{prefix}{i}',
            'type': iface_type,
            'enabled': True,
            'description': f'Interface {i} on {device_name}',
        })

    return interfaces


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk Interface Bulk Operations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--device-prefix', help='Only add interfaces to devices with this name prefix')
    parser.add_argument('--device-limit', type=int, default=100, help='Max devices to process (default: 100)')
    parser.add_argument('--interfaces-per-device', type=int, default=8, help='Interfaces per device (default: 8)')
    parser.add_argument('--interface-type', choices=['server', 'switch', 'gpu_server'], default='server',
                        help='Interface type profile (default: server)')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # Step 1: Get devices
        print(f"\nFetching devices...")
        devices = get_devices(
            client,
            filter_prefix=args.device_prefix,
            limit=args.device_limit,
        )

        if not devices:
            print("No devices found. Create devices first using 02_device_inventory.py")
            print("Or specify a different --device-prefix")
            return

        print(f"Found {len(devices)} devices")

        # Step 2: Generate interfaces
        print(f"\nGenerating {args.interfaces_per_device} interfaces per device...")
        all_interfaces = {
            'device_id': [],  # FK uses DB column name with _id suffix
            'name': [],
            'type': [],
            'enabled': [],
            'description': [],
        }

        for device in devices:
            interfaces = generate_interfaces_for_device(
                device_id=device['id'],
                device_name=device['name'],
                num_interfaces=args.interfaces_per_device,
                interface_type=args.interface_type,
            )
            for iface in interfaces:
                all_interfaces['device_id'].append(iface['device'])
                all_interfaces['name'].append(iface['name'])
                all_interfaces['type'].append(iface['type'])
                all_interfaces['enabled'].append(iface['enabled'])
                all_interfaces['description'].append(iface['description'])

        total_interfaces = len(all_interfaces['device_id'])
        print(f"Generated {total_interfaces} interfaces")

        # Step 3: Create Parquet file
        parquet_path = Path('/tmp/interfaces_bulk.parquet')
        create_parquet(all_interfaces, parquet_path)
        print(f"Created Parquet file: {parquet_path}")

        # Step 4: Submit bulk load job
        print(f"\nSubmitting bulk insert job...")
        result = client.load(
            model='dcim.interface',
            data_path=parquet_path,
            mode='insert',
            verbose=True,
        )

        # Summary
        print(f"\n{'='*50}")
        print("SUCCESS!")
        print(f"{'='*50}")
        print(f"Status:       {result.get('status')}")
        print(f"Rows:         {result.get('data', {}).get('rows_affected', 'N/A')}")
        print(f"Duration:     {result.get('duration_seconds', 'N/A')}s")

        rows = result.get('data', {}).get('rows_affected', 0)
        duration = result.get('duration_seconds', 1)
        if duration > 0:
            print(f"Throughput:   {rows/duration:.0f} interfaces/sec")

        if devices:
            sample_device = devices[0]['name']
            print(f"\nView interfaces at: {client.base_url}/dcim/devices/{devices[0]['id']}/interfaces/")

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
