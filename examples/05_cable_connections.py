#!/usr/bin/env python3
"""
TurboBulk Cable Connections

This example demonstrates:
- Creating cables between interfaces
- Using CableTermination with GenericForeignKey (ContentType)
- The two-phase cable loading process:
  1. Load Cable records
  2. Load CableTermination records

Usage:
    python 05_cable_connections.py --url http://netbox:8080 --token YOUR_TOKEN

Prerequisites:
    - Devices with interfaces must exist
    - API token with dcim.add_cable permission
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError
from common.parquet_utils import create_parquet, read_parquet


def get_available_interfaces(client: TurboBulkClient, device_prefix: str = None, limit: int = 200) -> List[Dict]:
    """
    Get interfaces that can be cabled (not already connected).

    Returns list of interface dicts with device and interface info.
    """
    params = {'limit': limit}
    if device_prefix:
        params['device__name__startswith'] = device_prefix

    # Get interfaces that are not already cabled
    # cable_id=null means not connected
    params['cabled'] = 'false'

    interfaces = client.rest_get_all('/api/dcim/interfaces/', params)
    return interfaces


def generate_cable_pairs(interfaces: List[Dict]) -> List[Tuple[Dict, Dict]]:
    """
    Generate pairs of interfaces to connect.

    Simple strategy: pair interfaces sequentially (0-1, 2-3, 4-5, etc.)
    Returns list of (interface_a, interface_b) tuples.
    """
    pairs = []
    for i in range(0, len(interfaces) - 1, 2):
        pairs.append((interfaces[i], interfaces[i + 1]))
    return pairs


def create_cables_and_terminations(
    client: TurboBulkClient,
    pairs: List[Tuple[Dict, Dict]],
    cable_type: str = 'cat6a',
    cable_status: str = 'connected',
) -> Tuple[int, int]:
    """
    Create cables and terminations for interface pairs.

    Two-phase process:
    1. Create Cable records with unique labels
    2. Export to get Cable IDs
    3. Create CableTermination records linking cables to interfaces

    Returns (cables_created, terminations_created)
    """
    if not pairs:
        return 0, 0

    # Get ContentType ID for Interface
    interface_ct_id = client.get_content_type_id('dcim', 'interface')
    print(f"Interface ContentType ID: {interface_ct_id}")

    # Phase 1: Create Cable records
    print(f"\nPhase 1: Creating {len(pairs)} cables...")
    cable_data = {
        'type': [],
        'status': [],
        'label': [],
        'color': [],
    }

    for i, (iface_a, iface_b) in enumerate(pairs):
        # Create unique label for cable
        label = f"cable-{iface_a['device']['name']}-{iface_a['name']}-{iface_b['device']['name']}-{iface_b['name']}"
        cable_data['type'].append(cable_type)
        cable_data['status'].append(cable_status)
        cable_data['label'].append(label)
        cable_data['color'].append('0000ff')  # Blue

    cables_path = Path('/tmp/cables.parquet')
    create_parquet(cable_data, cables_path)

    result = client.load(
        model='dcim.cable',
        data_path=cables_path,
        mode='insert',
        verbose=True,
    )

    cables_created = result.get('data', {}).get('rows_affected', 0)
    print(f"Created {cables_created} cables")

    # Phase 2: Get cable IDs by exporting
    print("\nPhase 2: Retrieving cable IDs...")

    # Export cables with our label prefix
    export_result = client.export(
        model='dcim.cable',
        filters={'label__startswith': 'cable-'},
        fields=['id', 'label'],
        output_path=Path('/tmp/cables_export.parquet'),
        format='parquet',
        verbose=False,
    )

    cable_export = read_parquet(export_result['path'])
    label_to_id = dict(zip(cable_export['label'], cable_export['id']))
    print(f"Retrieved {len(label_to_id)} cable IDs")

    # Phase 3: Create CableTermination records
    print("\nPhase 3: Creating cable terminations...")
    termination_data = {
        'cable_id': [],  # FK uses DB column name with _id suffix
        'cable_end': [],
        'termination_type_id': [],  # FK to ContentType
        'termination_id': [],  # ID of the terminated object
    }

    for i, (iface_a, iface_b) in enumerate(pairs):
        label = f"cable-{iface_a['device']['name']}-{iface_a['name']}-{iface_b['device']['name']}-{iface_b['name']}"
        cable_id = label_to_id.get(label)

        if cable_id is None:
            print(f"  Warning: No cable ID for {label}")
            continue

        # A-side termination
        termination_data['cable_id'].append(cable_id)
        termination_data['cable_end'].append('A')
        termination_data['termination_type_id'].append(interface_ct_id)
        termination_data['termination_id'].append(iface_a['id'])

        # B-side termination
        termination_data['cable_id'].append(cable_id)
        termination_data['cable_end'].append('B')
        termination_data['termination_type_id'].append(interface_ct_id)
        termination_data['termination_id'].append(iface_b['id'])

    terminations_path = Path('/tmp/cable_terminations.parquet')
    create_parquet(termination_data, terminations_path)

    result = client.load(
        model='dcim.cabletermination',
        data_path=terminations_path,
        mode='insert',
        verbose=True,
    )

    terminations_created = result.get('data', {}).get('rows_affected', 0)
    print(f"Created {terminations_created} cable terminations")

    return cables_created, terminations_created


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk Cable Connections',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--device-prefix', help='Only cable interfaces on devices with this prefix')
    parser.add_argument('--max-cables', type=int, default=50, help='Max cables to create (default: 50)')
    parser.add_argument('--cable-type', default='cat6a', help='Cable type (default: cat6a)')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # Step 1: Get available interfaces
        print(f"\nFetching uncabled interfaces...")
        max_interfaces = args.max_cables * 2  # Need 2 interfaces per cable
        interfaces = get_available_interfaces(
            client,
            device_prefix=args.device_prefix,
            limit=max_interfaces,
        )

        if len(interfaces) < 2:
            print("Not enough uncabled interfaces found.")
            print("Create devices with interfaces first using 04_interface_bulk.py")
            return

        print(f"Found {len(interfaces)} uncabled interfaces")

        # Step 2: Generate pairs
        pairs = generate_cable_pairs(interfaces[:max_interfaces])
        print(f"Will create {len(pairs)} cables")

        # Show sample
        if pairs:
            iface_a, iface_b = pairs[0]
            print(f"\nSample connection:")
            print(f"  {iface_a['device']['name']}:{iface_a['name']} <--> {iface_b['device']['name']}:{iface_b['name']}")

        # Step 3: Create cables and terminations
        cables, terminations = create_cables_and_terminations(
            client,
            pairs,
            cable_type=args.cable_type,
        )

        # Summary
        print(f"\n{'='*50}")
        print("SUCCESS!")
        print(f"{'='*50}")
        print(f"Cables created:      {cables}")
        print(f"Terminations created: {terminations}")
        print(f"\nView cables at: {client.base_url}/dcim/cables/")

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
