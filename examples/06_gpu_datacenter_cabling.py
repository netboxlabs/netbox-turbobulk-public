#!/usr/bin/env python3
"""
GPU Datacenter Cabling Design Tool

Demonstrates an iterative cabling design workflow for AI/ML datacenters:
1. Push ~200K cables to NetBox for review
2. Delete cables when changes are needed
3. Regenerate and rewrite cables with modifications
4. Repeat until design is finalized

This example creates a complete GPU cluster spine-leaf fabric:
- Spine switches for cross-pod connectivity
- Leaf (ToR) switches per rack
- GPU servers with multiple high-speed NICs
- Full mesh of fiber cables

Commands:
    setup    - Create infrastructure (site, device types, roles)
    devices  - Create devices and interfaces
    push     - Generate and load cables (the main operation)
    status   - Show current cable count
    delete   - Delete all cables (for redesign)
    teardown - Remove all created objects

Usage:
    # First time: set up infrastructure
    python 06_gpu_datacenter_cabling.py setup --url $URL --token $TOKEN

    # Create devices (takes a minute for large topology)
    python 06_gpu_datacenter_cabling.py devices --url $URL --token $TOKEN

    # Push initial cabling design
    python 06_gpu_datacenter_cabling.py push --url $URL --token $TOKEN

    # Check status
    python 06_gpu_datacenter_cabling.py status --url $URL --token $TOKEN

    # Make topology changes, then delete and re-push
    python 06_gpu_datacenter_cabling.py delete --url $URL --token $TOKEN
    python 06_gpu_datacenter_cabling.py push --url $URL --token $TOKEN

    # Cleanup when done
    python 06_gpu_datacenter_cabling.py teardown --url $URL --token $TOKEN

Scaling:
    Default topology: ~40K cables (8 pods)
    For ~200K cables, use: --pods 40 or adjust other parameters
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Optional

sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError
from common.parquet_utils import create_parquet, read_parquet, create_pk_parquet
from common.topology import GPUDatacenterTopology


def get_topology(args) -> GPUDatacenterTopology:
    """Create topology from command-line args."""
    return GPUDatacenterTopology(
        pods=args.pods,
        spines_per_pod=args.spines_per_pod,
        leaves_per_pod=args.leaves_per_pod,
        gpu_servers_per_leaf=args.gpu_servers_per_leaf,
        nics_per_gpu_server=args.nics_per_gpu_server,
        prefix=args.prefix,
    )


def cmd_setup(client: TurboBulkClient, args):
    """Create infrastructure: site, manufacturer, device types, roles."""
    topo = get_topology(args)
    print(topo.summary())

    print("Setting up infrastructure...")

    # Create site
    print("\nCreating site...")
    site_data = {
        'name': [f'{topo.prefix}-datacenter'],
        'slug': [f'{topo.prefix}-datacenter'],
        'status': ['active'],
        'description': ['GPU Datacenter for TurboBulk Example'],
    }
    site_path = Path('/tmp/gpu_dc_site.parquet')
    create_parquet(site_data, site_path)

    try:
        result = client.load('dcim.site', site_path, mode='insert', verbose=False)
        print(f"  Created site: {topo.prefix}-datacenter")
    except JobFailedError as e:
        if 'unique' in str(e).lower():
            print(f"  Site already exists: {topo.prefix}-datacenter")
        else:
            raise

    # Create manufacturer
    print("\nCreating manufacturer...")
    mfg_data = {
        'name': [topo.prefix],
        'slug': [topo.prefix],
    }
    mfg_path = Path('/tmp/gpu_dc_mfg.parquet')
    create_parquet(mfg_data, mfg_path)

    try:
        result = client.load('dcim.manufacturer', mfg_path, mode='insert', verbose=False)
        print(f"  Created manufacturer: {topo.prefix}")
    except JobFailedError as e:
        if 'unique' in str(e).lower():
            print(f"  Manufacturer already exists: {topo.prefix}")
        else:
            raise

    # Get manufacturer ID
    mfg_resp = client.rest_get('/api/dcim/manufacturers/', {'slug': topo.prefix})
    if not mfg_resp.get('results'):
        raise TurboBulkError(f"Manufacturer not found: {topo.prefix}")
    mfg_id = mfg_resp['results'][0]['id']

    # Create device types
    print("\nCreating device types...")
    device_types = topo.generate_device_types()
    dt_data = {
        'manufacturer_id': [mfg_id] * len(device_types),  # FK uses _id suffix
        'model': [dt['model'] for dt in device_types],
        'slug': [dt['slug'] for dt in device_types],
        'u_height': [dt['u_height'] for dt in device_types],
    }
    dt_path = Path('/tmp/gpu_dc_device_types.parquet')
    create_parquet(dt_data, dt_path)

    try:
        result = client.load('dcim.devicetype', dt_path, mode='insert', verbose=False)
        print(f"  Created {len(device_types)} device types")
    except JobFailedError as e:
        if 'unique' in str(e).lower():
            print(f"  Device types already exist")
        else:
            raise

    # Create device roles
    print("\nCreating device roles...")
    roles = topo.generate_device_roles()
    role_data = {
        'name': [r['name'] for r in roles],
        'slug': [r['slug'] for r in roles],
        'color': [r['color'] for r in roles],
    }
    role_path = Path('/tmp/gpu_dc_roles.parquet')
    create_parquet(role_data, role_path)

    try:
        result = client.load('dcim.devicerole', role_path, mode='insert', verbose=False)
        print(f"  Created {len(roles)} device roles")
    except JobFailedError as e:
        if 'unique' in str(e).lower():
            print(f"  Device roles already exist")
        else:
            raise

    print("\nSetup complete!")


def cmd_devices(client: TurboBulkClient, args):
    """Create devices and interfaces."""
    topo = get_topology(args)
    print(topo.summary())

    # Get reference IDs
    print("Fetching reference data...")

    # Site ID
    site_resp = client.rest_get('/api/dcim/sites/', {'slug': f'{topo.prefix}-datacenter'})
    if not site_resp.get('results'):
        raise TurboBulkError(f"Site not found. Run 'setup' first.")
    site_id = site_resp['results'][0]['id']

    # Device type IDs
    dt_resp = client.rest_get_all('/api/dcim/device-types/', {'slug__startswith': topo.prefix})
    device_type_ids = {dt['slug']: dt['id'] for dt in dt_resp}
    print(f"  Found {len(device_type_ids)} device types")

    # Device role IDs
    role_resp = client.rest_get_all('/api/dcim/device-roles/', {'slug__startswith': topo.prefix})
    device_role_ids = {r['slug']: r['id'] for r in role_resp}
    print(f"  Found {len(device_role_ids)} device roles")

    # Generate devices
    print(f"\nGenerating {topo.total_devices:,} devices...")
    device_data = topo.generate_devices(
        site_id=site_id,
        device_type_ids=device_type_ids,
        device_role_ids=device_role_ids,
    )

    device_path = Path('/tmp/gpu_dc_devices.parquet')
    create_parquet(device_data, device_path)

    print("Loading devices...")
    start = time.time()
    result = client.load('dcim.device', device_path, mode='insert', verbose=True)
    duration = time.time() - start
    rows = result.get('data', {}).get('rows_affected', 0)
    print(f"  Loaded {rows:,} devices in {duration:.1f}s ({rows/duration:.0f} devices/sec)")

    # Get device IDs for interface generation
    print("\nFetching device IDs...")
    devices = client.rest_get_all('/api/dcim/devices/', {'name__startswith': topo.prefix})
    device_id_map = {d['name']: d['id'] for d in devices}
    print(f"  Found {len(device_id_map):,} devices")

    # Generate interfaces
    print(f"\nGenerating interfaces...")
    interface_data = topo.generate_interfaces(device_id_map)
    total_interfaces = len(interface_data['device_id'])
    print(f"  Generated {total_interfaces:,} interfaces")

    interface_path = Path('/tmp/gpu_dc_interfaces.parquet')
    create_parquet(interface_data, interface_path)

    print("Loading interfaces...")
    start = time.time()
    result = client.load('dcim.interface', interface_path, mode='insert', verbose=True)
    duration = time.time() - start
    rows = result.get('data', {}).get('rows_affected', 0)
    print(f"  Loaded {rows:,} interfaces in {duration:.1f}s ({rows/duration:.0f} interfaces/sec)")

    print("\nDevices and interfaces created!")


def cmd_push(client: TurboBulkClient, args):
    """Push cable design to NetBox."""
    topo = get_topology(args)
    print(topo.summary())

    # Get interface map
    print("Fetching interfaces...")
    interfaces = client.rest_get_all(
        '/api/dcim/interfaces/',
        {'device__name__startswith': topo.prefix}
    )
    interface_map = {
        f"{iface['device']['name']}:{iface['name']}": iface['id']
        for iface in interfaces
    }
    print(f"  Found {len(interface_map):,} interfaces")

    if not interface_map:
        print("No interfaces found. Run 'devices' first.")
        return

    # Get ContentType ID for interface
    ct_id = client.get_content_type_id('dcim', 'interface')
    print(f"  Interface ContentType ID: {ct_id}")

    # Generate cables
    print(f"\nGenerating cables...")
    cables, terminations = topo.generate_cables(interface_map, ct_id)
    print(f"  Generated {len(cables['label']):,} cables")
    print(f"  Generated {len(terminations['cable_id']):,} termination records")

    # Load cables
    print("\nLoading cables...")
    cables_path = Path('/tmp/gpu_dc_cables.parquet')
    create_parquet(cables, cables_path)

    start = time.time()
    result = client.load('dcim.cable', cables_path, mode='insert', verbose=True)
    duration = time.time() - start
    rows = result.get('data', {}).get('rows_affected', 0)
    print(f"  Loaded {rows:,} cables in {duration:.1f}s ({rows/duration:.0f} cables/sec)")

    # Get cable IDs
    print("\nFetching cable IDs...")
    export_result = client.export(
        model='dcim.cable',
        filters={'label__startswith': topo.prefix},
        fields=['id', 'label'],
        output_path=Path('/tmp/gpu_dc_cables_export.parquet'),
        format='parquet',
        verbose=False,
    )
    cable_export = read_parquet(export_result['path'])
    label_to_id = dict(zip(cable_export['label'], cable_export['id']))
    print(f"  Retrieved {len(label_to_id):,} cable IDs")

    # Update terminations with cable IDs
    print("\nUpdating terminations with cable IDs...")
    updated_terminations = topo.update_terminations_with_cable_ids(terminations, label_to_id)
    print(f"  Prepared {len(updated_terminations['cable_id']):,} termination records")

    # Load terminations
    print("\nLoading cable terminations...")
    terms_path = Path('/tmp/gpu_dc_terminations.parquet')
    create_parquet(updated_terminations, terms_path)

    start = time.time()
    result = client.load('dcim.cabletermination', terms_path, mode='insert', verbose=True)
    duration = time.time() - start
    rows = result.get('data', {}).get('rows_affected', 0)
    print(f"  Loaded {rows:,} terminations in {duration:.1f}s ({rows/duration:.0f} terminations/sec)")

    print(f"\n{'='*60}")
    print("CABLE DESIGN PUSHED!")
    print(f"{'='*60}")
    print(f"Cables:       {len(cables['label']):,}")
    print(f"Terminations: {len(updated_terminations['cable_id']):,}")
    print(f"\nView cables at: {client.base_url}/dcim/cables/?label__startswith={topo.prefix}")


def cmd_status(client: TurboBulkClient, args):
    """Show current cable count."""
    topo = get_topology(args)

    # Count cables
    resp = client.rest_get('/api/dcim/cables/', {'label__startswith': topo.prefix, 'limit': 1})
    current = resp.get('count', 0)

    print(f"\nCable Design Status: {topo.prefix}")
    print(f"{'='*40}")
    print(f"Current cables:  {current:,}")
    print(f"Target cables:   {topo.estimated_cables:,}")

    if current == 0:
        print("\nNo cables found. Run 'push' to create cables.")
    elif current < topo.estimated_cables:
        print(f"\nMissing {topo.estimated_cables - current:,} cables.")
    else:
        print("\nCable design complete!")


def cmd_delete(client: TurboBulkClient, args):
    """Delete all cables from current design."""
    topo = get_topology(args)

    # Get cable IDs
    print(f"Fetching cables with prefix '{topo.prefix}'...")
    export_path = client.export(
        model='dcim.cable',
        filters={'label__startswith': topo.prefix},
        fields=['id'],
        output_path=Path('/tmp/gpu_dc_delete_cables.parquet'),
        verbose=False,
    )
    cable_data = read_parquet(export_path)
    cable_ids = cable_data.get('id', [])

    if not cable_ids:
        print("No cables found to delete.")
        return

    print(f"Found {len(cable_ids):,} cables to delete")

    # First, delete cable terminations (they reference cables via FK)
    print("\nFetching cable terminations...")
    term_export_path = client.export(
        model='dcim.cabletermination',
        filters={'cable_id__in': cable_ids},
        fields=['id'],
        output_path=Path('/tmp/gpu_dc_delete_terminations.parquet'),
        verbose=False,
    )
    term_data = read_parquet(term_export_path)
    term_ids = term_data.get('id', [])

    if term_ids:
        print(f"Found {len(term_ids):,} terminations to delete")
        term_delete_path = Path('/tmp/gpu_dc_delete_term_ids.parquet')
        create_pk_parquet(term_ids, term_delete_path)

        print("Deleting terminations...")
        start = time.time()
        result = client.delete('dcim.cabletermination', term_delete_path, verbose=True)
        duration = time.time() - start
        rows = result.get('data', {}).get('rows_affected', 0)
        print(f"  Deleted {rows:,} terminations in {duration:.1f}s")

    # Now delete cables
    delete_path = Path('/tmp/gpu_dc_delete_ids.parquet')
    create_pk_parquet(cable_ids, delete_path)

    print("\nDeleting cables...")
    start = time.time()
    result = client.delete('dcim.cable', delete_path, verbose=True)
    duration = time.time() - start
    rows = result.get('data', {}).get('rows_affected', 0)
    print(f"  Deleted {rows:,} cables in {duration:.1f}s")

    print("\nCables deleted. Ready for new design push.")


def cmd_teardown(client: TurboBulkClient, args):
    """Remove all created objects."""
    topo = get_topology(args)

    print(f"Tearing down topology: {topo.prefix}")

    # Delete cables first
    print("\n1. Deleting cables...")
    try:
        cmd_delete(client, args)
    except Exception as e:
        print(f"   Error: {e}")

    # Delete interfaces
    print("\n2. Deleting interfaces...")
    export_path = client.export(
        model='dcim.interface',
        filters={'device__name__startswith': topo.prefix},
        fields=['id'],
        output_path=Path('/tmp/gpu_dc_delete_interfaces.parquet'),
        verbose=False,
    )
    iface_data = read_parquet(export_path)
    iface_ids = iface_data.get('id', [])
    if iface_ids:
        delete_path = Path('/tmp/gpu_dc_delete_iface_ids.parquet')
        create_pk_parquet(iface_ids, delete_path)
        result = client.delete('dcim.interface', delete_path, verbose=False)
        print(f"   Deleted {result.get('data', {}).get('rows_affected', 0):,} interfaces")
    else:
        print("   No interfaces to delete")

    # Delete devices
    print("\n3. Deleting devices...")
    export_path = client.export(
        model='dcim.device',
        filters={'name__startswith': topo.prefix},
        fields=['id'],
        output_path=Path('/tmp/gpu_dc_delete_devices.parquet'),
        verbose=False,
    )
    device_data = read_parquet(export_path)
    device_ids = device_data.get('id', [])
    if device_ids:
        delete_path = Path('/tmp/gpu_dc_delete_device_ids.parquet')
        create_pk_parquet(device_ids, delete_path)
        result = client.delete('dcim.device', delete_path, verbose=False)
        print(f"   Deleted {result.get('data', {}).get('rows_affected', 0):,} devices")
    else:
        print("   No devices to delete")

    # Note: Keeping device types, roles, manufacturer, and site for reuse
    print("\n4. Keeping infrastructure (site, device types, roles) for reuse")
    print("   To remove those, delete manually via NetBox UI")

    print("\nTeardown complete!")


def main():
    parser = argparse.ArgumentParser(
        description='GPU Datacenter Cabling Design Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Connection args
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN)')

    # Topology args
    parser.add_argument('--prefix', default='gpu-dc', help='Naming prefix (default: gpu-dc)')
    parser.add_argument('--pods', type=int, default=8, help='Number of pods (default: 8)')
    parser.add_argument('--spines-per-pod', type=int, default=4, help='Spines per pod (default: 4)')
    parser.add_argument('--leaves-per-pod', type=int, default=32, help='Leaves per pod (default: 32)')
    parser.add_argument('--gpu-servers-per-leaf', type=int, default=16, help='GPU servers per leaf (default: 16)')
    parser.add_argument('--nics-per-gpu-server', type=int, default=8, help='NICs per GPU server (default: 8)')

    # Command
    parser.add_argument('command', choices=['setup', 'devices', 'push', 'status', 'delete', 'teardown'],
                        help='Command to execute')

    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}\n")

        if args.command == 'setup':
            cmd_setup(client, args)
        elif args.command == 'devices':
            cmd_devices(client, args)
        elif args.command == 'push':
            cmd_push(client, args)
        elif args.command == 'status':
            cmd_status(client, args)
        elif args.command == 'delete':
            cmd_delete(client, args)
        elif args.command == 'teardown':
            cmd_teardown(client, args)

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)


if __name__ == '__main__':
    main()
