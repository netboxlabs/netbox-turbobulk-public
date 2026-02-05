#!/usr/bin/env python3
"""
Example 11: Event Streams Integration

Demonstrates TurboBulk's event streams support, showing how bulk operations
trigger the same event pipeline as normal REST API operations.

Key concepts:
- Event dispatch enabled by default
- Per-request dispatch_events parameter
- Use dispatch_events=False for large initial loads
- Events dispatched asynchronously after operation completes

Prerequisites:
- NetBox Cloud or Enterprise with TurboBulk enabled
- Authentication token

Run: python 11_event_streams.py [--prefix PREFIX] [--cleanup]
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

from turbobulk_client import TurboBulkClient


def write_jsonl(data: list, path: Path):
    """Write data to gzipped JSONL file."""
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        for row in data:
            f.write(json.dumps(row) + '\n')


def main():
    parser = argparse.ArgumentParser(
        description='Event Streams Integration Example',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python 11_event_streams.py --prefix evt
    python 11_event_streams.py --prefix evt --cleanup

Environment variables:
    NETBOX_URL    - NetBox server URL (e.g., https://your-instance.cloud.netboxapp.com)
    NETBOX_TOKEN  - Authentication token
        """
    )
    parser.add_argument('--prefix', default='evt', help='Name prefix for test objects')
    parser.add_argument('--cleanup', action='store_true', help='Clean up test objects')
    args = parser.parse_args()

    client = TurboBulkClient()

    if args.cleanup:
        cleanup(client, args.prefix)
        return

    run_examples(client, args.prefix)


def run_examples(client: TurboBulkClient, prefix: str):
    """Run the event streams examples."""
    print("=" * 60)
    print("TurboBulk Event Streams Integration Example")
    print("=" * 60)
    print()

    # Example 1: Normal load (events dispatched by default)
    print("=" * 60)
    print("Example 1: Normal Load (events enabled by default)")
    print("=" * 60)
    print()
    print("When you load data without specifying dispatch_events, events are")
    print("dispatched through NetBox's event pipeline by default. This means")
    print("any webhooks, event rules, or event stream consumers will see the")
    print("bulk operations just like regular API operations.")
    print()

    # Create 5 sites as row-oriented data
    sites = [
        {'name': f'{prefix}-site-{i}', 'slug': f'{prefix}-site-{i}', 'status': 'active'}
        for i in range(1, 6)
    ]
    path = Path(f'/tmp/{prefix}_sites.jsonl.gz')
    write_jsonl(sites, path)

    print(f"Loading 5 sites from {path}...")
    result = client.load('dcim.site', path)

    if result.get('status') == 'pending':
        print(f"Job submitted: {result.get('job_id')}")
        print("Waiting for completion...")
        result = client.wait_for_job(result['job_id'])

    print(f"Loaded {result.get('data', {}).get('rows_inserted', 0)} sites")

    # Check for event dispatch job
    event_job_id = result.get('data', {}).get('event_dispatch_job_id')
    if event_job_id:
        print(f"Event dispatch job: {event_job_id}")
        print("  -> Events will be dispatched asynchronously to EVENTS_PIPELINE")
    else:
        print("No event dispatch job ID found (events may be disabled in config)")

    print()

    # Example 2: Initial load with events disabled
    print("=" * 60)
    print("Example 2: Initial Load (events disabled for performance)")
    print("=" * 60)
    print()
    print("For large initial data loads (10k+ objects), you may want to disable")
    print("event dispatch to avoid overwhelming event consumers. Use the")
    print("dispatch_events=False parameter:")
    print()

    # Create 100 sites
    bulk_sites = [
        {'name': f'{prefix}-bulk-{i}', 'slug': f'{prefix}-bulk-{i}', 'status': 'active'}
        for i in range(1, 101)
    ]
    bulk_path = Path(f'/tmp/{prefix}_bulk.jsonl.gz')
    write_jsonl(bulk_sites, bulk_path)

    print(f"Loading 100 sites with dispatch_events=False...")
    result = client.load('dcim.site', bulk_path, dispatch_events=False)

    if result.get('status') == 'pending':
        print(f"Job submitted: {result.get('job_id')}")
        result = client.wait_for_job(result['job_id'])

    print(f"Loaded {result.get('data', {}).get('rows_inserted', 0)} sites")
    event_job_id = result.get('data', {}).get('event_dispatch_job_id')
    print(f"Event dispatch job: {event_job_id or 'None (events disabled)'}")
    print()

    # Example 3: Configuration options
    print("=" * 60)
    print("Example 3: Configuration Options")
    print("=" * 60)
    print()
    print("TurboBulk provides several configuration options for event dispatch:")
    print()
    print("PLUGINS_CONFIG = {")
    print("    'netbox_turbobulk': {")
    print("        'dispatch_events': True,    # Enable/disable globally")
    print("        'events_chunk_size': 1000,  # Objects per serialization batch")
    print("    }")
    print("}")
    print()
    print("Per-request overrides:")
    print("  - dispatch_events=True   -> Always dispatch events")
    print("  - dispatch_events=False  -> Never dispatch events")
    print("  - dispatch_events=None   -> Use global config (default)")
    print()

    # Example 4: Recommended workflow
    print("=" * 60)
    print("Example 4: Recommended Workflow")
    print("=" * 60)
    print()
    print("For optimal performance and event handling:")
    print()
    print("1. Initial data migration:")
    print("   client.load('dcim.device', 'migration.jsonl.gz', dispatch_events=False)")
    print()
    print("2. Ongoing bulk updates:")
    print("   client.load('dcim.device', 'updates.jsonl.gz')  # events enabled")
    print()
    print("3. If event consumer is overloaded:")
    print("   - Temporarily set dispatch_events: False in plugin config")
    print("   - Or use dispatch_events=False per request")
    print()

    print("=" * 60)
    print("Event Streams Example Complete")
    print("=" * 60)


def cleanup(client: TurboBulkClient, prefix: str):
    """Clean up test objects using delete_by_filter."""
    print(f"Cleaning up objects with prefix '{prefix}'...")

    try:
        # Use delete_by_filter for simple cleanup
        result = client.delete_by_filter(
            'dcim.site',
            {'slug__startswith': prefix},
            dispatch_events=False  # Skip events for cleanup
        )

        if result.get('status') == 'pending':
            result = client.wait_for_job(result['job_id'])

        deleted = result.get('data', {}).get('rows_deleted', 0)
        if deleted:
            print(f"Deleted {deleted} sites")
        else:
            print("No objects to clean up")

    except Exception as e:
        print(f"Cleanup failed: {e}")


if __name__ == '__main__':
    main()
