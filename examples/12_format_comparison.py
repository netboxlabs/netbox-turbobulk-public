#!/usr/bin/env python3
"""
Example 12: JSONL vs Parquet Format Comparison

Demonstrates both data formats supported by TurboBulk:
- JSONL (JSON Lines): Row-oriented, easy to create, default format
- Parquet: Columnar, compact, high performance

Shows how to:
- Create data in both formats
- Load using each format
- Compare file sizes and performance
- Choose the right format for your use case

Run: python 12_format_comparison.py [--count N] [--cleanup]
"""

import argparse
import sys
import time
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from common.parquet_utils import create_jsonl, create_parquet, read_jsonl, read_parquet
from turbobulk_client import TurboBulkClient


def generate_site_data(count: int, prefix: str) -> list:
    """Generate site data as list of row dicts."""
    return [
        {
            'name': f'{prefix}-SITE-{i:05d}',
            'slug': f'{prefix}-site-{i:05d}',
            'status': 'active',
            'description': f'Test site {i} for format comparison',
        }
        for i in range(1, count + 1)
    ]


def main():
    parser = argparse.ArgumentParser(description='Compare JSONL vs Parquet formats')
    parser.add_argument('--count', type=int, default=1000, help='Number of rows')
    parser.add_argument('--prefix', default='fmt', help='Data prefix')
    parser.add_argument('--cleanup', action='store_true', help='Cleanup only')
    parser.add_argument('--skip-cleanup', action='store_true', help='Skip cleanup after test')
    args = parser.parse_args()

    # TurboBulkClient reads NETBOX_URL and NETBOX_TOKEN from environment
    client = TurboBulkClient()

    if args.cleanup:
        cleanup(client, args.prefix)
        return

    print(f"\n{'='*70}")
    print(f"FORMAT COMPARISON: JSONL vs Parquet ({args.count:,} rows)")
    print(f"{'='*70}\n")

    # Generate row-oriented data (same for both formats)
    print("Generating test data...")
    rows = generate_site_data(args.count, args.prefix)

    # =========================================================================
    # JSONL Format (Default)
    # =========================================================================
    print("\n--- JSONL Format (Default) ---")

    # Create JSONL file (gzipped)
    jsonl_path = create_jsonl(rows, Path(f'/tmp/{args.prefix}_sites'), compress=True)
    jsonl_size = jsonl_path.stat().st_size
    print(f"File: {jsonl_path.name}")
    print(f"Size: {jsonl_size:,} bytes ({jsonl_size / 1024:.1f} KB)")

    # Load via TurboBulk
    print("Loading JSONL data...")
    start = time.time()
    result = client.load('dcim.site', jsonl_path)
    jsonl_duration = time.time() - start

    if result.get('status') == 'success' or result.get('job_id'):
        rows_per_sec = args.count / jsonl_duration if jsonl_duration > 0 else 0
        print(f"Load time: {jsonl_duration:.2f}s ({rows_per_sec:.0f} rows/sec)")
    else:
        print(f"Load failed: {result}")
        jsonl_duration = None

    # Cleanup JSONL test data using export + delete
    print("Cleaning up JSONL test data...")
    cleanup_result = client.export(
        'dcim.site',
        filters={'slug__startswith': args.prefix},
        fields=['id'],
        output_path=Path(f'/tmp/{args.prefix}_cleanup.jsonl.gz'),
        verbose=False,
    )
    if cleanup_result.get('path'):
        client.delete('dcim.site', cleanup_result['path'], verbose=False)

    # =========================================================================
    # Parquet Format (High Performance)
    # =========================================================================
    print("\n--- Parquet Format (High Performance) ---")

    # Convert rows to column-oriented format for Parquet
    columns = {
        'name': [r['name'] for r in rows],
        'slug': [r['slug'] for r in rows],
        'status': [r['status'] for r in rows],
        'description': [r['description'] for r in rows],
    }

    # Create Parquet file
    parquet_path = create_parquet(columns, Path(f'/tmp/{args.prefix}_sites.parquet'))
    parquet_size = parquet_path.stat().st_size
    print(f"File: {parquet_path.name}")
    print(f"Size: {parquet_size:,} bytes ({parquet_size / 1024:.1f} KB)")

    # Load via TurboBulk
    print("Loading Parquet data...")
    start = time.time()
    result = client.load('dcim.site', parquet_path)
    parquet_duration = time.time() - start

    if result.get('status') == 'success' or result.get('job_id'):
        rows_per_sec = args.count / parquet_duration if parquet_duration > 0 else 0
        print(f"Load time: {parquet_duration:.2f}s ({rows_per_sec:.0f} rows/sec)")
    else:
        print(f"Load failed: {result}")
        parquet_duration = None

    # =========================================================================
    # Comparison Summary
    # =========================================================================
    print(f"\n{'='*70}")
    print("COMPARISON SUMMARY")
    print(f"{'='*70}")
    print(f"{'Metric':<25} {'JSONL (gzip)':<20} {'Parquet':<20}")
    print("-" * 65)
    print(f"{'File size':<25} {jsonl_size:,} bytes{'':<8} {parquet_size:,} bytes")

    if parquet_size > 0:
        ratio = jsonl_size / parquet_size
        smaller = "Parquet" if ratio > 1 else "JSONL"
        ratio_val = ratio if ratio > 1 else 1/ratio
        print(f"{'Size comparison':<25} {smaller} is {ratio_val:.1f}x smaller")

    if jsonl_duration and parquet_duration:
        print(f"{'Load time':<25} {jsonl_duration:.2f}s{'':<14} {parquet_duration:.2f}s")
        print(f"{'Throughput':<25} {args.count/jsonl_duration:,.0f} rows/sec{'':<6} {args.count/parquet_duration:,.0f} rows/sec")

    print(f"\n{'Feature comparison':<25} {'JSONL':<20} {'Parquet':<20}")
    print("-" * 65)
    print(f"{'Ease of creation':<25} {'Very easy':<20} {'Requires PyArrow':<20}")
    print(f"{'Language support':<25} {'Universal (JSON)':<20} {'Python, Java, Go...':<20}")
    print(f"{'Data orientation':<25} {'Row-oriented':<20} {'Column-oriented':<20}")
    print(f"{'Human readable':<25} {'Yes':<20} {'No':<20}")
    print(f"{'Compression':<25} {'gzip (automatic)':<20} {'Snappy (built-in)':<20}")
    print(f"{'='*70}")

    # =========================================================================
    # Recommendation
    # =========================================================================
    print("\nRECOMMENDATION:")
    print("  JSONL (Default): Easy integration, any language, moderate datasets")
    print("  Parquet: Maximum performance, large/repeated operations, Python/Java")
    print()

    # Cleanup
    if not args.skip_cleanup:
        cleanup(client, args.prefix)
    else:
        print(f"\nSkipping cleanup. Run with --cleanup to remove test data.")


def cleanup(client, prefix):
    """Clean up test data."""
    print(f"\nCleaning up {prefix}-* sites...")
    try:
        # Export IDs first, then delete
        export_result = client.export(
            'dcim.site',
            filters={'slug__startswith': prefix},
            fields=['id'],
            output_path=Path(f'/tmp/{prefix}_cleanup.jsonl.gz'),
            verbose=False,
        )
        if export_result.get('path'):
            result = client.delete('dcim.site', export_result['path'], verbose=False)
            print(f"Cleanup complete")
        else:
            print("No sites found to clean up")
    except Exception as e:
        print(f"Cleanup error (may be OK if no data exists): {e}")


if __name__ == '__main__':
    main()
