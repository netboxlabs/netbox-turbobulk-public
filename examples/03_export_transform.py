#!/usr/bin/env python3
"""
TurboBulk Export-Transform-Reimport (ETL) Workflow

This example demonstrates a common ETL pattern:
1. Export existing data from NetBox to JSONL
2. Transform the data (modify, enrich, clean)
3. Reimport using upsert to update existing records

Use cases:
- Bulk status updates
- Adding custom field data to existing objects
- Migrating data between NetBox instances
- Syncing from external systems

Usage:
    python 03_export_transform.py --url https://your-instance.cloud.netboxapp.com --token YOUR_TOKEN

Prerequisites:
    - Some sites must exist in NetBox
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError


def read_jsonl(path: Path) -> list:
    """Read JSONL file (auto-detects gzip by magic bytes, not extension)."""
    # Check magic bytes to detect actual gzip
    with open(path, 'rb') as f:
        magic = f.read(2)

    if magic == b'\x1f\x8b':  # gzip magic bytes
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            return [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f if line.strip()]


def write_jsonl(data: list, path: Path):
    """Write data to gzipped JSONL file."""
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        for row in data:
            f.write(json.dumps(row) + '\n')


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk ETL Workflow - Export, Transform, Reimport',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--filter-prefix', help='Only process sites with this name prefix')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # ============================================================
        # Step 1: EXPORT - Get current data from NetBox
        # ============================================================
        print("\n" + "="*60)
        print("STEP 1: EXPORT - Fetching site data from NetBox")
        print("="*60)

        # Build filters if prefix specified
        filters = {}
        if args.filter_prefix:
            filters['name__startswith'] = args.filter_prefix
            print(f"Filtering sites starting with: {args.filter_prefix}")

        # Export to JSONL (default format)
        result = client.export(
            model='dcim.site',
            filters=filters if filters else None,
            fields=['id', 'name', 'slug', 'status', 'description', 'custom_field_data'],
            output_path=Path('/tmp/sites_export.jsonl.gz'),
            verbose=True,
        )

        # Read the exported data
        export_path = result.get('path')
        data = read_jsonl(export_path)
        row_count = len(data)
        print(f"Exported {row_count} sites to {export_path}")

        if row_count == 0:
            print("No sites found to process. Create some sites first or adjust the filter.")
            return

        # Show sample of exported data
        print("\nSample of exported data:")
        for row in data[:3]:
            print(f"  {row['name']}: status={row['status']}")

        # ============================================================
        # Step 2: TRANSFORM - Modify the data
        # ============================================================
        print("\n" + "="*60)
        print("STEP 2: TRANSFORM - Modifying data")
        print("="*60)

        # Example transformations:
        print("Transforming records...")
        for row in data:
            # First, remove internal fields (start with underscore) that shouldn't be reimported
            internal_fields = [k for k in row.keys() if k.startswith('_')]
            for field in internal_fields:
                del row[field]

            # 2a. Add/update custom field data
            cf = row.get('custom_field_data') or {}
            if isinstance(cf, str):
                try:
                    cf = json.loads(cf)
                except json.JSONDecodeError:
                    cf = {}
            cf['etl_processed'] = True
            cf['etl_source'] = 'turbobulk-example'
            row['custom_field_data'] = cf

            # 2b. Update descriptions
            desc = row.get('description') or ''
            if desc:
                row['description'] = f"{desc} [ETL processed]"
            else:
                row['description'] = "[ETL processed]"

            # 2c. Could also: change status, add tags, modify other fields

        print(f"Transformed {row_count} records")

        # ============================================================
        # Step 3: REIMPORT - Upsert back to NetBox
        # ============================================================
        print("\n" + "="*60)
        print("STEP 3: REIMPORT - Upserting changes to NetBox")
        print("="*60)

        # Write transformed data to new JSONL file
        reimport_path = Path('/tmp/sites_reimport.jsonl.gz')
        write_jsonl(data, reimport_path)
        print(f"Created reimport file: {reimport_path}")

        # Upsert using 'id' as conflict field (default)
        # This will update existing records matching by ID
        print("Submitting upsert job...")
        result = client.load(
            model='dcim.site',
            data_path=reimport_path,
            mode='upsert',
            conflict_fields=['id'],  # Match on primary key
            verbose=True,
        )

        # ============================================================
        # Summary
        # ============================================================
        print("\n" + "="*60)
        print("ETL WORKFLOW COMPLETE!")
        print("="*60)
        print(f"Status:       {result.get('status')}")
        print(f"Rows updated: {result.get('data', {}).get('rows_affected', 'N/A')}")
        print(f"Duration:     {result.get('duration_seconds', 'N/A')}s")
        print(f"\nView changes at: {client.base_url}/dcim/sites/")

        # Verify changes by re-exporting (optional - main workflow is complete)
        print("\n" + "-"*40)
        print("Verifying changes...")
        try:
            verify_result = client.export(
                model='dcim.site',
                filters={'id__in': [data[0]['id']]} if data else None,
                fields=['name', 'description', 'custom_field_data'],
                output_path=Path('/tmp/sites_verify.jsonl.gz'),
                verbose=False,
            )
            verify_data = read_jsonl(verify_result.get('path'))
            if verify_data:
                row = verify_data[0]
                print(f"Sample record after ETL:")
                print(f"  Name: {row['name']}")
                print(f"  Description: {row['description']}")
                print(f"  Custom fields: {row.get('custom_field_data')}")
            else:
                print("(Verification export returned no data)")
        except Exception as e:
            print(f"(Verification step skipped: {e})")

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
