#!/usr/bin/env python3
"""
TurboBulk Hello World - Bulk Insert 10 Sites

This is the simplest TurboBulk example. It demonstrates:
- Creating a TurboBulkClient
- Generating a JSONL file with site data
- Submitting a bulk insert job
- Waiting for job completion

Usage:
    python 01_hello_turbobulk.py --url https://your-instance.cloud.netboxapp.com --token YOUR_TOKEN

    Or set environment variables:
    export NETBOX_URL=https://your-instance.cloud.netboxapp.com
    export NETBOX_TOKEN=nbt_your-api-token
    python 01_hello_turbobulk.py

Prerequisites:
    - NetBox Cloud or NetBox Enterprise with TurboBulk enabled
    - API token with dcim.add_site permission
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk Hello World - Bulk insert 10 sites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--prefix', default='hello', help='Site name prefix (default: hello)')
    parser.add_argument('--count', type=int, default=10, help='Number of sites (default: 10)')
    args = parser.parse_args()

    try:
        # Create client (uses env vars if --url/--token not provided)
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # Generate site data
        print(f"\nGenerating {args.count} sites with prefix '{args.prefix}'...")
        sites = [
            {
                'name': f'{args.prefix}-dc-{i:02d}',
                'slug': f'{args.prefix}-dc-{i:02d}',
                'status': 'active',
                'description': f'Hello TurboBulk site {i}',
            }
            for i in range(1, args.count + 1)
        ]

        # Create JSONL file (gzipped)
        jsonl_path = Path('/tmp/hello_sites.jsonl.gz')
        with gzip.open(jsonl_path, 'wt', encoding='utf-8') as f:
            for site in sites:
                f.write(json.dumps(site) + '\n')
        print(f"Created JSONL file: {jsonl_path}")

        # Submit bulk load job
        print(f"\nSubmitting bulk insert job...")
        result = client.load(
            model='dcim.site',
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
        print(f"Duration:     {result.get('duration_seconds', 'N/A')}s")
        print(f"\nView your sites at: {client.base_url}/dcim/sites/?q={args.prefix}")

    except TurboBulkError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except JobFailedError as e:
        print(f"\nJob failed: {e}", file=sys.stderr)
        print(f"Details: {e.job_result}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
