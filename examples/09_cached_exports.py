#!/usr/bin/env python3
"""
TurboBulk Export Caching Demonstration

This example demonstrates how to use TurboBulk's export caching feature
for efficient repeated exports.

Key concepts:
1. Cache HIT: When data hasn't changed, get cached file instantly (HTTP 200)
2. Cache MISS: When data changed or first request, new job created (HTTP 202)
3. Force refresh: Bypass cache to get fresh data
4. Check-only mode: Verify cache status without creating jobs
5. Client-side caching: Avoid re-downloading unchanged files by sending back
   the cache_key AND cache_version of the copy you hold (HTTP 304)

Use cases:
- Scheduled sync jobs that need to export data regularly
- Analytics pipelines where data changes infrequently
- Multi-system integrations with polling-based updates

Usage:
    python 09_cached_exports.py --url http://netbox:8080 --token YOUR_TOKEN

Prerequisites:
    - Some devices must exist in NetBox
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError


def main():
    parser = argparse.ArgumentParser(
        description='TurboBulk Export Caching Demonstration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--model', default='dcim.device', help='Model to export (default: dcim.device)')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # ============================================================
        # Step 1: First export (cache miss - creates new job)
        # ============================================================
        print("\n" + "="*60)
        print("STEP 1: First Export (Cache Miss)")
        print("="*60)

        start = time.time()
        result1 = export_with_cache_info(client, args.model)
        duration1 = time.time() - start

        print(f"Result: {'CACHED' if result1.get('cached') else 'NEW JOB'}")
        print(f"Duration: {duration1:.2f}s")
        if result1.get('cache_key'):
            print(f"Cache key: {result1['cache_key'][:32]}...")
        if result1.get('cache_version'):
            print(f"Cache version: {result1['cache_version']}")
        if result1.get('row_count'):
            print(f"Rows: {result1['row_count']}")

        # Store the cache key AND version of the copy we now hold. The key
        # names the request (it never changes between rebuilds); the version
        # names this particular build. A 304 needs both.
        cache_key = result1.get('cache_key')
        cache_version = result1.get('cache_version')

        # ============================================================
        # Step 2: Second export (cache hit - instant response)
        # ============================================================
        print("\n" + "="*60)
        print("STEP 2: Second Export (Cache Hit Expected)")
        print("="*60)

        start = time.time()
        result2 = export_with_cache_info(client, args.model)
        duration2 = time.time() - start

        print(f"Result: {'CACHED' if result2.get('cached') else 'NEW JOB'}")
        print(f"Duration: {duration2:.2f}s")

        if result2.get('cached'):
            print(f"Speed improvement: {duration1 / max(duration2, 0.001):.1f}x faster!")
            # The cache hit is the copy we hold now: keep its key and version.
            cache_key = result2.get('cache_key', cache_key)
            cache_version = result2.get('cache_version', cache_version)
        else:
            print("Note: Cache miss may indicate data changed between requests")

        # ============================================================
        # Step 3: Check cache status without creating job
        # ============================================================
        print("\n" + "="*60)
        print("STEP 3: Check Cache Status Only")
        print("="*60)

        status = check_cache_status(client, args.model)
        print(f"Cache status: {'VALID' if status.get('cached') else 'INVALID/MISSING'}")
        if status.get('data_changed'):
            print("Data has changed since last cache")

        # ============================================================
        # Step 4: Send back client_cache_key + client_cache_version for 304
        # ============================================================
        if cache_key:
            print("\n" + "="*60)
            print("STEP 4: Client Cache Validation (304 Response)")
            print("="*60)

            if not cache_version:
                # Servers older than cache_version support don't return it.
                # They still answer 304 on the key alone.
                print("Server did not return a cache_version (older server); sending key only")

            result304 = check_client_cache(client, args.model, cache_key, cache_version)
            if result304.get('status_code') == 304:
                print("Server returned 304 Not Modified - client cache is current!")
                print("No need to re-download the file.")
            elif result304.get('cached'):
                print("Cache valid but our copy is not the current build - re-download")
                print(f"Current cache version: {result304.get('cache_version', 'N/A')}")
            else:
                print("Cache invalid - would need to create new job")

        # ============================================================
        # Step 5: Force refresh to bypass cache
        # ============================================================
        print("\n" + "="*60)
        print("STEP 5: Force Refresh (Bypass Cache)")
        print("="*60)

        start = time.time()
        result_fresh = export_with_force_refresh(client, args.model)
        duration_fresh = time.time() - start

        print(f"Result: {'CACHED' if result_fresh.get('cached') else 'NEW JOB'}")
        print(f"Duration: {duration_fresh:.2f}s")
        if result_fresh.get('job_id'):
            print(f"New job created: {result_fresh['job_id']}")

        # ============================================================
        # Summary
        # ============================================================
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"First export (cold):      {duration1:.2f}s")
        print(f"Second export (cached):   {duration2:.2f}s")
        print(f"Force refresh:            {duration_fresh:.2f}s")

        if duration2 < duration1 / 2:
            print(f"\nCaching provided {duration1 / max(duration2, 0.001):.1f}x speedup!")

    except TurboBulkError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def export_with_cache_info(client, model):
    """
    Export data and return cache information.

    This demonstrates the standard export flow with caching.
    """
    url = f"{client.base_url}/api/plugins/turbobulk/export/"
    data = {'model': model}

    # Use client.session which already has correct auth headers
    response = client.session.post(url, json=data)
    result = response.json() if response.content else {}
    result['status_code'] = response.status_code

    # If it's a cache hit (200), we have the result immediately
    if response.status_code == 200 and result.get('cached'):
        return result

    # If it's a job (202), wait for completion
    if response.status_code == 202 and result.get('job_id'):
        # Poll for job completion
        job_id = result['job_id']
        status_url = f"{client.base_url}/api/plugins/turbobulk/jobs/{job_id}/"

        for _ in range(60):  # Max 60 seconds
            time.sleep(1)
            status_response = client.session.get(status_url)
            status = status_response.json()

            if status.get('status') == 'completed':
                job_data = status.get('data') or {}
                result['row_count'] = job_data.get('rows_exported', 0)
                result['file_path'] = job_data.get('file_path')
                # When the server cached this build (cache_stored is true), the
                # job's data names it: keep cache_key + cache_version so a
                # later request can earn a 304. Older servers omit these.
                if job_data.get('cache_stored'):
                    result['cache_key'] = job_data.get('cache_key')
                    result['cache_version'] = job_data.get('cache_version')
                return result
            elif status.get('status') == 'errored':
                raise TurboBulkError(f"Job failed: {status.get('error')}")

    return result


def check_cache_status(client, model):
    """
    Check cache status without creating a job.

    Uses check_cache_only=true to only verify if a valid cache exists.
    """
    url = f"{client.base_url}/api/plugins/turbobulk/export/"
    data = {
        'model': model,
        'check_cache_only': True,
    }

    response = client.session.post(url, json=data)
    result = response.json() if response.content else {}
    result['status_code'] = response.status_code
    return result


def check_client_cache(client, model, client_cache_key, client_cache_version=None):
    """
    Check if client's cached copy is still current.

    Send back both the cache_key and the cache_version of the copy you hold.
    If the server returns 304, the client's local file is still valid. A key
    without its version gets the cached file again (200), never a 304, on
    servers that support cache_version. Older servers ignore the version.

    The same thing through the client library:
        client.check_export_cache(model, client_cache_key=..., client_cache_version=...)
    """
    url = f"{client.base_url}/api/plugins/turbobulk/export/"
    data = {
        'model': model,
        'client_cache_key': client_cache_key,
    }
    if client_cache_version:
        data['client_cache_version'] = client_cache_version

    response = client.session.post(url, json=data)
    result = response.json() if response.content else {}
    result['status_code'] = response.status_code
    return result


def export_with_force_refresh(client, model):
    """
    Export with force_refresh to bypass cache.

    This always creates a new export job regardless of cache status.
    """
    url = f"{client.base_url}/api/plugins/turbobulk/export/"
    data = {
        'model': model,
        'force_refresh': True,
    }

    response = client.session.post(url, json=data)
    result = response.json() if response.content else {}
    result['status_code'] = response.status_code
    return result


if __name__ == '__main__':
    main()
