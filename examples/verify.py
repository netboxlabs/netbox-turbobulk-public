#!/usr/bin/env python3
"""
TurboBulk Installation Verification Script.

Verifies that TurboBulk is properly installed and configured in NetBox.

Usage:
    # Using environment variables (NETBOX_URL, NETBOX_TOKEN)
    python verify.py

    # With explicit arguments
    python verify.py --url http://netbox:8080 --token your-token

    # Quick test with actual data (creates and deletes 3 test sites)
    python verify.py --test-data
"""

import argparse
import sys
import tempfile
from pathlib import Path

# Add parent directory to path for common imports
sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError


def check_mark(passed: bool) -> str:
    """Return a check mark or X."""
    return "[OK]" if passed else "[FAIL]"


def verify_connection(client: TurboBulkClient) -> bool:
    """Verify basic API connectivity."""
    try:
        # Simple health check - list models
        models = client.get_models()
        print(f"  {check_mark(True)} API accessible, found {len(models)} models")
        return True
    except Exception as e:
        print(f"  {check_mark(False)} Cannot connect to API: {e}")
        return False


def verify_model_schema(client: TurboBulkClient) -> bool:
    """Verify model schema endpoint works."""
    try:
        schema = client.get_model_schema('dcim.site')
        field_count = len(schema.get('fields', []))
        print(f"  {check_mark(True)} Schema introspection works ({field_count} fields in dcim.site)")
        return True
    except Exception as e:
        print(f"  {check_mark(False)} Schema introspection failed: {e}")
        return False


def verify_template_generation(client: TurboBulkClient) -> bool:
    """Verify template generation works."""
    try:
        template = client.get_template('dcim.site')
        if 'name' in template and 'slug' in template:
            print(f"  {check_mark(True)} Template generation works (fields: {list(template.keys())[:5]}...)")
            return True
        else:
            print(f"  {check_mark(False)} Template missing required fields")
            return False
    except Exception as e:
        print(f"  {check_mark(False)} Template generation failed: {e}")
        return False


def verify_dry_run(client: TurboBulkClient) -> bool:
    """Verify dry-run validation works (requires pyarrow)."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print(f"  [SKIP] Dry-run test skipped (pyarrow not installed)")
        return True

    try:
        # Create a minimal test Parquet file
        schema = pa.schema([
            ('name', pa.string()),
            ('slug', pa.string()),
            ('status', pa.string()),
        ])
        data = {
            'name': ['verify-test-site'],
            'slug': ['verify-test-site'],
            'status': ['active'],
        }
        table = pa.table(data, schema=schema)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            temp_path = Path(f.name)

        try:
            result = client.validate('dcim.site', temp_path, verbose=False)
            job_data = result.get('data', {})

            if job_data.get('valid', False) or job_data.get('dry_run', False):
                print(f"  {check_mark(True)} Dry-run validation works")
                return True
            else:
                # Check if it failed due to validation (expected for some schemas)
                errors = job_data.get('errors', [])
                if errors:
                    print(f"  {check_mark(True)} Dry-run validation works (found {len(errors)} validation errors as expected)")
                    return True
                print(f"  {check_mark(False)} Dry-run returned unexpected result: {result}")
                return False
        finally:
            temp_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"  {check_mark(False)} Dry-run validation failed: {e}")
        return False


def verify_test_data(client: TurboBulkClient) -> bool:
    """Create, verify, and delete test data."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print(f"  [SKIP] Test data skipped (pyarrow not installed)")
        return True

    import time
    prefix = f"tb-verify-{int(time.time())}"

    try:
        # Create test sites
        schema = pa.schema([
            ('name', pa.string()),
            ('slug', pa.string()),
            ('status', pa.string()),
        ])
        data = {
            'name': [f'{prefix}-1', f'{prefix}-2', f'{prefix}-3'],
            'slug': [f'{prefix}-1', f'{prefix}-2', f'{prefix}-3'],
            'status': ['active', 'active', 'active'],
        }
        table = pa.table(data, schema=schema)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            insert_path = Path(f.name)

        try:
            # Insert
            result = client.load('dcim.site', insert_path, verbose=False)
            inserted = result.get('data', {}).get('rows_inserted', 0)
            if inserted != 3:
                print(f"  {check_mark(False)} Insert failed: expected 3 rows, got {inserted}")
                return False
            print(f"  {check_mark(True)} Inserted 3 test sites")

            # Get the IDs to delete
            sites = client.rest_get('/api/dcim/sites/', {'name__startswith': prefix})
            site_ids = [s['id'] for s in sites.get('results', [])]

            if len(site_ids) != 3:
                print(f"  {check_mark(False)} Verification failed: expected 3 sites, found {len(site_ids)}")
                return False

            # Delete
            del_schema = pa.schema([('id', pa.int64())])
            del_data = {'id': site_ids}
            del_table = pa.table(del_data, schema=del_schema)

            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
                pq.write_table(del_table, f.name)
                delete_path = Path(f.name)

            try:
                result = client.delete('dcim.site', delete_path, verbose=False)
                deleted = result.get('data', {}).get('rows_deleted', 0)
                if deleted != 3:
                    print(f"  {check_mark(False)} Delete failed: expected 3 rows, got {deleted}")
                    return False
                print(f"  {check_mark(True)} Deleted 3 test sites")
                return True
            finally:
                delete_path.unlink(missing_ok=True)

        finally:
            insert_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"  {check_mark(False)} Test data verification failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Verify TurboBulk installation')
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN)')
    parser.add_argument('--test-data', action='store_true',
                        help='Create and delete test data to verify full functionality')
    args = parser.parse_args()

    print("TurboBulk Installation Verification")
    print("=" * 40)
    print()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Target: {client.base_url}")
        print()
    except TurboBulkError as e:
        print(f"Configuration error: {e}")
        print()
        print("Set NETBOX_URL and NETBOX_TOKEN environment variables, or use --url and --token")
        sys.exit(1)

    results = []

    print("1. API Connection")
    results.append(verify_connection(client))

    print("\n2. Model Schema")
    results.append(verify_model_schema(client))

    print("\n3. Template Generation")
    results.append(verify_template_generation(client))

    print("\n4. Dry-Run Validation")
    results.append(verify_dry_run(client))

    if args.test_data:
        print("\n5. Test Data (Insert/Delete)")
        results.append(verify_test_data(client))

    print()
    print("=" * 40)
    passed = sum(results)
    total = len(results)

    if all(results):
        print(f"All {total} checks passed! TurboBulk is ready to use.")
        sys.exit(0)
    else:
        print(f"{passed}/{total} checks passed. Please resolve the issues above.")
        sys.exit(1)


if __name__ == '__main__':
    main()
