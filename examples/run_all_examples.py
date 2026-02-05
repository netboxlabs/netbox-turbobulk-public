#!/usr/bin/env python3
"""
Test Runner for TurboBulk Examples

Runs all examples with proper cleanup between each to ensure idempotency.
Each example uses a unique prefix to avoid conflicts.

Usage:
    python run_all_examples.py --url http://localhost:8000 --token YOUR_TOKEN

    Or set environment variables:
    export NETBOX_URL=http://localhost:8000
    export NETBOX_TOKEN=your-api-token
    python run_all_examples.py
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))

from turbobulk_client import TurboBulkClient, TurboBulkError
from common.parquet_utils import create_pk_parquet, read_parquet


def get_timestamp_prefix():
    """Generate a unique prefix based on timestamp."""
    return f"ex{int(time.time()) % 100000:05d}"


class ExampleRunner:
    """Runs TurboBulk examples with cleanup."""

    def __init__(self, client: TurboBulkClient, base_prefix: str):
        self.client = client
        self.base_prefix = base_prefix
        self.results = {}

    def cleanup_model(self, model: str, name_filter: dict, verbose: bool = True):
        """Delete all objects matching a filter."""
        model_name = model.split('.')[-1]

        # Export IDs
        try:
            export_path = self.client.export(
                model=model,
                filters=name_filter,
                fields=['id'],
                output_path=Path(f'/tmp/cleanup_{model_name}.parquet'),
                verbose=False,
            )
            data = read_parquet(export_path)
            ids = data.get('id', [])

            if not ids:
                if verbose:
                    print(f"    No {model_name} to delete")
                return 0

            # Delete
            delete_path = Path(f'/tmp/cleanup_{model_name}_ids.parquet')
            create_pk_parquet(ids, delete_path)
            result = self.client.delete(model, delete_path, verbose=False)
            deleted = result.get('data', {}).get('rows_deleted', len(ids))

            if verbose:
                print(f"    Deleted {deleted} {model_name}")
            return deleted

        except Exception as e:
            if verbose:
                print(f"    Error cleaning {model_name}: {e}")
            return 0

    def cleanup_by_prefix(self, prefix: str, verbose: bool = True):
        """Clean up all objects with a given prefix."""
        if verbose:
            print(f"  Cleaning up prefix '{prefix}'...")

        # Order matters: delete dependent objects first
        # Cables -> Interfaces -> Devices -> Sites

        # 1. Cable terminations (reference cables)
        self.cleanup_model('dcim.cabletermination', {'cable__label__startswith': prefix}, verbose)

        # 2. Cables
        self.cleanup_model('dcim.cable', {'label__startswith': prefix}, verbose)

        # 3. Interfaces (reference devices)
        self.cleanup_model('dcim.interface', {'device__name__startswith': prefix}, verbose)

        # 4. Devices (reference sites)
        self.cleanup_model('dcim.device', {'name__startswith': prefix}, verbose)

        # 5. Sites
        self.cleanup_model('dcim.site', {'name__startswith': prefix}, verbose)
        self.cleanup_model('dcim.site', {'slug__startswith': prefix}, verbose)

        # 6. Device roles (if prefixed)
        self.cleanup_model('dcim.devicerole', {'slug__startswith': prefix}, verbose)

        # 7. Device types (if prefixed)
        self.cleanup_model('dcim.devicetype', {'slug__startswith': prefix}, verbose)

        # 8. Manufacturers (if prefixed)
        self.cleanup_model('dcim.manufacturer', {'slug__startswith': prefix}, verbose)

    def run_example(self, name: str, script: str, args: list, prefix: str = None,
                    skip_pre_cleanup: bool = False, skip_post_cleanup: bool = False,
                    uses_env_vars: bool = False):
        """Run a single example and record the result."""
        print(f"\n{'='*60}")
        print(f"EXAMPLE: {name}")
        print(f"{'='*60}")

        # Use provided prefix or generate one
        example_prefix = prefix or f"{self.base_prefix}-{name[:3]}"

        # Pre-cleanup (unless skipped)
        if not skip_pre_cleanup:
            print(f"\n  Pre-cleanup for prefix '{example_prefix}'...")
            self.cleanup_by_prefix(example_prefix, verbose=False)

        # Build command with environment variables
        env = os.environ.copy()
        env['NETBOX_URL'] = self.client.base_url
        env['NETBOX_TOKEN'] = self.client.token

        # Build command
        cmd = [sys.executable, script]

        # Add URL/token only if script accepts them (not uses_env_vars)
        if not uses_env_vars:
            cmd.extend(['--url', self.client.base_url, '--token', self.client.token])

        cmd.extend(args)

        print(f"\n  Running: {Path(script).name} {' '.join(args)}")
        print(f"  Prefix: {example_prefix}")
        print("-" * 40)

        start_time = time.time()
        try:
            result = subprocess.run(
                cmd,
                cwd=Path(__file__).parent,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
                env=env,
            )
            duration = time.time() - start_time

            if result.returncode == 0:
                print(result.stdout)
                self.results[name] = {'status': 'PASS', 'duration': duration}
                print(f"\n  Result: PASS ({duration:.1f}s)")
            else:
                print(result.stdout)
                print(result.stderr, file=sys.stderr)
                self.results[name] = {'status': 'FAIL', 'duration': duration, 'error': result.stderr}
                print(f"\n  Result: FAIL ({duration:.1f}s)")

        except subprocess.TimeoutExpired:
            self.results[name] = {'status': 'TIMEOUT', 'duration': 120}
            print(f"\n  Result: TIMEOUT (>120s)")
        except Exception as e:
            duration = time.time() - start_time
            self.results[name] = {'status': 'ERROR', 'duration': duration, 'error': str(e)}
            print(f"\n  Result: ERROR - {e}")

        # Post-cleanup (unless skipped)
        if not skip_post_cleanup:
            print(f"\n  Post-cleanup...")
            self.cleanup_by_prefix(example_prefix, verbose=False)

        return self.results[name]['status'] == 'PASS'

    def print_summary(self):
        """Print summary of all results."""
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")

        passed = sum(1 for r in self.results.values() if r['status'] == 'PASS')
        total = len(self.results)

        for name, result in self.results.items():
            status = result['status']
            duration = result.get('duration', 0)
            icon = '✓' if status == 'PASS' else '✗'
            print(f"  {icon} {name}: {status} ({duration:.1f}s)")

        print(f"\n  Total: {passed}/{total} passed")
        return passed == total


def ensure_prerequisites(client: TurboBulkClient):
    """Ensure NetBox has minimum prerequisites for examples."""
    from common.parquet_utils import create_parquet

    print("Checking and creating prerequisites...")

    # Check for manufacturer
    mfrs = client.rest_get('/api/dcim/manufacturers/', {'limit': 1})
    if not mfrs.get('results'):
        print("  Creating manufacturer...")
        data = {
            'name': ['Generic Corp'],
            'slug': ['generic-corp'],
        }
        path = Path('/tmp/prereq_mfr.parquet')
        create_parquet(data, path)
        client.load('dcim.manufacturer', path, verbose=False)
        print("    Created: Generic Corp")
    else:
        print(f"  Manufacturers: OK ({mfrs.get('count', 1)})")

    # Check for device types
    dt = client.rest_get('/api/dcim/device-types/', {'limit': 1})
    if not dt.get('results'):
        print("  Creating device type...")
        # Get first manufacturer ID
        mfrs = client.rest_get('/api/dcim/manufacturers/', {'limit': 1})
        mfr_id = mfrs['results'][0]['id'] if mfrs.get('results') else 1
        data = {
            'manufacturer_id': [mfr_id],  # Use _id suffix with actual ID
            'model': ['Generic Server'],
            'slug': ['generic-server'],
            'u_height': [1],
        }
        path = Path('/tmp/prereq_dt.parquet')
        create_parquet(data, path)
        client.load('dcim.devicetype', path, verbose=False)
        print("    Created: Generic Server")
    else:
        print(f"  Device types: OK ({dt.get('count', 1)})")

    # Check for device roles
    roles = client.rest_get('/api/dcim/device-roles/', {'limit': 1})
    if not roles.get('results'):
        print("  Creating device role...")
        data = {
            'name': ['Server'],
            'slug': ['server'],
            'color': ['ff0000'],
        }
        path = Path('/tmp/prereq_role.parquet')
        create_parquet(data, path)
        client.load('dcim.devicerole', path, verbose=False)
        print("    Created: Server")
    else:
        print(f"  Device roles: OK ({roles.get('count', 1)})")

    # Check for at least one site
    sites = client.rest_get('/api/dcim/sites/', {'limit': 1})
    if not sites.get('results'):
        print("  Creating site...")
        data = {
            'name': ['Main DC'],
            'slug': ['main-dc'],
            'status': ['active'],
        }
        path = Path('/tmp/prereq_site.parquet')
        create_parquet(data, path)
        client.load('dcim.site', path, verbose=False)
        print("    Created: Main DC")
    else:
        print(f"  Sites: OK ({sites.get('count', 1)})")


def main():
    parser = argparse.ArgumentParser(
        description='Run all TurboBulk examples with cleanup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--url', help='NetBox URL (or set NETBOX_URL env var)')
    parser.add_argument('--token', help='API token (or set NETBOX_TOKEN env var)')
    parser.add_argument('--skip', nargs='*', default=[], help='Examples to skip (e.g., 06)')
    parser.add_argument('--only', nargs='*', default=[], help='Only run these examples')
    args = parser.parse_args()

    try:
        client = TurboBulkClient(base_url=args.url, token=args.token)
        print(f"Connected to: {client.base_url}")

        # Check prerequisites
        ensure_prerequisites(client)

        # Create runner with unique prefix
        prefix = get_timestamp_prefix()
        runner = ExampleRunner(client, prefix)

        print(f"\nUsing base prefix: {prefix}")

        # Define examples to run with their configurations
        # Format: (name, script, args, extra_config)
        # extra_config: dict with skip_pre_cleanup, skip_post_cleanup, uses_env_vars, prefix_arg
        examples = [
            # Example 01: Hello sites
            ('01_hello_sites', '01_hello_turbobulk.py',
             ['--prefix', f'{prefix}-01', '--count', '5'], {}),

            # Example 02: Device inventory (include prefix in asset_tag to avoid conflicts)
            ('02_device_inventory', '02_device_inventory.py',
             ['--prefix', f'{prefix}-02', '--count', '20'], {}),

            # Example 03: Export transform (works on existing sites, no prefix needed)
            ('03_export_transform', '03_export_transform.py',
             ['--filter-prefix', f'{prefix}-01'], {}),  # Filter to our test sites

            # Example 04: Interface bulk (uses --device-prefix)
            ('04_interface_bulk', '04_interface_bulk.py',
             ['--device-prefix', f'{prefix}-02', '--device-limit', '5', '--interfaces-per-device', '4'], {}),

            # Example 05: Cable connections (uses --device-prefix)
            ('05_cable_connections', '05_cable_connections.py',
             ['--device-prefix', f'{prefix}-02', '--max-cables', '5'], {}),

            # Example 06: GPU datacenter - run as a sequence without intermediate cleanup
            ('06_gpu_dc_setup', '06_gpu_datacenter_cabling.py',
             ['--prefix', f'{prefix}-gpu', 'setup', '--pods', '1', '--leaves-per-pod', '2',
              '--gpu-servers-per-leaf', '2', '--nics-per-gpu-server', '2'],
             {'skip_post_cleanup': True}),
            ('06_gpu_dc_devices', '06_gpu_datacenter_cabling.py',
             ['--prefix', f'{prefix}-gpu', 'devices', '--pods', '1', '--leaves-per-pod', '2',
              '--gpu-servers-per-leaf', '2', '--nics-per-gpu-server', '2'],
             {'skip_pre_cleanup': True, 'skip_post_cleanup': True}),
            ('06_gpu_dc_status', '06_gpu_datacenter_cabling.py',
             ['--prefix', f'{prefix}-gpu', 'status', '--pods', '1', '--leaves-per-pod', '2',
              '--gpu-servers-per-leaf', '2', '--nics-per-gpu-server', '2'],
             {'skip_pre_cleanup': True, 'skip_post_cleanup': True}),
            ('06_gpu_dc_teardown', '06_gpu_datacenter_cabling.py',
             ['--prefix', f'{prefix}-gpu', 'teardown', '--pods', '1', '--leaves-per-pod', '2',
              '--gpu-servers-per-leaf', '2', '--nics-per-gpu-server', '2'],
             {'skip_pre_cleanup': True}),

            # Example 07: Post hooks (uses env vars, not CLI args for url/token)
            ('07_post_hooks', '07_post_hooks.py',
             ['--prefix', f'{prefix}-hooks'],
             {'uses_env_vars': True}),
            ('07_post_hooks_cleanup', '07_post_hooks.py',
             ['--prefix', f'{prefix}-hooks', '--cleanup'],
             {'uses_env_vars': True, 'skip_pre_cleanup': True}),

            # Example 08: NetBox Branching workflow (requires netbox-branching plugin)
            # Skip the generic pre-cleanup as it doesn't apply to branching
            # The branching example uses its own 'tb-branch' prefix internally
            ('08_branching_workflow', '08_branching_workflow.py',
             [],  # Uses env vars, cleanup handled internally
             {'uses_env_vars': True, 'skip_pre_cleanup': True}),
            ('08_branching_cleanup', '08_branching_workflow.py',
             ['--cleanup'],
             {'uses_env_vars': True, 'skip_pre_cleanup': True}),

            # Example 09: Cached exports (exports existing data, no cleanup needed)
            ('09_cached_exports', '09_cached_exports.py',
             ['--model', 'dcim.site'],
             {'skip_pre_cleanup': True, 'skip_post_cleanup': True}),

            # Example 10: Validation best practices (uses env vars)
            ('10_validation', '10_validation_best_practices.py',
             ['--prefix', f'{prefix}-val'],
             {'uses_env_vars': True}),
            ('10_validation_cleanup', '10_validation_best_practices.py',
             ['--prefix', f'{prefix}-val', '--cleanup'],
             {'uses_env_vars': True, 'skip_pre_cleanup': True}),

            # Example 11: Event streams integration (uses env vars)
            ('11_event_streams', '11_event_streams.py',
             ['--prefix', f'{prefix}-evt'],
             {'uses_env_vars': True}),
            ('11_event_streams_cleanup', '11_event_streams.py',
             ['--prefix', f'{prefix}-evt', '--cleanup'],
             {'uses_env_vars': True, 'skip_pre_cleanup': True}),

            # Example 12: Format comparison (uses env vars)
            ('12_format_comparison', '12_format_comparison.py',
             ['--prefix', f'{prefix}-fmt', '--count', '100'],
             {'uses_env_vars': True}),
        ]

        # Filter examples based on --skip and --only
        if args.only:
            examples = [(n, s, a, c) for n, s, a, c in examples if any(o in n for o in args.only)]
        if args.skip:
            examples = [(n, s, a, c) for n, s, a, c in examples if not any(sk in n for sk in args.skip)]

        if not examples:
            print("No examples to run!")
            return 1

        print(f"\nRunning {len(examples)} example(s)...")

        # Run each example
        for name, script, example_args, config in examples:
            runner.run_example(
                name, script, example_args,
                prefix=prefix,
                skip_pre_cleanup=config.get('skip_pre_cleanup', False),
                skip_post_cleanup=config.get('skip_post_cleanup', False),
                uses_env_vars=config.get('uses_env_vars', False),
            )

        # Print summary
        success = runner.print_summary()

        # Final cleanup
        print(f"\nFinal cleanup...")
        runner.cleanup_by_prefix(f"{prefix}-01", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-02", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-gpu", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-hooks", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-val", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-evt", verbose=True)
        runner.cleanup_by_prefix(f"{prefix}-fmt", verbose=True)

        return 0 if success else 1

    except TurboBulkError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
