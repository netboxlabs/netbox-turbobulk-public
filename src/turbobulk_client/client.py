"""
TurboBulk API Client.

High-level client for TurboBulk bulk data operations.
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import requests

from .exceptions import TurboBulkError, JobFailedError


class TurboBulkClient:
    """
    High-level client for TurboBulk API operations.

    Features:
    - Connection configuration from arguments or environment
    - Automatic job status polling with progress output
    - JSONL and Parquet file upload handling (auto-detected)
    - Error handling with detailed messages

    Example:
        client = TurboBulkClient()  # Uses NETBOX_URL and NETBOX_TOKEN env vars
        result = client.load('dcim.site', 'sites.jsonl.gz')
        print(f"Loaded {result['data']['rows_inserted']} rows")
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        verify_ssl: bool = True,
    ):
        """
        Initialize TurboBulk client.

        Args:
            base_url: NetBox URL (or set NETBOX_URL env var)
            token: API token (or set NETBOX_TOKEN env var)
            verify_ssl: Verify SSL certificates (default True)
        """
        self.base_url = (base_url or os.environ.get("NETBOX_URL", "")).rstrip("/")
        self.token = token or os.environ.get("NETBOX_TOKEN", "")

        if not self.base_url:
            raise TurboBulkError("NetBox URL required (pass base_url or set NETBOX_URL)")
        if not self.token:
            raise TurboBulkError("API token required (pass token or set NETBOX_TOKEN)")

        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        # Detect token version: v2 tokens start with 'nbt_'
        if self.token.startswith("nbt_"):
            self.session.headers["Authorization"] = f"Bearer {self.token}"
        else:
            self.session.headers["Authorization"] = f"Token {self.token}"
        self.session.verify = verify_ssl

        # TurboBulk API base
        self.api_base = f"{self.base_url}/api/plugins/turbobulk"

    def get_models(self) -> List[Dict]:
        """
        List available models with their schemas.

        Returns:
            List of model info dicts with app_label, model_name, etc.
        """
        response = self.session.get(f"{self.api_base}/models/")
        response.raise_for_status()
        return response.json()

    def get_model_schema(self, model: str) -> Dict:
        """
        Get detailed schema for a specific model.

        Args:
            model: Model identifier (e.g., 'dcim.device', 'dcim.site')

        Returns:
            Schema dict with fields, constraints, etc.
        """
        response = self.session.get(f"{self.api_base}/models/{model}/")
        response.raise_for_status()
        return response.json()

    def get_template(self, model: str, include_optional: bool = False) -> Dict[str, Any]:
        """
        Generate a template dict with all required fields for a model.

        Use this to understand what fields are needed before creating a data file.

        Args:
            model: Model identifier (e.g., 'dcim.device', 'dcim.site')
            include_optional: If True, include optional (nullable) fields too

        Returns:
            Dict with field names as keys and example/default values.
            FK fields use the _id suffix format required by TurboBulk.

        Example:
            template = client.get_template('dcim.site')
            # {'name': '', 'slug': '', 'status': 'active', ...}
        """
        schema = self.get_model_schema(model)
        template = {}

        for field in schema.get("fields", []):
            name = field["name"]
            is_pk = field.get("primary_key", False)
            nullable = field.get("nullable", True)
            has_default = field.get("default") is not None
            is_fk = field.get("foreign_key") is not None

            # Skip primary key (auto-generated)
            if is_pk:
                continue

            # For FK fields, use the _id suffix
            if is_fk:
                name = f"{name}_id" if not name.endswith("_id") else name

            # Skip optional fields if not requested
            if nullable and has_default and not include_optional:
                continue

            # Generate appropriate default value based on type
            field_type = field.get("type", "")
            default = field.get("default")

            if default is not None:
                template[name] = default
            elif "Char" in field_type or "Text" in field_type:
                template[name] = ""
            elif "Int" in field_type or "BigInt" in field_type:
                template[name] = 0
            elif "Bool" in field_type:
                template[name] = False
            elif "JSON" in field_type:
                template[name] = {}
            elif "Decimal" in field_type or "Float" in field_type:
                template[name] = 0.0
            else:
                template[name] = None

        return template

    def validate(
        self,
        model: str,
        data_path: Union[str, Path],
        mode: str = "insert",
        conflict_fields: Optional[List[str]] = None,
        validation_mode: str = "auto",
        wait: bool = True,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
    ) -> Dict:
        """
        Validate a data file without committing changes (dry-run).

        This is a convenience wrapper around load(..., dry_run=True).

        Args:
            model: Model identifier (e.g., 'dcim.device')
            data_path: Path to data file (JSONL, JSONL.gz, or Parquet)
            mode: 'insert' or 'upsert'
            conflict_fields: Fields for upsert conflict detection
            validation_mode: 'none', 'auto' (default), or 'full'
            wait: If True, poll until validation completes
            poll_interval: Seconds between status polls
            timeout: Max seconds to wait for completion
            verbose: Print progress messages

        Returns:
            Validation result dict with 'valid', 'rows', 'errors', 'warnings'.
        """
        return self.load(
            model=model,
            data_path=data_path,
            mode=mode,
            conflict_fields=conflict_fields,
            validation_mode=validation_mode,
            dry_run=True,
            wait=wait,
            poll_interval=poll_interval,
            timeout=timeout,
            verbose=verbose,
        )

    def load(
        self,
        model: str,
        data_path: Union[str, Path],
        mode: str = "insert",
        conflict_fields: Optional[List[str]] = None,
        conflict_constraint: Optional[str] = None,
        validation_mode: str = "auto",
        post_hooks: Optional[Dict[str, bool]] = None,
        create_changelogs: bool = True,
        dispatch_events: Optional[bool] = None,
        branch: Optional[str] = None,
        dry_run: bool = False,
        wait: bool = True,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
    ) -> Dict:
        """
        Submit a bulk load (insert/upsert) job.

        Args:
            model: Model identifier (e.g., 'dcim.device')
            data_path: Path to data file (JSONL, JSONL.gz, or Parquet - auto-detected)
            mode: 'insert' or 'upsert'
            conflict_fields: Fields for upsert conflict detection (default: primary key)
            conflict_constraint: Named constraint for expression-based conflicts
                (overrides conflict_fields)
            validation_mode: Validation behavior - 'none', 'auto' (default), or 'full'
            post_hooks: Dict of post-operation hooks to enable/disable
            create_changelogs: Generate ObjectChange records (default: True)
            dispatch_events: Override global event dispatch setting
                (True=dispatch, False=skip, None=use global config)
            branch: Target branch name (requires netbox-branching)
            dry_run: If True, validate data without committing changes
            wait: If True, poll until job completes
            poll_interval: Seconds between status polls
            timeout: Max seconds to wait for completion
            verbose: Print progress messages

        Returns:
            Job result dict with status, rows_affected, duration, etc.
            For dry_run, returns validation result with 'valid', 'rows', 'errors'.

        Raises:
            JobFailedError: If job fails or times out
        """
        data_path = Path(data_path)
        if not data_path.exists():
            raise TurboBulkError(f"Data file not found: {data_path}")

        # Build form data
        data = {
            "model": model,
            "mode": mode,
            "validation_mode": validation_mode,
            "create_changelogs": str(create_changelogs).lower(),
        }
        if conflict_fields:
            data["conflict_fields"] = ",".join(conflict_fields)
        if conflict_constraint:
            data["conflict_constraint"] = conflict_constraint
        if post_hooks:
            import json

            data["post_hooks"] = json.dumps(post_hooks)
        if dispatch_events is not None:
            data["dispatch_events"] = str(dispatch_events).lower()
        if branch:
            data["branch"] = branch
        if dry_run:
            data["dry_run"] = "true"

        # Upload file
        with open(data_path, "rb") as f:
            files = {"file": (data_path.name, f, "application/octet-stream")}
            response = self.session.post(
                f"{self.api_base}/load/",
                data=data,
                files=files,
            )

        response.raise_for_status()
        result = response.json()

        if not wait:
            return result

        # Poll for completion
        job_id = result.get("job_id")
        if not job_id:
            return result

        operation = f"dry-run {mode} {model}" if dry_run else f"{mode} {model}"
        return self._wait_for_job(
            job_id,
            poll_interval=poll_interval,
            timeout=timeout,
            verbose=verbose,
            operation=operation,
        )

    def delete(
        self,
        model: str,
        data_path: Union[str, Path],
        key_fields: Optional[List[str]] = None,
        cascade_nullable_fks: bool = True,
        create_changelogs: bool = True,
        dispatch_events: Optional[bool] = None,
        branch: Optional[str] = None,
        dry_run: bool = False,
        wait: bool = True,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
    ) -> Dict:
        """
        Submit a bulk delete job.

        Args:
            model: Model identifier (e.g., 'dcim.device')
            data_path: Path to data file with keys to delete (JSONL or Parquet)
            key_fields: Key field names (default: primary key)
            cascade_nullable_fks: Clear nullable FK references before delete
            create_changelogs: Generate ObjectChange records (default: True)
            dispatch_events: Override global event dispatch setting
                (True=dispatch, False=skip, None=use global config)
            branch: Target branch name (requires netbox-branching)
            dry_run: If True, validate and count rows without deleting
            wait: If True, poll until job completes
            poll_interval: Seconds between status polls
            timeout: Max seconds to wait for completion
            verbose: Print progress messages

        Returns:
            Job result dict with status, rows_deleted, etc.
            For dry_run, returns validation result with 'valid', 'rows', 'fks_would_nullify'.
        """
        data_path = Path(data_path)
        if not data_path.exists():
            raise TurboBulkError(f"Data file not found: {data_path}")

        data = {
            "model": model,
            "cascade_nullable_fks": str(cascade_nullable_fks).lower(),
            "create_changelogs": str(create_changelogs).lower(),
        }
        if key_fields:
            data["key_fields"] = ",".join(key_fields)
        if dispatch_events is not None:
            data["dispatch_events"] = str(dispatch_events).lower()
        if branch:
            data["branch"] = branch
        if dry_run:
            data["dry_run"] = "true"

        with open(data_path, "rb") as f:
            files = {"file": (data_path.name, f, "application/octet-stream")}
            response = self.session.post(
                f"{self.api_base}/delete/",
                data=data,
                files=files,
            )

        response.raise_for_status()
        result = response.json()

        if not wait:
            return result

        job_id = result.get("job_id")
        if not job_id:
            return result

        operation = f"dry-run delete {model}" if dry_run else f"delete {model}"
        return self._wait_for_job(
            job_id,
            poll_interval=poll_interval,
            timeout=timeout,
            verbose=verbose,
            operation=operation,
        )

    def export(
        self,
        model: str,
        filters: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        include_custom_fields: bool = True,
        include_tags: bool = True,
        format: str = "jsonl",
        output_path: Optional[Path] = None,
        force_refresh: bool = False,
        check_cache_only: bool = False,
        client_cache_key: Optional[str] = None,
        wait: bool = True,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Export data from NetBox to JSONL or Parquet file.

        Supports caching: if data hasn't changed since last export, returns
        cached file immediately (HTTP 200) instead of creating a new job.

        Args:
            model: Model identifier (e.g., 'dcim.device')
            filters: Dict of filter parameters (e.g., {'site_id': 1})
            fields: Specific fields to export (default: all)
            include_custom_fields: Include custom_field_data column
            include_tags: Include tags column
            format: Export format - 'jsonl' (default) or 'parquet'
            output_path: Where to save file (default: temp)
            force_refresh: Bypass cache and generate fresh export
            check_cache_only: Only check cache status, don't create job on miss
            client_cache_key: Client's cached version key (for 304 response)
            wait: If True, poll until job completes
            poll_interval: Seconds between status polls
            timeout: Max seconds to wait for completion
            verbose: Print progress messages

        Returns:
            Dict with export result:
            - For cache hit: {'cached': True, 'cache_key': '...', 'path': Path, ...}
            - For cache miss with job: {'cached': False, 'job_id': '...', 'path': Path, ...}
            - For check_cache_only: {'cached': bool, 'data_changed': bool, ...}
            - For 304 Not Modified: {'status_code': 304, 'cached': True, ...}
        """
        import tempfile

        data = {
            "model": model,
            "format": format,
            "include_custom_fields": include_custom_fields,
            "include_tags": include_tags,
        }
        if filters:
            data["filters"] = filters
        if fields:
            data["fields"] = fields
        if force_refresh:
            data["force_refresh"] = True
        if check_cache_only:
            data["check_cache_only"] = True
        if client_cache_key:
            data["client_cache_key"] = client_cache_key

        response = self.session.post(
            f"{self.api_base}/export/",
            json=data,
        )

        # Handle 304 Not Modified
        if response.status_code == 304:
            result = response.json() if response.content else {}
            result["status_code"] = 304
            result["cached"] = True
            if verbose:
                print(f"Cache current: client file is up to date")
            return result

        response.raise_for_status()
        result = response.json()
        result["status_code"] = response.status_code

        # Handle check_cache_only response
        if check_cache_only:
            if verbose:
                if result.get("cached"):
                    print(f"Cache valid: {model}")
                else:
                    print(f"Cache invalid or missing: {model}")
            return result

        # Handle cache hit (HTTP 200)
        if response.status_code == 200 and result.get("cached"):
            if verbose:
                print(f"Cache hit: {result.get('row_count', 'N/A')} rows")

            # Download the cached file
            download_url = result.get("download_url")
            if download_url:
                output_path = self._download_export_file(
                    download_url,
                    output_path,
                    format,
                    verbose,
                )
                result["path"] = output_path

            return result

        # Handle job submission (HTTP 202)
        if not wait:
            return result

        job_id = result.get("job_id")
        if not job_id:
            raise TurboBulkError("No job_id in export response")

        if verbose:
            print(f"Cache miss: creating new export job")

        # Wait for completion
        job_result = self._wait_for_job(
            job_id,
            poll_interval=poll_interval,
            timeout=timeout,
            verbose=verbose,
            operation=f"export {model}",
        )

        # Download the exported file
        # Prefer download_url at top level (new API), then file_url in data (legacy)
        download_url = job_result.get("download_url")
        if not download_url:
            download_url = job_result.get("data", {}).get("file_url")

        # Fall back to constructing API download endpoint URL
        if not download_url:
            download_url = f"{self.api_base}/jobs/{job_id}/download/"

        file_path = job_result.get("data", {}).get("file_path")
        if verbose:
            print(f"Export file at: {file_path or download_url}")
        output_path = self._download_export_file(download_url, output_path, format, verbose)
        job_result["path"] = output_path

        # Add cache info to result
        job_result["cached"] = False
        return job_result

    def check_export_cache(
        self,
        model: str,
        filters: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        include_custom_fields: bool = True,
        include_tags: bool = True,
        format: str = "jsonl",
        client_cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Check if a cached export exists for the given parameters.

        This is a convenience wrapper that doesn't create a job on cache miss.

        Args:
            model: Model identifier
            filters: Filter parameters
            fields: Specific fields
            include_custom_fields: Include custom fields
            include_tags: Include tags
            format: Export format - 'jsonl' (default) or 'parquet'
            client_cache_key: Optional client cache key for 304 check

        Returns:
            Dict with cache status:
            - {'cached': True, 'cache_key': '...', 'download_url': '...', ...}
            - {'cached': False, 'data_changed': True, ...}
            - {'status_code': 304, ...} if client_cache_key matches
        """
        return self.export(
            model=model,
            filters=filters,
            fields=fields,
            include_custom_fields=include_custom_fields,
            include_tags=include_tags,
            format=format,
            check_cache_only=True,
            client_cache_key=client_cache_key,
            wait=False,
            verbose=False,
        )

    def _download_export_file(
        self,
        url: str,
        output_path: Optional[Path],
        format: str,
        verbose: bool,
    ) -> Path:
        """Download an export file from URL."""
        import tempfile

        if url.startswith("/"):
            url = f"{self.base_url}{url}"

        if verbose:
            print(f"Downloading export file...")

        download_response = self.session.get(url)
        download_response.raise_for_status()

        if output_path is None:
            suffix = ".jsonl.gz" if format == "jsonl" else ".parquet"
            fd, output_path = tempfile.mkstemp(suffix=suffix)
            os.close(fd)
            output_path = Path(output_path)
        else:
            output_path = Path(output_path)

        with open(output_path, "wb") as f:
            f.write(download_response.content)

        if verbose:
            print(f"Saved to: {output_path}")

        return output_path

    def get_job_status(self, job_id: str) -> Dict:
        """
        Get status of a bulk operation job.

        Args:
            job_id: Job UUID

        Returns:
            Job status dict
        """
        response = self.session.get(f"{self.api_base}/jobs/{job_id}/")
        response.raise_for_status()
        return response.json()

    def _wait_for_job(
        self,
        job_id: str,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
        operation: str = "operation",
    ) -> Dict:
        """Wait for a job to complete."""
        start_time = time.time()
        last_status = None

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TurboBulkError(f"Job {job_id} timed out after {timeout}s")

            result = self.get_job_status(job_id)
            status = result.get("status")

            if verbose and status != last_status:
                print(f"[{elapsed:.1f}s] {operation}: {status}")
                last_status = status

            if status == "completed":
                if verbose:
                    rows = result.get("data", {}).get("rows_affected", "N/A")
                    duration = result.get("duration_seconds", "N/A")
                    print(f"Completed: {rows} rows in {duration}s")
                return result

            if status == "errored" or status == "failed":
                error_msg = result.get("data", {}).get("error", "Unknown error")
                raise JobFailedError(f"Job failed: {error_msg}", result)

            time.sleep(poll_interval)

    # Convenience methods for NetBox REST API queries

    def rest_get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a GET request to the NetBox REST API.

        Args:
            endpoint: API endpoint (e.g., '/api/dcim/sites/')
            params: Query parameters

        Returns:
            Response JSON
        """
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        response = self.session.get(f"{self.base_url}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def rest_get_all(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Get all results from a paginated endpoint.

        Args:
            endpoint: API endpoint (e.g., '/api/dcim/sites/')
            params: Query parameters

        Returns:
            List of all result objects
        """
        params = params or {}
        params["limit"] = 1000
        results = []

        while True:
            data = self.rest_get(endpoint, params)
            results.extend(data.get("results", []))

            next_url = data.get("next")
            if not next_url:
                break

            from urllib.parse import urlparse, parse_qs

            parsed = urlparse(next_url)
            next_params = parse_qs(parsed.query)
            params["offset"] = next_params.get("offset", [0])[0]

        return results

    def get_content_type_id(self, app_label: str, model: str) -> int:
        """
        Get ContentType ID for a model.

        Useful for CableTermination and other GenericFK fields.

        Args:
            app_label: App label (e.g., 'dcim')
            model: Model name (e.g., 'interface')

        Returns:
            ContentType ID
        """
        data = self.rest_get(
            "/api/core/object-types/", params={"app_label": app_label, "model": model}
        )
        results = data.get("results", [])
        if not results:
            raise TurboBulkError(f"ContentType not found: {app_label}.{model}")
        return results[0]["id"]
