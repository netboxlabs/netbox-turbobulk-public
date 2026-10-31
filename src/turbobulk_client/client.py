"""
TurboBulk API Client.

High-level client for TurboBulk bulk data operations.
"""

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

import requests

from .exceptions import AuthenticationError, JobFailedError, TurboBulkError


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

    def _raise_for_status(self, response: requests.Response) -> None:
        """Raise an appropriate exception for HTTP errors with helpful messages."""
        if response.status_code in (401, 403):
            detail = ""
            try:
                detail = response.json().get("detail", "")
            except Exception:
                pass

            # Distinguish server-side write disable from auth failures
            if "write operations are disabled" in str(detail).lower():
                raise TurboBulkError(f"{response.status_code} {detail}")

            msg = f"{response.status_code} Authentication failed: {detail}"

            if not self.token.startswith("nbt_"):
                msg += (
                    "\n\nHint: your token does not start with 'nbt_'. If you created a v2 token "
                    "(default in NetBox 4.5+), you must use the full token value in the format: "
                    "nbt_<key>.<plaintext>. See: https://github.com/netboxlabs/netbox-turbobulk-public#authentication"
                )

            raise AuthenticationError(msg)

        response.raise_for_status()

    def get_models(self) -> List[Dict]:
        """
        List available models with their schemas.

        Returns:
            List of model info dicts with app_label, model_name, etc.
        """
        response = self.session.get(f"{self.api_base}/models/")
        self._raise_for_status(response)
        return cast(List[Dict[Any, Any]], response.json())

    def get_model_schema(self, model: str) -> Dict:
        """
        Get detailed schema for a specific model.

        Args:
            model: Model identifier (e.g., 'dcim.device', 'dcim.site')

        Returns:
            Schema dict with fields, constraints, etc.
        """
        response = self.session.get(f"{self.api_base}/models/{model}/")
        self._raise_for_status(response)
        return cast(Dict[Any, Any], response.json())

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
        apply_save_hooks: Optional[bool] = None,
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
            apply_save_hooks: Apply post-merge SQL fixups for computed
                fields (server default: False). None=use server default.
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
            apply_save_hooks=apply_save_hooks,
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
        apply_save_hooks: Optional[bool] = None,
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
            apply_save_hooks: Apply post-merge SQL fixups for computed fields
                (server default: False). When enabled, uses bulk SQL to populate
                Device.location (from Rack), custom field defaults, unit
                normalizations (_abs_weight, _abs_length), scope cache fields,
                and more. Falls back to per-object save() only for plugin models
                with custom save() overrides. None=use server default.
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

        # Build form data as list of tuples to support repeated keys
        form_data = [
            ("model", model),
            ("mode", mode),
            ("validation_mode", validation_mode),
            ("create_changelogs", str(create_changelogs).lower()),
        ]
        if conflict_fields:
            for field in conflict_fields:
                form_data.append(("conflict_fields", field))
        if conflict_constraint:
            form_data.append(("conflict_constraint", conflict_constraint))
        if post_hooks:
            import json

            form_data.append(("post_hooks", json.dumps(post_hooks)))
        if dispatch_events is not None:
            form_data.append(("dispatch_events", str(dispatch_events).lower()))
        if apply_save_hooks is not None:
            form_data.append(("apply_save_hooks", str(apply_save_hooks).lower()))
        if branch:
            form_data.append(("branch", branch))
        if dry_run:
            form_data.append(("dry_run", "true"))

        # Upload file
        with open(data_path, "rb") as f:
            files = {"file": (data_path.name, f, "application/octet-stream")}
            response = self.session.post(
                f"{self.api_base}/load/",
                data=form_data,
                files=files,
            )

        self._raise_for_status(response)
        result: Dict[Any, Any] = response.json()

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

        form_data = [
            ("model", model),
            ("cascade_nullable_fks", str(cascade_nullable_fks).lower()),
            ("create_changelogs", str(create_changelogs).lower()),
        ]
        if key_fields:
            for field in key_fields:
                form_data.append(("key_fields", field))
        if dispatch_events is not None:
            form_data.append(("dispatch_events", str(dispatch_events).lower()))
        if branch:
            form_data.append(("branch", branch))
        if dry_run:
            form_data.append(("dry_run", "true"))

        with open(data_path, "rb") as f:
            files = {"file": (data_path.name, f, "application/octet-stream")}
            response = self.session.post(
                f"{self.api_base}/delete/",
                data=form_data,
                files=files,
            )

        self._raise_for_status(response)
        result: Dict[Any, Any] = response.json()

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
        client_cache_version: Optional[str] = None,
        branch: Optional[str] = None,
        wait: bool = True,
        poll_interval: float = 1.0,
        timeout: int = 3600,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Export data from NetBox to JSONL or Parquet file.

        Supports caching: if data hasn't changed since last export, returns
        cached file immediately (HTTP 200) instead of creating a new job.

        Client-side caching (HTTP 304): every result that came from a cached
        build carries ``cache_key`` and ``cache_version``. Pass both back as
        ``client_cache_key`` and ``client_cache_version`` and the server
        answers 304 Not Modified while that build is still current, so the
        file you already hold is reused. ``cache_key`` names the request,
        not the data, so on servers that support ``cache_version`` a key
        sent without its version never earns a 304 (you get the cached file
        again instead). Older servers ignore ``client_cache_version`` and
        do not return ``cache_version``; the client works unchanged there.

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
            client_cache_key: ``cache_key`` of the copy you already hold
                (for a 304 response). Send it together with
                ``client_cache_version``.
            client_cache_version: ``cache_version`` of the copy you already
                hold, from an earlier result. Only sent when set.
            branch: Branch name to export from (requires netbox-branching).
                Rows, tags, and custom fields are read from the branch schema;
                branch exports bypass the export cache.
            wait: If True, poll until job completes
            poll_interval: Seconds between status polls
            timeout: Max seconds to wait for completion
            verbose: Print progress messages

        Returns:
            Dict with export result:
            - For cache hit: {'cached': True, 'cache_key': '...',
              'cache_version': '...', 'path': Path, ...}
            - For cache miss with job: {'cached': False, 'job_id': '...',
              'path': Path, 'cache_key': '...', 'cache_version': '...', ...}
              (cache fields present only when the server stored the build)
            - For check_cache_only: {'cached': bool, 'data_changed': bool, ...}
            - For 304 Not Modified: {'status_code': 304, 'cached': True,
              'cache_key': '...', 'cache_version': '...'} echoing the values
              you sent, so the result can be stored like any other
            ``cache_version`` is absent when talking to servers older than
            the release that introduced it.
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
        # Only sent when set. Servers without cache_version support ignore
        # unknown request fields, so sending it is safe either way.
        if client_cache_version:
            data["client_cache_version"] = client_cache_version
        if branch:
            data["branch"] = branch

        response = self.session.post(
            f"{self.api_base}/export/",
            json=data,
        )

        # Handle 304 Not Modified
        if response.status_code == 304:
            result: Dict[str, Any] = response.json() if response.content else {}
            result["status_code"] = 304
            result["cached"] = True
            # Echo what the caller holds so a 304 result can be stored and
            # replayed exactly like a 200 one.
            if client_cache_key:
                result.setdefault("cache_key", client_cache_key)
            if client_cache_version:
                result.setdefault("cache_version", client_cache_version)
            if verbose:
                print(f"Cache current: client file is up to date")
            return result

        self._raise_for_status(response)
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

        # Add cache info to result. Servers with cache_version support report
        # the stored build's cache_key/cache_version in the job's data (only
        # when the build was cached, see cache_stored); lift them to the top
        # level so callers read them the same way as from a cache hit.
        job_result["cached"] = False
        job_data = job_result.get("data") or {}
        for field in ("cache_key", "cache_version", "cache_stored"):
            if field in job_data and field not in job_result:
                job_result[field] = job_data[field]
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
        client_cache_version: Optional[str] = None,
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
            client_cache_key: ``cache_key`` of the copy you hold, for a 304 check
            client_cache_version: ``cache_version`` of the copy you hold. On
                servers that support it, a 304 needs both values.

        Returns:
            Dict with cache status:
            - {'cached': True, 'cache_key': '...', 'cache_version': '...',
              'download_url': '...', ...}
            - {'cached': False, 'data_changed': True, ...}
            - {'status_code': 304, ...} if the copy you hold is current
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
            client_cache_version=client_cache_version,
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
        """
        Download an export file from URL.

        Handles both direct file responses (local storage) and HTTP 302
        redirects to presigned URLs (cloud storage). Streams content to
        disk to avoid loading entire files into memory.
        """
        import tempfile

        if url.startswith("/"):
            url = f"{self.base_url}{url}"

        if verbose:
            print("Downloading export file...")

        # Request without following redirects so we can strip auth for presigned URLs
        response = self.session.get(url, allow_redirects=False, stream=True)

        if response.is_redirect:
            # Cloud storage: follow the presigned URL without auth headers
            presigned_url = response.headers["Location"]
            response.close()
            if verbose:
                print("Following redirect to cloud storage...")
            response = requests.get(presigned_url, stream=True, verify=self.verify_ssl)

        self._raise_for_status(response)

        if output_path is None:
            suffix = ".jsonl.gz" if format == "jsonl" else ".parquet"
            fd, temp_path = tempfile.mkstemp(suffix=suffix)
            os.close(fd)
            final_path = Path(temp_path)
        else:
            final_path = Path(output_path)

        with open(final_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        response.close()

        if verbose:
            print(f"Saved to: {final_path}")

        return final_path

    def get_job_status(self, job_id: str) -> Dict:
        """
        Get status of a bulk operation job.

        Args:
            job_id: Job UUID

        Returns:
            Job status dict
        """
        response = self.session.get(f"{self.api_base}/jobs/{job_id}/")
        self._raise_for_status(response)
        return cast(Dict[Any, Any], response.json())

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
        self._raise_for_status(response)
        return cast(Dict[Any, Any], response.json())

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

            from urllib.parse import parse_qs, urlparse

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
        return cast(int, results[0]["id"])
