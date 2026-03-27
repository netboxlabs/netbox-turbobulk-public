"""
Tests for TurboBulkClient.

Uses unittest.mock to mock HTTP requests without requiring a running server.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from turbobulk_client import TurboBulkClient
from turbobulk_client.exceptions import AuthenticationError, TurboBulkError


class TestClientInitialization(unittest.TestCase):
    """Tests for TurboBulkClient initialization."""

    def test_init_with_explicit_args(self):
        """Initialize client with explicit URL and token."""
        client = TurboBulkClient("http://netbox:8080", "test-token")
        self.assertEqual(client.base_url, "http://netbox:8080")
        self.assertEqual(client.token, "test-token")

    def test_init_strips_trailing_slash(self):
        """Trailing slash is stripped from base_url."""
        client = TurboBulkClient("http://netbox:8080/", "test-token")
        self.assertEqual(client.base_url, "http://netbox:8080")

    def test_init_from_env_vars(self):
        """Initialize client from environment variables."""
        with patch.dict(
            "os.environ", {"NETBOX_URL": "http://env-netbox:8080", "NETBOX_TOKEN": "env-token"}
        ):
            client = TurboBulkClient()
            self.assertEqual(client.base_url, "http://env-netbox:8080")
            self.assertEqual(client.token, "env-token")

    def test_init_missing_url_raises(self):
        """Missing URL raises TurboBulkError."""
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(TurboBulkError) as ctx:
                TurboBulkClient(token="test-token")
            self.assertIn("URL required", str(ctx.exception))

    def test_init_missing_token_raises(self):
        """Missing token raises TurboBulkError."""
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(TurboBulkError) as ctx:
                TurboBulkClient(base_url="http://netbox:8080")
            self.assertIn("token required", str(ctx.exception))


class TestGetTemplate(unittest.TestCase):
    """Tests for get_template() method."""

    def setUp(self):
        self.client = TurboBulkClient("http://netbox:8080", "test-token")

    def test_get_template_returns_required_fields_only(self):
        """get_template() returns only required fields by default."""
        mock_schema = {
            "fields": [
                {
                    "name": "id",
                    "type": "AutoField",
                    "primary_key": True,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "slug",
                    "type": "SlugField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "status",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": "active",
                },
                {
                    "name": "description",
                    "type": "TextField",
                    "primary_key": False,
                    "nullable": True,
                    "default": "",
                },
            ]
        }

        with patch.object(self.client, "get_model_schema", return_value=mock_schema):
            template = self.client.get_template("dcim.site")

        # Should include required fields without defaults
        self.assertIn("name", template)
        self.assertIn("slug", template)
        # Should NOT include primary key
        self.assertNotIn("id", template)
        # Should NOT include optional field with default
        self.assertNotIn("description", template)

    def test_get_template_with_include_optional(self):
        """get_template(include_optional=True) includes all fields."""
        mock_schema = {
            "fields": [
                {
                    "name": "id",
                    "type": "AutoField",
                    "primary_key": True,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "description",
                    "type": "TextField",
                    "primary_key": False,
                    "nullable": True,
                    "default": "",
                },
            ]
        }

        with patch.object(self.client, "get_model_schema", return_value=mock_schema):
            template = self.client.get_template("dcim.site", include_optional=True)

        # Should include optional field
        self.assertIn("description", template)
        # Still NOT include primary key
        self.assertNotIn("id", template)

    def test_get_template_fk_uses_id_suffix(self):
        """FK fields use _id suffix in template."""
        mock_schema = {
            "fields": [
                {
                    "name": "id",
                    "type": "AutoField",
                    "primary_key": True,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "site",
                    "type": "ForeignKey",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                    "foreign_key": "dcim.site",
                },
            ]
        }

        with patch.object(self.client, "get_model_schema", return_value=mock_schema):
            template = self.client.get_template("dcim.device")

        # FK should use _id suffix
        self.assertIn("site_id", template)
        self.assertNotIn("site", template)

    def test_get_template_skips_primary_key(self):
        """Primary key field is not included in template."""
        mock_schema = {
            "fields": [
                {
                    "name": "id",
                    "type": "AutoField",
                    "primary_key": True,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
            ]
        }

        with patch.object(self.client, "get_model_schema", return_value=mock_schema):
            template = self.client.get_template("dcim.site")

        self.assertNotIn("id", template)

    def test_get_template_generates_correct_defaults_by_type(self):
        """Template generates appropriate default values by field type."""
        mock_schema = {
            "fields": [
                {
                    "name": "name",
                    "type": "CharField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "count",
                    "type": "IntegerField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "active",
                    "type": "BooleanField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "data",
                    "type": "JSONField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
                {
                    "name": "rate",
                    "type": "DecimalField",
                    "primary_key": False,
                    "nullable": False,
                    "default": None,
                },
            ]
        }

        with patch.object(self.client, "get_model_schema", return_value=mock_schema):
            template = self.client.get_template("test.model")

        self.assertEqual(template["name"], "")
        self.assertEqual(template["count"], 0)
        self.assertEqual(template["active"], False)
        self.assertEqual(template["data"], {})
        self.assertEqual(template["rate"], 0.0)


class TestValidate(unittest.TestCase):
    """Tests for validate() method."""

    def setUp(self):
        self.client = TurboBulkClient("http://netbox:8080", "test-token")

    def test_validate_calls_load_with_dry_run_true(self):
        """validate() is wrapper around load(dry_run=True)."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            # Write minimal valid parquet
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"name": ["test"]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            with patch.object(self.client, "load") as mock_load:
                mock_load.return_value = {"valid": True, "rows": 1}
                result = self.client.validate("dcim.site", parquet_path)

            # Verify load was called with dry_run=True
            mock_load.assert_called_once()
            call_kwargs = mock_load.call_args.kwargs
            self.assertTrue(call_kwargs.get("dry_run"))
        finally:
            parquet_path.unlink()

    def test_validate_passes_all_parameters(self):
        """validate() passes mode and conflict_fields to load()."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"name": ["test"]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            with patch.object(self.client, "load") as mock_load:
                mock_load.return_value = {"valid": True, "rows": 1}
                self.client.validate(
                    "dcim.device",
                    parquet_path,
                    mode="upsert",
                    conflict_fields=["name", "site"],
                )

            call_kwargs = mock_load.call_args.kwargs
            self.assertEqual(call_kwargs["mode"], "upsert")
            self.assertEqual(call_kwargs["conflict_fields"], ["name", "site"])
            self.assertTrue(call_kwargs["dry_run"])
        finally:
            parquet_path.unlink()


class TestLoadDryRun(unittest.TestCase):
    """Tests for load() with dry_run parameter."""

    def setUp(self):
        self.client = TurboBulkClient("http://netbox:8080", "test-token")

    def test_load_sends_dry_run_parameter_in_form_data(self):
        """load(dry_run=True) sends dry_run in request form data."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"name": ["test"]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            mock_response = MagicMock()
            mock_response.json.return_value = {"job_id": "test-job-id"}
            mock_response.raise_for_status = MagicMock()

            with patch.object(self.client.session, "post", return_value=mock_response) as mock_post:
                with patch.object(
                    self.client, "_wait_for_job", return_value={"valid": True, "rows": 1}
                ):
                    self.client.load("dcim.site", parquet_path, dry_run=True)

                # Check that dry_run was included in form data
                call_args = mock_post.call_args
                form_data = call_args.kwargs.get("data", {})
                self.assertEqual(form_data.get("dry_run"), "true")
        finally:
            parquet_path.unlink()

    def test_load_dry_run_false_by_default(self):
        """load() does not send dry_run when not specified."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"name": ["test"]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            mock_response = MagicMock()
            mock_response.json.return_value = {"job_id": "test-job-id"}
            mock_response.raise_for_status = MagicMock()

            with patch.object(self.client.session, "post", return_value=mock_response) as mock_post:
                with patch.object(self.client, "_wait_for_job", return_value={"status": "success"}):
                    self.client.load("dcim.site", parquet_path)

                # dry_run should not be in form data
                call_args = mock_post.call_args
                form_data = call_args.kwargs.get("data", {})
                self.assertNotIn("dry_run", form_data)
        finally:
            parquet_path.unlink()

    def test_load_dry_run_operation_label(self):
        """load(dry_run=True) uses dry-run in operation label."""
        # This tests internal behavior - the operation label for verbose output
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"name": ["test"]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            mock_response = MagicMock()
            mock_response.json.return_value = {"job_id": "test-job-id"}
            mock_response.raise_for_status = MagicMock()

            with patch.object(self.client.session, "post", return_value=mock_response):
                with patch.object(
                    self.client, "_wait_for_job", return_value={"valid": True}
                ) as mock_wait:
                    self.client.load("dcim.site", parquet_path, dry_run=True)

                # Check operation label includes dry-run
                call_kwargs = mock_wait.call_args.kwargs
                self.assertIn("dry-run", call_kwargs.get("operation", ""))
        finally:
            parquet_path.unlink()


class TestDeleteDryRun(unittest.TestCase):
    """Tests for delete() with dry_run parameter."""

    def setUp(self):
        self.client = TurboBulkClient("http://netbox:8080", "test-token")

    def test_delete_sends_dry_run_parameter_in_form_data(self):
        """delete(dry_run=True) sends dry_run in request form data."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"id": [1, 2, 3]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            mock_response = MagicMock()
            mock_response.json.return_value = {"job_id": "test-job-id"}
            mock_response.raise_for_status = MagicMock()

            with patch.object(self.client.session, "post", return_value=mock_response) as mock_post:
                with patch.object(
                    self.client, "_wait_for_job", return_value={"valid": True, "rows": 3}
                ):
                    self.client.delete("dcim.site", parquet_path, dry_run=True)

                # Check that dry_run was included in form data
                call_args = mock_post.call_args
                form_data = call_args.kwargs.get("data", {})
                self.assertEqual(form_data.get("dry_run"), "true")
        finally:
            parquet_path.unlink()

    def test_delete_dry_run_false_by_default(self):
        """delete() does not send dry_run when not specified."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"id": [1, 2, 3]})
            pq.write_table(table, f.name)
            parquet_path = Path(f.name)

        try:
            mock_response = MagicMock()
            mock_response.json.return_value = {"job_id": "test-job-id"}
            mock_response.raise_for_status = MagicMock()

            with patch.object(self.client.session, "post", return_value=mock_response) as mock_post:
                with patch.object(self.client, "_wait_for_job", return_value={"status": "success"}):
                    self.client.delete("dcim.site", parquet_path)

                # dry_run should not be in form data
                call_args = mock_post.call_args
                form_data = call_args.kwargs.get("data", {})
                self.assertNotIn("dry_run", form_data)
        finally:
            parquet_path.unlink()


class TestAuthErrorHandling(unittest.TestCase):
    """Tests for authentication error handling and v2 token hint."""

    def test_403_with_non_nbt_token_includes_hint(self):
        """403 with non-nbt_ token raises AuthenticationError with v2 format hint."""
        client = TurboBulkClient("http://netbox:8080", "plaintext-only-token-value-40chars0000")

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"detail": "Invalid v1 token"}

        with patch.object(client.session, "get", return_value=mock_response):
            with self.assertRaises(AuthenticationError) as ctx:
                client.get_models()

        self.assertIn("nbt_", str(ctx.exception))
        self.assertIn("v2 token", str(ctx.exception))

    def test_403_with_nbt_token_no_hint(self):
        """403 with nbt_ token raises AuthenticationError without v2 format hint."""
        client = TurboBulkClient("http://netbox:8080", "nbt_key123456ab.plaintextvalue")

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"detail": "Invalid v2 token"}

        with patch.object(client.session, "get", return_value=mock_response):
            with self.assertRaises(AuthenticationError) as ctx:
                client.get_models()

        self.assertIn("Invalid v2 token", str(ctx.exception))
        self.assertNotIn("does not start with", str(ctx.exception))

    def test_401_with_non_nbt_token_includes_hint(self):
        """401 with non-nbt_ token also includes the v2 format hint."""
        client = TurboBulkClient("http://netbox:8080", "some-legacy-token")

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"detail": "Invalid v1 token"}

        with patch.object(client.session, "get", return_value=mock_response):
            with self.assertRaises(AuthenticationError) as ctx:
                client.get_models()

        self.assertIn("nbt_", str(ctx.exception))

    def test_403_writes_disabled_raises_turbobulk_error(self):
        """403 with 'write operations are disabled' raises TurboBulkError, not AuthenticationError."""
        client = TurboBulkClient("http://netbox:8080", "nbt_key123456ab.plaintextvalue")

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {
            "detail": "TurboBulk write operations are disabled. "
            "Set 'enable_writes' to True in PLUGINS_CONFIG to enable."
        }

        with patch.object(client.session, "post", return_value=mock_response):
            with self.assertRaises(TurboBulkError) as ctx:
                client.session.post.return_value = mock_response
                client._raise_for_status(mock_response)

        # Should be TurboBulkError, not AuthenticationError
        self.assertNotIsInstance(ctx.exception, AuthenticationError)
        self.assertIn("write operations are disabled", str(ctx.exception))
        self.assertIn("enable_writes", str(ctx.exception))

    def test_non_auth_error_raises_http_error(self):
        """Non-auth HTTP errors (e.g. 500) raise standard HTTPError."""
        import requests

        client = TurboBulkClient("http://netbox:8080", "test-token")

        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error"
        )

        with patch.object(client.session, "get", return_value=mock_response):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_models()


class TestDownloadHandling(unittest.TestCase):
    """Tests for _download_export_file redirect and streaming behavior."""

    def setUp(self):
        self.client = TurboBulkClient("http://netbox:8080", "nbt_key.token123")

    def _mock_response(
        self, status_code=200, content=b"file data", headers=None, is_redirect=False
    ):
        """Create a mock response with streaming support."""
        import requests

        resp = MagicMock(spec=requests.Response)
        resp.status_code = status_code
        resp.headers = headers or {}
        resp.is_redirect = is_redirect
        resp.iter_content = MagicMock(return_value=[content])
        resp.content = content
        resp.close = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    def test_download_handles_302_redirect(self):
        """302 from server is followed via a bare GET to the presigned URL."""
        redirect_resp = self._mock_response(
            status_code=302,
            is_redirect=True,
            headers={"Location": "https://s3.amazonaws.com/bucket/file.jsonl.gz?sig=abc"},
        )
        final_resp = self._mock_response(content=b"real file data")

        with patch.object(self.client.session, "get", return_value=redirect_resp):
            with patch("turbobulk_client.client.requests.get", return_value=final_resp) as bare_get:
                path = self.client._download_export_file(
                    f"{self.client.api_base}/jobs/123/download/",
                    None,
                    "jsonl",
                    False,
                )

        # Verify the redirect was followed with a bare requests.get
        bare_get.assert_called_once_with(
            "https://s3.amazonaws.com/bucket/file.jsonl.gz?sig=abc",
            stream=True,
            verify=True,
        )
        # File was written
        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), b"real file data")
        path.unlink()

    def test_download_redirect_no_auth_headers(self):
        """Presigned URL request must not carry Authorization headers."""
        redirect_resp = self._mock_response(
            status_code=302,
            is_redirect=True,
            headers={"Location": "https://storage.cloud.google.com/bucket/file.parquet?token=xyz"},
        )
        final_resp = self._mock_response(content=b"parquet data")

        with patch.object(self.client.session, "get", return_value=redirect_resp):
            with patch("turbobulk_client.client.requests.get", return_value=final_resp) as bare_get:
                path = self.client._download_export_file(
                    f"{self.client.api_base}/jobs/456/download/",
                    None,
                    "parquet",
                    False,
                )

        # bare requests.get (not session.get) means no auth headers
        call_kwargs = bare_get.call_args
        # The call should NOT include any Authorization header — it uses
        # requests.get() directly, not self.session.get()
        self.assertNotIn("headers", call_kwargs.kwargs)
        path.unlink()

    def test_download_200_binary_writes_directly(self):
        """200 response (local storage) writes content directly to disk."""
        direct_resp = self._mock_response(content=b"local file bytes")

        with patch.object(self.client.session, "get", return_value=direct_resp):
            path = self.client._download_export_file(
                f"{self.client.api_base}/jobs/789/download/",
                None,
                "jsonl",
                False,
            )

        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), b"local file bytes")
        path.unlink()

    def test_download_streams_to_disk(self):
        """File is written via iter_content, not response.content."""
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        resp = self._mock_response()
        resp.iter_content = MagicMock(return_value=iter(chunks))

        with patch.object(self.client.session, "get", return_value=resp):
            path = self.client._download_export_file(
                f"{self.client.api_base}/jobs/abc/download/",
                None,
                "jsonl",
                False,
            )

        # iter_content was called with chunk_size
        resp.iter_content.assert_called_once_with(chunk_size=8192)
        self.assertEqual(path.read_bytes(), b"chunk1chunk2chunk3")
        path.unlink()

    def test_download_error_raises(self):
        """HTTP errors during download raise appropriate exception."""
        import requests as req

        error_resp = self._mock_response(status_code=404)
        error_resp.raise_for_status.side_effect = req.exceptions.HTTPError("404 Not Found")

        with patch.object(self.client.session, "get", return_value=error_resp):
            with self.assertRaises(req.exceptions.HTTPError):
                self.client._download_export_file(
                    f"{self.client.api_base}/jobs/999/download/",
                    None,
                    "jsonl",
                    False,
                )

    def test_download_relative_url_prepends_base(self):
        """Relative URLs get base_url prepended."""
        resp = self._mock_response(content=b"data")

        with patch.object(self.client.session, "get", return_value=resp) as mock_get:
            path = self.client._download_export_file(
                "/api/plugins/turbobulk/jobs/123/download/",
                None,
                "jsonl",
                False,
            )

        # Should have prepended the base URL
        called_url = mock_get.call_args[0][0]
        self.assertTrue(called_url.startswith("http://netbox:8080"))
        path.unlink()


if __name__ == "__main__":
    unittest.main()
