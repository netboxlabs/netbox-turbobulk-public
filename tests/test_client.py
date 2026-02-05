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
from turbobulk_client.exceptions import TurboBulkError


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


if __name__ == "__main__":
    unittest.main()
