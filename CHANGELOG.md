# Changelog

All notable changes to the TurboBulk Client will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-03-25

### Changed

- **Write APIs disabled by default**: The server `enable_writes` setting now defaults to `False`. Write operations (load, delete) require explicit `enable_writes: True` in plugin config. Export and read endpoints are unaffected.

## [0.1.2] - 2026-03-18

### Added

- **Server: `?stream=true` parameter**: Download endpoints now accept `?stream=true` to bypass presigned URL redirects and stream the file through Django. This is useful for browser-based API clients (e.g. Visual Explorer) where `fetch()` would hit CORS errors following cross-origin redirects to cloud storage. The Python client does not need this parameter — it handles redirects natively.

## [0.1.1] - 2026-03-18

### Fixed

- **Cloud storage downloads**: When NetBox uses cloud storage (S3, GCS, Azure), export downloads now work correctly via HTTP 302 redirects to presigned URLs. Previously, clients received a JSON wrapper `{"download_url": "..."}` instead of the actual file, causing silent data corruption.
- **Streaming downloads**: Downloads now stream to disk via `iter_content()` instead of loading the entire file into memory, reducing memory usage for large exports.
- **Presigned URL auth**: Redirect-followed requests to presigned URLs no longer include the NetBox authorization header, which caused 403 errors on some cloud providers.

### Changed

- `_download_export_file()` now uses `allow_redirects=False` and follows redirects manually to strip auth headers for presigned URLs.
- Minimum compatible server version: 0.1.1 (server must return 302 redirects for cloud storage).

## [0.1.0] - 2025-02-04

### Added

Initial public release of the TurboBulk Python client library.

**Core Features:**
- `TurboBulkClient` class for interacting with TurboBulk API
- `load()` method for bulk insert and upsert operations
- `delete()` method for bulk delete operations
- `export()` method for bulk data exports
- `validate()` method for validating data without committing

**Data Format Support:**
- JSONL (JSON Lines) as the default format - easy to create in any language
- Parquet support for maximum throughput with large datasets
- Automatic format detection from file extension or content

**Validation:**
- `validation_mode` parameter: `none`, `auto` (default), `full`
- `validate()` convenience method for testing data before load

**Export Caching:**
- Automatic caching of export results when data is unchanged
- `force_refresh` parameter to bypass cache
- `check_cache_only` mode for efficient sync workflows
- Client-side cache key validation with HTTP 304 support

**Helper Methods:**
- `get_models()` - List available models with schemas
- `get_template()` - Generate template dict with required fields for a model

**Error Handling:**
- `TurboBulkError` base exception
- `JobFailedError` with job result details
- `ValidationError` for data validation failures
- `ConnectionError` for network issues
- `AuthenticationError` for token problems

**Configuration:**
- Environment variable support (`NETBOX_URL`, `NETBOX_TOKEN`)
- SSL verification toggle
- Verbose mode for debugging

### Documentation

- Comprehensive README with quick start guide
- Full API reference documentation
- User guide with operational runbooks
- 12 progressive example scripts
- Troubleshooting guide

### Examples

- `01_hello_turbobulk.py` - Basic site insert
- `02_device_inventory.py` - Devices with FK resolution
- `03_export_transform.py` - ETL workflow
- `04_interface_bulk.py` - Bulk interface creation
- `05_cable_connections.py` - Cable terminations
- `06_gpu_datacenter_cabling.py` - Large-scale cabling (200K cables)
- `07_post_hooks.py` - Post-operation hooks
- `08_branching_workflow.py` - NetBox Branching integration
- `09_cached_exports.py` - Export caching strategies
- `10_validation_best_practices.py` - Validation patterns
- `11_event_streams.py` - Server-sent events for job progress
- `12_format_comparison.py` - JSONL vs Parquet performance
