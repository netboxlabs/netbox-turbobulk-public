# Changelog

All notable changes to the TurboBulk Client will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.17] - 2026-11-12

Version parity with TurboBulk server v0.4.16 and v0.4.17. No client code
changes; documentation only (#24). The server releases are the Nov 2–9 bug
batch; the items below are the ones a client user can observe.

### Changed (documentation)

- `rebuild_search_index` now re-caches only the objects an operation touched
  instead of rebuilding the model's whole search index (server #121), so the
  advice to disable it for large imports is withdrawn. Hook cost no longer grows
  with table size; default loads are several times faster.

### Server behaviour relevant to client users

- **Authorization (fail closed).** Read-only API tokens (`write_enabled=False`)
  get 403 on `load`/`delete` (export still works). Bulk load/upsert/delete now
  honour ObjectPermission *constraints*: an operation touching any row outside
  the caller's constraints fails as a whole and is rolled back. Upsert requires
  both `add` and `change`. Permissions are re-checked when the job runs.
- **Protected models refused as write targets.** `users.*`, `core.*`, `extras.*`
  (except `extras.tag`), netbox-branching and TurboBulk's own tables return
  400 with `error_type: "model_write_denied"` on load/delete; export is
  unaffected. `GET /models/` and `GET /models/<app.model>/` report
  `write_denied`.
- **Multipart requests.** `filters`, `fields`, `conflict_fields` and
  `key_fields` sent as a JSON-string form field (how this client sends them)
  are parsed; a field sent both as a JSON string and as dotted/repeated keys, a
  repeated `filters` field, or a non-array column list is rejected with 400
  instead of being silently dropped or misread.
- **Data written by bulk operations.** `created`/`last_updated` are stamped
  (client-supplied values win; upserts bump `last_updated`). Tags attach on
  insert and natural-key upsert for every model; a `_tags` value of `null`
  leaves an object's tags alone, `[]` clears them. `_path` on port models is
  server-managed and ignored if present in a payload. Changelog `object_repr`
  matches `str(obj)` for name-less models (Prefix, IPAddress, IPRange, Cable,
  …), and changelog/ChangeDiff data is written in NetBox's `serialize_object()`
  shape, so branch loads merge and conflict detection works.
- **Job result.** `post_hooks.rebuild_search_index` reports `reindexed`,
  `cached_values` and `removed_values`; `success`/`message` are unchanged and
  the command `output` field is gone.

## [0.4.10] - 2026-10-31

Version parity with TurboBulk server v0.4.10 (netbox-turbobulk#87).

### Added

- **Export cache versions for HTTP 304.** `export()` and `check_export_cache()`
  take a new optional `client_cache_version` argument, sent as
  `client_cache_version` only when set. Export results now surface the server's
  `cache_version` next to `cache_key`: from a cached 200, from a completed export
  job's `data` (lifted to the top level along with `cache_stored`), and echoed
  back on a 304. Store both values with the file and pass both back to skip a
  re-download. Matches the server change that returns 304 only when both the key
  and the version of the current build match.

### Compatibility

- Against servers with `cache_version` support, a request that sends only
  `client_cache_key` now gets the cached file again (200) instead of a 304.
  Nothing errors; pass `client_cache_version` too to get 304s back.
- Against older servers, nothing changes: they ignore `client_cache_version`
  and do not return `cache_version`, and `client_cache_key` alone still gets a
  304.

## [0.4.1] - 2026-09-23

Version parity with TurboBulk server v0.4.1. No client code changes.

Server v0.4.1 is a metadata/docs patch: the plugin now declares its catalog
metadata in the `schema_version: 1` format (so it surfaces correctly in the
plugins catalog), caps its supported NetBox range at `4.7.99` (was `4.99`) to
match what is tested, and corrects stale documentation. No API, schema, or
export-format changes affecting the client.

## [0.4.0] - 2026-09-17

Version parity with TurboBulk server v0.4.0. No client code changes.

### Added

- **Server: opt-in resource guardrails.** The server can now bound a single bulk
  operation's impact on the shared database (noisy-neighbor protection on
  multi-tenant deployments): per-operation statement/lock timeouts, a
  max-rows-per-operation limit, a per-database concurrency cap, and an optional
  empty-delete guard. All are **off by default** and configured server-side. When
  a limit is exceeded, the operation fails with a clear `guardrail` error, which
  the client surfaces as `JobFailedError`.
- **Server: per-operation timing.** Load and delete job results now include
  `duration_seconds`, and lifecycle logs carry structured job/operation context.

### Changed

- **Server: dependency and CI maintenance** across server v0.3.2–v0.3.6 (Renovate
  adoption; pandas 3, pyarrow 25, pytest 9, ruff 0.16.6, GitHub Actions v7). No
  API, request/response schema, or export-format changes affecting the client.

## [0.3.1] - 2026-09-02

Version parity with TurboBulk server v0.3.1. No client code changes.

### Security

- **Server: export permission hardening.** Exports are now filtered to the
  rows the caller's NetBox ObjectPermissions actually permit (a model-level
  permission no longer returns every row), and credential columns (e.g. token
  `key`/`plaintext`) are never emitted. Cached exports are bound to their
  owning caller so a cached file cannot be served to a differently-scoped one.

### Fixed

- **Server: NetBox Enterprise auth.** TurboBulk's API now accepts the NetBox
  Identity Token on the `JWT` scheme, so requests from NetBox Enterprise
  authenticate instead of being rejected as anonymous.

## [0.3.0] - 2026-08-23

Version parity with TurboBulk server v0.3.0. No client code changes — schema
responses pass through as-is and any model can be exported.

### Added

- **Server: NetBox 4.7 compatibility.** TurboBulk now supports NetBox 4.7 (in
  addition to 4.5/4.6): the MPTT→ltree migration for nested/tree models, new
  cooling/thermal models (`DiameterMixin` `_abs_diameter`, `MaxFlowMixin`
  `_abs_max_flow`), and Django 6.1 partial expression indexes (so
  case-insensitive natural-key upserts of Device/VM keep working).

### Fixed

- **Server: upsert with an empty `conflict_fields` list** no longer errors with
  `ON CONFLICT ()` — it defaults to the primary key like an omitted value.
- **Server: `apply_save_hooks` clear semantics** — clearing a value+unit pair
  together now also clears the derived `_abs_*` column, matching NetBox `save()`.

## [0.2.1] - 2026-08-06

Version parity with TurboBulk server v0.2.1, which was a release-pipeline fix
(the v0.2.0 build never reached CodeArtifact/S3; v0.2.1 is the first fully
published build). No functional changes to the client or plugin since 0.2.0.

## [0.2.0] - 2026-08-06

Tandem release aligning the client version with TurboBulk server v0.2.0. No client
code changes — the client already surfaces these server capabilities (schema
responses are returned as-is, and any model can be passed to `export()`).

### Added

- **Server: port-mapping export**. The private `dcim.portmapping` (and
  `dcim.porttemplatemapping`) tables are now exportable through TurboBulk. NetBox
  moved the front↔rear pass-through relationship into these tables, so this is the
  only way to bulk-read that wiring. They appear in the model list flagged
  `export_only` and are authorized by the public ports' view permissions
  (`dcim.view_frontport` + `dcim.view_rearport`). Load/delete remain unsupported.
- **Server: richer model schema**. `get_model_schema()` now returns, per field,
  the JSON type the JSONL export actually writes (`jsonl_type`, `array_item_type`,
  `jsonl_nullable`) plus `choice_labels`; and per model, `export_only_columns`
  (currently `_tags`) and a `jsonl_format_version`. Consumers can now describe an
  export row exactly instead of inferring wire types from `arrow_type`.

### Changed

- **Server: private/internal models filtered from discovery**. NetBox
  `_netbox_private` and non-public models are no longer listed by the models
  endpoint, and a direct request for one returns a clear error instead of a
  confusing ungrantable-permission denial.

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
