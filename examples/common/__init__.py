"""Common utilities for TurboBulk examples."""

# Try to import from installed turbobulk_client package first,
# fall back to local implementation if not installed
try:
    from turbobulk_client import TurboBulkClient, TurboBulkError, JobFailedError
except ImportError:
    from .client import TurboBulkClient, TurboBulkError, JobFailedError

from .parquet_utils import create_parquet, read_parquet, create_pk_parquet

__all__ = [
    'TurboBulkClient',
    'TurboBulkError',
    'JobFailedError',
    'create_parquet',
    'read_parquet',
    'create_pk_parquet',
]
