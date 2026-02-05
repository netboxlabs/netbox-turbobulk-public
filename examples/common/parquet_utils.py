"""
Data file utilities for TurboBulk examples.

Provides simple helpers for creating and reading Parquet and JSONL files.

Supported formats:
- JSONL (JSON Lines): Default format, row-oriented, easy to create
- Parquet: High-performance columnar format

JSONL is the default and recommended format for most use cases due to:
- Universal language support (any language with JSON)
- Row-oriented data (natural for most applications)
- Easy debugging (human-readable)
- Compression via gzip
"""

import gzip
import json
from pathlib import Path
from typing import Dict, Iterator, List, Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq


# =============================================================================
# JSONL UTILITIES (Default Format)
# =============================================================================

def create_jsonl(
    rows: List[Dict[str, Any]],
    path: Path,
    compress: bool = True,
) -> Path:
    """
    Create a JSONL file from row-oriented data.

    This is the recommended format for TurboBulk operations.

    Args:
        rows: List of row dictionaries
        path: Output file path (extension will be added)
        compress: Whether to gzip compress (default: True, recommended)

    Returns:
        Path to created file

    Example:
        create_jsonl([
            {'name': 'site-1', 'slug': 'site-1', 'status': 'active'},
            {'name': 'site-2', 'slug': 'site-2', 'status': 'active'},
        ], Path('/tmp/sites'))
    """
    path = Path(path)
    if compress:
        output_path = path.with_suffix('.jsonl.gz')
        with gzip.open(output_path, 'wt', encoding='utf-8') as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + '\n')
    else:
        output_path = path.with_suffix('.jsonl')
        with open(output_path, 'w', encoding='utf-8') as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + '\n')

    return output_path


def create_pk_jsonl(ids: List[int], path: Path, compress: bool = True) -> Path:
    """
    Create a JSONL file with just an 'id' column for delete operations.

    Args:
        ids: List of primary key IDs to delete
        path: Output file path
        compress: Whether to gzip compress (default: True)

    Returns:
        Path to created file

    Example:
        create_pk_jsonl([1, 2, 3, 4, 5], Path('/tmp/delete_ids'))
    """
    return create_jsonl([{'id': id_} for id_ in ids], path, compress=compress)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """
    Read a JSONL file to list of row dicts.

    Handles both plain and gzipped JSONL files.

    Args:
        path: Path to JSONL file

    Returns:
        List of row dictionaries
    """
    path = Path(path)
    rows = []

    # Check if gzipped
    opener = gzip.open if str(path).endswith('.gz') else open
    mode = 'rt' if str(path).endswith('.gz') else 'r'

    with opener(path, mode, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    return rows


def jsonl_row_count(path: Path) -> int:
    """
    Get row count from a JSONL file.

    Args:
        path: Path to JSONL file

    Returns:
        Number of rows
    """
    path = Path(path)
    opener = gzip.open if str(path).endswith('.gz') else open
    mode = 'rt' if str(path).endswith('.gz') else 'r'

    count = 0
    with opener(path, mode, encoding='utf-8') as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def create_jsonl_streaming(
    row_iterator: Iterator[Dict[str, Any]],
    path: Path,
    compress: bool = True,
) -> Path:
    """
    Stream rows to JSONL file (memory efficient for large datasets).

    Args:
        row_iterator: Iterator yielding row dictionaries
        path: Output file path (extension will be added)
        compress: Whether to gzip compress (default: True)

    Returns:
        Path to created file

    Example:
        def generate_sites():
            for i in range(1000000):
                yield {'name': f'site-{i}', 'slug': f'site-{i}'}

        create_jsonl_streaming(generate_sites(), Path('/tmp/sites'))
    """
    path = Path(path)
    if compress:
        output_path = path.with_suffix('.jsonl.gz')
        with gzip.open(output_path, 'wt', encoding='utf-8') as f:
            for row in row_iterator:
                f.write(json.dumps(row, default=str) + '\n')
    else:
        output_path = path.with_suffix('.jsonl')
        with open(output_path, 'w', encoding='utf-8') as f:
            for row in row_iterator:
                f.write(json.dumps(row, default=str) + '\n')

    return output_path


# =============================================================================
# PARQUET UTILITIES (High-Performance Alternative)
# =============================================================================

def create_parquet(
    data: Dict[str, List[Any]],
    path: Path,
    schema: Optional[pa.Schema] = None,
) -> Path:
    """
    Create a Parquet file from column-oriented data.

    Args:
        data: Dict mapping column names to lists of values.
              All lists must have the same length.
        path: Output file path
        schema: Optional PyArrow schema for explicit type control

    Returns:
        Path to created file

    Example:
        create_parquet({
            'name': ['site-1', 'site-2', 'site-3'],
            'slug': ['site-1', 'site-2', 'site-3'],
            'status': ['active', 'active', 'planned'],
        }, Path('/tmp/sites.parquet'))
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if schema:
        table = pa.table(data, schema=schema)
    else:
        table = pa.table(data)

    pq.write_table(table, path)
    return path


def create_pk_parquet(ids: List[int], path: Path) -> Path:
    """
    Create a Parquet file with just an 'id' column for delete operations.

    Args:
        ids: List of primary key IDs to delete
        path: Output file path

    Returns:
        Path to created file

    Example:
        create_pk_parquet([1, 2, 3, 4, 5], Path('/tmp/delete_ids.parquet'))
    """
    return create_parquet({'id': ids}, path)


def read_parquet(path: Path) -> Dict[str, List[Any]]:
    """
    Read a Parquet file to column-oriented dict.

    Args:
        path: Path to Parquet file

    Returns:
        Dict mapping column names to lists of values

    Example:
        data = read_parquet(Path('/tmp/sites.parquet'))
        for name, slug in zip(data['name'], data['slug']):
            print(f"{name}: {slug}")
    """
    table = pq.read_table(path)
    return {col: table[col].to_pylist() for col in table.column_names}


def read_parquet_table(path: Path) -> pa.Table:
    """
    Read a Parquet file as a PyArrow Table.

    Useful when you need the full PyArrow functionality.

    Args:
        path: Path to Parquet file

    Returns:
        PyArrow Table
    """
    return pq.read_table(path)


def parquet_row_count(path: Path) -> int:
    """
    Get row count from a Parquet file without loading all data.

    Args:
        path: Path to Parquet file

    Returns:
        Number of rows
    """
    metadata = pq.read_metadata(path)
    return metadata.num_rows


def merge_parquet_files(input_paths: List[Path], output_path: Path) -> Path:
    """
    Merge multiple Parquet files into one.

    All input files must have the same schema.

    Args:
        input_paths: List of Parquet file paths to merge
        output_path: Output file path

    Returns:
        Path to merged file
    """
    tables = [pq.read_table(p) for p in input_paths]
    merged = pa.concat_tables(tables)
    pq.write_table(merged, output_path)
    return output_path


# Schema helpers for common NetBox types

def netbox_schema_site() -> pa.Schema:
    """Return PyArrow schema for dcim.site."""
    return pa.schema([
        ('name', pa.string()),
        ('slug', pa.string()),
        ('status', pa.string()),
        ('facility', pa.string()),
        ('description', pa.string()),
    ])


def netbox_schema_device() -> pa.Schema:
    """Return PyArrow schema for dcim.device."""
    return pa.schema([
        ('name', pa.string()),
        ('device_type', pa.int64()),  # FK
        ('role', pa.int64()),  # FK
        ('site', pa.int64()),  # FK
        ('rack', pa.int64()),  # FK (nullable)
        ('position', pa.float64()),  # Nullable
        ('face', pa.string()),  # 'front' or 'rear'
        ('status', pa.string()),
        ('serial', pa.string()),
        ('asset_tag', pa.string()),
        ('custom_field_data', pa.string()),  # JSON
    ])


def netbox_schema_interface() -> pa.Schema:
    """Return PyArrow schema for dcim.interface."""
    return pa.schema([
        ('device', pa.int64()),  # FK
        ('name', pa.string()),
        ('type', pa.string()),  # e.g., '400gbase-x-qsfpdd'
        ('enabled', pa.bool_()),
        ('description', pa.string()),
    ])


def netbox_schema_cable() -> pa.Schema:
    """Return PyArrow schema for dcim.cable."""
    return pa.schema([
        ('type', pa.string()),  # e.g., 'cat6a', 'mmf-om4'
        ('status', pa.string()),  # 'connected', 'planned'
        ('label', pa.string()),
        ('color', pa.string()),  # Hex color code
        ('length', pa.float64()),
        ('length_unit', pa.string()),  # 'm', 'ft', 'cm'
    ])


def netbox_schema_cable_termination() -> pa.Schema:
    """Return PyArrow schema for dcim.cabletermination."""
    return pa.schema([
        ('cable', pa.int64()),  # FK to Cable
        ('cable_end', pa.string()),  # 'A' or 'B'
        ('termination_type', pa.int64()),  # FK to ContentType
        ('termination_id', pa.int64()),  # PK of terminating object
    ])
