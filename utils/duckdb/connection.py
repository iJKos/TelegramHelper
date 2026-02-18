import os
import threading
from contextlib import contextmanager

import duckdb

from config import duckdb_path

# Thread lock for write operations to prevent concurrent writes
_write_lock = threading.Lock()


def get_db_path() -> str:
    """Get DB file path."""
    return f'{duckdb_path}.db'


def ensure_schema_once():
    """Ensure DB schema is present."""
    from utils.duckdb.schema import ensure_tables
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
    with _write_lock:
        con = duckdb.connect(db_path, read_only=False)
        try:
            ensure_tables(con)
        finally:
            con.close()


def get_connection(read_only=True):
    """Return a DuckDB connection."""
    db_path = get_db_path()
    return duckdb.connect(db_path, read_only=read_only)


@contextmanager
def get_write_connection():
    """
    Context manager for write operations with thread locking.
    Ensures only one thread can write at a time.
    """
    db_path = get_db_path()
    with _write_lock:
        con = duckdb.connect(db_path, read_only=False)
        try:
            yield con
        finally:
            con.close()


@contextmanager
def get_read_connection():
    """
    Context manager for read operations.
    Multiple threads can read concurrently.
    """
    db_path = get_db_path()
    con = duckdb.connect(db_path, read_only=True)
    try:
        yield con
    finally:
        con.close()
