import os
import sqlite3
import threading
from contextlib import contextmanager

from config import duckdb_path  # Reuse the same config path, just with different extension

# Thread lock for write operations to prevent concurrent writes
_write_lock = threading.Lock()


def get_db_path() -> str:
    """
    Возвращает путь к файлу SQLite БД.

    Формат: {duckdb_path}_sqlite.db
    """
    return f'{duckdb_path}_sqlite.db'


def ensure_schema_once():
    """
    Инициализирует схему БД (создаёт таблицы, индексы).

    Вызывается при старте приложения.
    """
    from utils.sqlite.schema import ensure_tables
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
    with _write_lock:
        con = sqlite3.connect(db_path)
        try:
            ensure_tables(con)
            con.commit()
        finally:
            con.close()


def get_connection():
    """Возвращает соединение SQLite для чтения/записи."""
    db_path = get_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row  # Enable dict-like access to rows
    return con


@contextmanager
def get_write_connection():
    """
    Контекстный менеджер для записи с блокировкой потока.

    Особенности:
        - Thread-safe через _write_lock
        - Автоматический commit при успехе
        - Автоматический rollback при ошибке

    Example:
        with get_write_connection() as con:
            cur = con.cursor()
            cur.execute('INSERT INTO ...')
            cur.close()
    """
    db_path = get_db_path()
    with _write_lock:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


@contextmanager
def get_read_connection():
    """
    Контекстный менеджер для чтения.

    Множество потоков могут читать одновременно.
    """
    db_path = get_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()
