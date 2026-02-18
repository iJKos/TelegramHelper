import logging
from utils.models import READ_MESSAGE_FIELDS, SENT_MESSAGE_FIELDS

logger = logging.getLogger(__name__)

# DuckDB to SQLite type mapping
TYPE_MAP = {
    'UUID': 'TEXT',
    'BIGINT': 'INTEGER',
    'TEXT': 'TEXT',
    'TIMESTAMP': 'TEXT',  # SQLite stores timestamps as TEXT in ISO format
    'VARCHAR[]': 'TEXT',  # Store arrays as JSON strings
    'VARCHAR': 'TEXT',
    'INTEGER': 'INTEGER',
    'FLOAT': 'REAL',
}


def _convert_type(duckdb_type: str) -> str:
    """
    Конвертирует тип DuckDB в SQLite.

    Маппинг: UUID→TEXT, BIGINT→INTEGER, TIMESTAMP→TEXT, VARCHAR[]→TEXT, FLOAT→REAL
    Также конвертирует gen_random_uuid() и current_timestamp.
    """
    # Handle DEFAULT clauses
    type_part = duckdb_type.split()[0]

    for duck_type, sqlite_type in TYPE_MAP.items():
        if type_part.upper().startswith(duck_type):
            # Preserve DEFAULT and other modifiers
            rest = duckdb_type[len(type_part):].strip()

            # Convert gen_random_uuid() to SQLite equivalent
            if 'gen_random_uuid()' in rest:
                rest = rest.replace('gen_random_uuid()', "(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))))")

            # Convert current_timestamp
            if 'current_timestamp' in rest.lower():
                rest = rest.replace('current_timestamp', "CURRENT_TIMESTAMP")

            return f'{sqlite_type} {rest}'.strip()

    return duckdb_type


def _build_create_table(table_name: str, fields: dict) -> str:
    """Генерирует SQL CREATE TABLE из словаря полей."""
    columns = []
    for name, definition in fields.items():
        sqlite_def = _convert_type(definition)
        columns.append(f'{name} {sqlite_def}')

    columns_sql = ',\n            '.join(columns)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
        )
    """


def ensure_tables(con):
    """
    Создаёт таблицы read_messages и sent_messages.

    Также выполняет миграции:
        - Добавляет отсутствующие колонки
        - Создаёт индексы для быстрого поиска
    """
    logger.info('Ensuring tables: read_messages, sent_messages')

    create_read = _build_create_table('read_messages', READ_MESSAGE_FIELDS)
    create_sent = _build_create_table('sent_messages', SENT_MESSAGE_FIELDS)

    cur = con.cursor()
    logger.debug('Executing CREATE TABLE statements (if not exists)')
    cur.execute(create_sent)
    cur.execute(create_read)

    # Migration: add missing columns
    def get_columns(tname):
        cur.execute(f"PRAGMA table_info('{tname}')")
        cols = [r[1] for r in cur.fetchall()]
        return cols

    try:
        sent_cols = get_columns('sent_messages')
        for name, definition in SENT_MESSAGE_FIELDS.items():
            if name not in sent_cols:
                logger.info("Adding missing column '%s' to 'sent_messages'", name)
                try:
                    sqlite_def = _convert_type(definition)
                    # SQLite doesn't support adding columns with some constraints
                    # Remove PRIMARY KEY and NOT NULL for ALTER TABLE
                    sqlite_def = sqlite_def.replace('PRIMARY KEY', '').strip()
                    cur.execute(f"ALTER TABLE sent_messages ADD COLUMN {name} {sqlite_def}")
                except Exception as e:
                    logger.warning("Failed to add column '%s' to 'sent_messages': %s", name, e)

        read_cols = get_columns('read_messages')
        for name, definition in READ_MESSAGE_FIELDS.items():
            if name not in read_cols:
                logger.info("Adding missing column '%s' to 'read_messages'", name)
                try:
                    sqlite_def = _convert_type(definition)
                    sqlite_def = sqlite_def.replace('PRIMARY KEY', '').strip()
                    cur.execute(f"ALTER TABLE read_messages ADD COLUMN {name} {sqlite_def}")
                except Exception as e:
                    logger.warning("Failed to add column '%s' to 'read_messages': %s", name, e)

        # Indexes for fast lookups
        logger.debug('Creating indexes for read_messages and sent_messages')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_read_msg_state ON read_messages(state)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_state ON sent_messages(state)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_read_msg_dt ON read_messages(msg_dttm)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_dt ON sent_messages(sent_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_message_dttm ON sent_messages(message_dttm)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_read_msg_sent_id ON read_messages(sent_message_id)')
        logger.info('Tables ensured and migrations applied (if any)')
    except Exception as e:
        logger.debug('Exception while applying migrations/indexes: %s', e)
    cur.close()
