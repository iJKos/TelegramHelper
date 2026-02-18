import logging
from utils.models import READ_MESSAGE_FIELDS, SENT_MESSAGE_FIELDS

logger = logging.getLogger(__name__)


def _build_create_table(table_name: str, fields: dict) -> str:
    """Генерирует SQL CREATE TABLE из словаря полей."""
    columns = ',\n            '.join(f'{name} {definition}' for name, definition in fields.items())
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
    """


def ensure_tables(con):
    """Создаёт таблицы read_messages и sent_messages при необходимости.

    Этот метод также выполняет лёгкую миграцию: добавляет отсутствующие
    колонки и создаёт индексы для ускорения выборок по состоянию/времени.
    """
    logger.info('Ensuring tables: read_messages, sent_messages')

    create_read = _build_create_table('read_messages', READ_MESSAGE_FIELDS)
    create_sent = _build_create_table('sent_messages', SENT_MESSAGE_FIELDS)

    cur = con.cursor()
    logger.debug('Executing CREATE TABLE statements (if not exists)')
    cur.execute(create_sent)
    cur.execute(create_read)

    # Миграция: добавляем отсутствующие колонки
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
                    cur.execute(f"ALTER TABLE sent_messages ADD COLUMN {name} {definition}")
                except Exception as e:
                    logger.warning("Failed to add column '%s' to 'sent_messages': %s", name, e)

        read_cols = get_columns('read_messages')
        for name, definition in READ_MESSAGE_FIELDS.items():
            if name not in read_cols:
                logger.info("Adding missing column '%s' to 'read_messages'", name)
                try:
                    cur.execute(f"ALTER TABLE read_messages ADD COLUMN {name} {definition}")
                except Exception as e:
                    logger.warning("Failed to add column '%s' to 'read_messages': %s", name, e)

        # Индексы для быстрого поиска
        logger.debug('Creating indexes for read_messages and sent_messages')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_read_msg_state ON read_messages(state)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_state ON sent_messages(state)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_read_msg_dt ON read_messages(msg_dttm)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_dt ON sent_messages(sent_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_sent_msg_message_dttm ON sent_messages(message_dttm)')
        logger.info('Tables ensured and migrations applied (if any)')
    except Exception as e:
        # duckdb may ignore index creation depending on version; ignore errors but log at debug
        logger.debug('Exception while applying migrations/indexes: %s', e)
    cur.close()