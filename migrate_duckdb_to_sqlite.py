#!/usr/bin/env python3
"""
Migration script: DuckDB -> SQLite

Migrates all data from DuckDB database to SQLite database.
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def migrate():
    # Import DuckDB modules
    from utils.duckdb.connection import get_read_connection as duckdb_read
    from utils.duckdb.connection import get_db_path as duckdb_path

    # Import SQLite modules
    from utils.sqlite.connection import ensure_schema_once as sqlite_ensure_schema
    from utils.sqlite.connection import get_write_connection as sqlite_write
    from utils.sqlite.connection import get_db_path as sqlite_path
    from utils.sqlite.messages import _serialize_list, _serialize_datetime

    log.info(f'Source DuckDB: {duckdb_path()}')
    log.info(f'Target SQLite: {sqlite_path()}')

    # Ensure SQLite schema exists
    log.info('Ensuring SQLite schema...')
    sqlite_ensure_schema()

    # Migrate read_messages
    log.info('Migrating read_messages...')
    with duckdb_read() as duckdb_con:
        rows = duckdb_con.execute('SELECT * FROM read_messages').fetchall()
        columns = [desc[0] for desc in duckdb_con.description]
        read_messages = [dict(zip(columns, row)) for row in rows]

    log.info(f'Found {len(read_messages)} read_messages to migrate')

    if read_messages:
        with sqlite_write() as sqlite_con:
            cur = sqlite_con.cursor()
            # Clear existing data
            cur.execute('DELETE FROM read_messages')

            inserted = 0
            for msg in read_messages:
                try:
                    # Serialize arrays and datetimes
                    urls = _serialize_list(msg.get('urls'))
                    hashtags = _serialize_list(msg.get('hashtags'))
                    msg_dttm = _serialize_datetime(msg.get('msg_dttm'))
                    read_at = _serialize_datetime(msg.get('read_at'))

                    cur.execute(
                        """
                        INSERT INTO read_messages (id, telegram_id, channel_id, author, public_link, raw_text, text, msg_dttm, urls, summary, hashtags, headline, state, read_at, error, sent_message_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            str(msg.get('id')) if msg.get('id') else None,
                            msg.get('telegram_id'),
                            msg.get('channel_id'),
                            msg.get('author'),
                            msg.get('public_link'),
                            msg.get('raw_text'),
                            msg.get('text'),
                            msg_dttm,
                            urls,
                            msg.get('summary'),
                            hashtags,
                            msg.get('headline'),
                            msg.get('state') or 'read',
                            read_at,
                            msg.get('error'),
                            str(msg.get('sent_message_id')) if msg.get('sent_message_id') else None,
                        ],
                    )
                    inserted += 1
                except Exception as e:
                    log.warning(f'Failed to insert read_message {msg.get("id")}: {e}')

            cur.close()
            log.info(f'Inserted {inserted} read_messages')

    # Migrate sent_messages
    log.info('Migrating sent_messages...')
    with duckdb_read() as duckdb_con:
        rows = duckdb_con.execute('SELECT * FROM sent_messages').fetchall()
        columns = [desc[0] for desc in duckdb_con.description]
        sent_messages = [dict(zip(columns, row)) for row in rows]

    log.info(f'Found {len(sent_messages)} sent_messages to migrate')

    if sent_messages:
        with sqlite_write() as sqlite_con:
            cur = sqlite_con.cursor()
            # Clear existing data
            cur.execute('DELETE FROM sent_messages')

            inserted = 0
            for msg in sent_messages:
                try:
                    message_dttm = _serialize_datetime(msg.get('message_dttm'))
                    sent_at = _serialize_datetime(msg.get('sent_at'))

                    cur.execute(
                        """
                        INSERT INTO sent_messages (id, telegram_id, text, read_message_id, message_dttm, state, sent_at, error, emodji_count, normalized_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            str(msg.get('id')) if msg.get('id') else None,
                            msg.get('telegram_id'),
                            msg.get('text'),
                            str(msg.get('read_message_id')) if msg.get('read_message_id') else None,
                            message_dttm,
                            msg.get('state') or 'to_send',
                            sent_at,
                            msg.get('error'),
                            msg.get('emodji_count'),
                            msg.get('normalized_score'),
                        ],
                    )
                    inserted += 1
                except Exception as e:
                    log.warning(f'Failed to insert sent_message {msg.get("id")}: {e}')

            cur.close()
            log.info(f'Inserted {inserted} sent_messages')

    log.info('Migration complete!')

    # Verify counts
    with sqlite_write() as sqlite_con:
        cur = sqlite_con.cursor()
        cur.execute('SELECT COUNT(*) FROM read_messages')
        read_count = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*) FROM sent_messages')
        sent_count = cur.fetchone()[0]
        cur.close()

    log.info(f'Verification: {read_count} read_messages, {sent_count} sent_messages in SQLite')


if __name__ == '__main__':
    try:
        migrate()
    except Exception as e:
        log.exception(f'Migration failed: {e}')
        sys.exit(1)
