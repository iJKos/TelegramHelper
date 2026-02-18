"""
utils.duckdb.messages

Synchronous database helpers for read_messages and sent_messages tables.
All write operations use thread-safe locking via get_write_connection().
"""

from typing import List, Optional

from utils.duckdb.connection import get_read_connection, get_write_connection
from utils.models import ReadMessage, SentMessage


def _row_to_read_message(row_dict: dict) -> ReadMessage:
    return ReadMessage(
        id=str(row_dict.get('id')) if row_dict.get('id') else None,
        telegram_id=row_dict.get('telegram_id'),
        channel_id=row_dict.get('channel_id'),
        author=row_dict.get('author'),
        public_link=row_dict.get('public_link'),
        raw_text=row_dict.get('raw_text'),
        text=row_dict.get('text'),
        msg_dttm=row_dict.get('msg_dttm'),
        urls=row_dict.get('urls'),
        summary=row_dict.get('summary'),
        hashtags=row_dict.get('hashtags'),
        headline=row_dict.get('headline'),
        state=row_dict.get('state') or 'read',
        read_at=row_dict.get('read_at'),
        error=row_dict.get('error'),
        sent_message_id=str(row_dict.get('sent_message_id')) if row_dict.get('sent_message_id') else None,
    )


def _row_to_sent_message(row_dict: dict) -> SentMessage:
    return SentMessage(
        id=str(row_dict.get('id')) if row_dict.get('id') else None,
        telegram_id=row_dict.get('telegram_id'),
        text=row_dict.get('text'),
        read_message_id=str(row_dict.get('read_message_id')) if row_dict.get('read_message_id') else None,
        message_dttm=row_dict.get('message_dttm'),
        state=row_dict.get('state') or 'to_send',
        sent_at=row_dict.get('sent_at'),
        error=row_dict.get('error'),
        emodji_count=row_dict.get('emodji_count'),
        normalized_score=row_dict.get('normalized_score'),
    )


def _execute_select_and_map(query: str, params: list, mapper):
    with get_read_connection() as con:
        rows = con.execute(query, params).fetchall()
        columns = [desc[0] for desc in con.description]
        return [mapper(dict(zip(columns, row))) for row in rows]


# ============== READ MESSAGES ==============


def insert_read_message(msg: ReadMessage) -> str:
    with get_write_connection() as con:
        cur = con.cursor()
        urls = msg.urls if isinstance(msg.urls, list) else ([msg.urls] if msg.urls else [])
        cur.execute(
            """
            INSERT INTO read_messages (telegram_id, channel_id, author, public_link, raw_text, text, msg_dttm, urls, summary, hashtags, headline, state, error, sent_message_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            [
                msg.telegram_id,
                msg.channel_id,
                msg.author,
                msg.public_link,
                msg.raw_text,
                msg.text,
                msg.msg_dttm,
                urls,
                msg.summary,
                msg.hashtags,
                msg.headline,
                getattr(msg, 'state', 'read'),
                msg.error,
                msg.sent_message_id,
            ],
        )
        insert_id = cur.fetchone()[0]
        cur.close()
        return str(insert_id)


def get_read_messages(from_date=None, to_date=None, state: str = None, limit=1000) -> List[ReadMessage]:
    query = 'SELECT * FROM read_messages'
    params = []
    where_clauses = []
    if state:
        where_clauses.append('state = ?')
        params.append(state)
    if from_date:
        where_clauses.append('msg_dttm >= ?')
        params.append(from_date)
    if to_date:
        where_clauses.append('msg_dttm <= ?')
        params.append(to_date)
    if where_clauses:
        query += ' WHERE ' + ' AND '.join(where_clauses)
    query += ' ORDER BY msg_dttm DESC LIMIT ?'
    params.append(limit)
    return _execute_select_and_map(query, params, _row_to_read_message)


def get_messages_by_state(state: str, from_date=None, limit=1000, min_text_length=0) -> List[ReadMessage]:
    query = 'SELECT * FROM read_messages WHERE state = ?'
    params = [state]
    if from_date:
        query += ' AND msg_dttm >= ?'
        params.append(from_date)
    if min_text_length > 0:
        query += ' AND LENGTH(text) >= ?'
        params.append(min_text_length)
    query += ' ORDER BY msg_dttm ASC LIMIT ?'
    params.append(limit)
    return _execute_select_and_map(query, params, _row_to_read_message)


def update_read_message_parsed(msg: ReadMessage, set_state: str = None):
    with get_write_connection() as con:
        cur = con.cursor()
        urls = msg.urls if isinstance(msg.urls, list) else ([msg.urls] if msg.urls else [])
        cur.execute(
            'UPDATE read_messages SET text = ?, urls = ?, summary = ?, hashtags = ?, headline = ? WHERE id = ?',
            [msg.text, urls, msg.summary, msg.hashtags, msg.headline, msg.id],
        )
        if set_state:
            cur.execute('UPDATE read_messages SET state = ? WHERE id = ?', [set_state, msg.id])
        cur.close()


def link_read_message_to_sent(read_message_id: str, sent_message_id: str, set_state: str = None):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE read_messages SET sent_message_id = ? WHERE id = ?', [sent_message_id, read_message_id])
        if set_state:
            cur.execute('UPDATE read_messages SET state = ? WHERE id = ?', [set_state, read_message_id])
        cur.close()


def update_read_message_error(read_id: str, error: str):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE read_messages SET error = ?, state = ? WHERE id = ?', [error, 'error', read_id])
        cur.close()


def get_summarized_unlinked_messages(from_date=None, limit=1000) -> List[ReadMessage]:
    query = "SELECT * FROM read_messages WHERE state = 'summarized' AND sent_message_id IS NULL"
    params = []
    if from_date:
        query += ' AND msg_dttm >= ?'
        params.append(from_date)
    query += ' ORDER BY msg_dttm ASC LIMIT ?'
    params.append(limit)
    return _execute_select_and_map(query, params, _row_to_read_message)


def message_exists(telegram_id: int, channel_id: str) -> bool:
    with get_read_connection() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT 1 FROM read_messages WHERE telegram_id = ? AND channel_id = ? LIMIT 1',
            [telegram_id, channel_id],
        )
        exists = cur.fetchone() is not None
        cur.close()
        return exists


def get_read_message_by_id(read_id: str) -> Optional[ReadMessage]:
    with get_read_connection() as con:
        cur = con.cursor()
        cur.execute('SELECT * FROM read_messages WHERE id = ?', [read_id])
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        columns = [desc[0] for desc in cur.description]
        msg = _row_to_read_message(dict(zip(columns, row)))
        cur.close()
        return msg


# ============== SENT MESSAGES ==============


def insert_sent_message(msg: SentMessage) -> str:
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO sent_messages (telegram_id, text, read_message_id, message_dttm, state, sent_at, error, emodji_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            [
                msg.telegram_id,
                msg.text,
                msg.read_message_id,
                msg.message_dttm,
                getattr(msg, 'state', 'to_send'),
                msg.sent_at,
                msg.error,
                msg.emodji_count,
            ],
        )
        insert_id = cur.fetchone()[0]
        cur.close()
        return str(insert_id)


def get_sent_messages(from_date=None, to_date=None, state: str = None, limit=1000, order_asc=False) -> List[SentMessage]:
    query = 'SELECT * FROM sent_messages'
    params = []
    where_clauses = []
    if state:
        where_clauses.append('state = ?')
        params.append(state)
    if from_date:
        where_clauses.append('sent_at >= ?')
        params.append(from_date)
    if to_date:
        where_clauses.append('sent_at <= ?')
        params.append(to_date)
    if where_clauses:
        query += ' WHERE ' + ' AND '.join(where_clauses)
    order_dir = 'ASC' if order_asc else 'DESC'
    query += f' ORDER BY message_dttm {order_dir} LIMIT ?'
    params.append(limit)
    return _execute_select_and_map(query, params, _row_to_sent_message)


def get_sent_messages_by_states(states: list, from_date=None, limit=1000) -> List[SentMessage]:
    if not states:
        return []
    placeholders = ', '.join(['?' for _ in states])
    query = f'SELECT * FROM sent_messages WHERE state IN ({placeholders})'
    params = list(states)
    if from_date:
        query += ' AND sent_at >= ?'
        params.append(from_date)
    query += ' ORDER BY sent_at DESC LIMIT ?'
    params.append(limit)
    return _execute_select_and_map(query, params, _row_to_sent_message)


def update_sent_message_state(sent_id: str, new_state: str):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE sent_messages SET state = ? WHERE id = ?', [new_state, sent_id])
        cur.close()


def update_sent_message_text(sent_id: str, text: str):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE sent_messages SET text = ? WHERE id = ?', [text, sent_id])
        cur.close()


def update_sent_message_telegram_id(sent_id: str, telegram_id: int):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute(
            'UPDATE sent_messages SET telegram_id = ?, sent_at = current_timestamp, state = ? WHERE id = ?',
            [telegram_id, 'sent', sent_id],
        )
        cur.close()


def update_sent_message_error(sent_id: str, error: str):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE sent_messages SET error = ?, state = ? WHERE id = ?', [error, 'error', sent_id])
        cur.close()


def update_sent_message_emodji_count(sent_id: str, emodji_count: int):
    with get_write_connection() as con:
        cur = con.cursor()
        cur.execute('UPDATE sent_messages SET emodji_count = ? WHERE id = ?', [emodji_count, sent_id])
        cur.close()


def get_sent_message_by_telegram_id(telegram_id: int) -> Optional[SentMessage]:
    with get_read_connection() as con:
        cur = con.cursor()
        cur.execute('SELECT * FROM sent_messages WHERE telegram_id = ?', [telegram_id])
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        columns = [desc[0] for desc in cur.description]
        msg = _row_to_sent_message(dict(zip(columns, row)))
        cur.close()
        return msg


def get_max_read_message_date():
    """Returns max msg_dttm from read_messages or None if table is empty."""
    with get_read_connection() as con:
        cur = con.cursor()
        cur.execute('SELECT MAX(msg_dttm) FROM read_messages')
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else None


def get_read_messages_by_sent_id(sent_message_id: str) -> List[ReadMessage]:
    """Returns all read messages linked to a sent message."""
    query = 'SELECT * FROM read_messages WHERE sent_message_id = ? ORDER BY msg_dttm ASC'
    return _execute_select_and_map(query, [sent_message_id], _row_to_read_message)


def get_sent_messages_for_dedup(from_date, limit=10000) -> List[SentMessage]:
    """
    Returns sent messages for deduplication check.
    Selects by message_dttm (original message date), ignoring state.
    """
    query = 'SELECT * FROM sent_messages WHERE message_dttm >= ? ORDER BY message_dttm DESC LIMIT ?'
    return _execute_select_and_map(query, [from_date, limit], _row_to_sent_message)


# ============== BATCH OPERATIONS ==============


def batch_insert_read_messages(messages: List[ReadMessage]) -> List[str]:
    """Insert multiple read messages in a single transaction. Returns list of inserted IDs."""
    if not messages:
        return []
    with get_write_connection() as con:
        cur = con.cursor()
        inserted_ids = []
        for msg in messages:
            urls = msg.urls if isinstance(msg.urls, list) else ([msg.urls] if msg.urls else [])
            cur.execute(
                """
                INSERT INTO read_messages (telegram_id, channel_id, author, public_link, raw_text, text, msg_dttm, urls, summary, hashtags, headline, state, error, sent_message_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                [
                    msg.telegram_id,
                    msg.channel_id,
                    msg.author,
                    msg.public_link,
                    msg.raw_text,
                    msg.text,
                    msg.msg_dttm,
                    urls,
                    msg.summary,
                    msg.hashtags,
                    msg.headline,
                    getattr(msg, 'state', 'read'),
                    msg.error,
                    msg.sent_message_id,
                ],
            )
            inserted_ids.append(str(cur.fetchone()[0]))
        cur.close()
        return inserted_ids


def batch_update_read_messages_parsed(messages: List[ReadMessage], set_state: str = None):
    """Update multiple read messages (text, urls, summary, hashtags, headline) in a single transaction."""
    if not messages:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for msg in messages:
            urls = msg.urls if isinstance(msg.urls, list) else ([msg.urls] if msg.urls else [])
            cur.execute(
                'UPDATE read_messages SET text = ?, urls = ?, summary = ?, hashtags = ?, headline = ? WHERE id = ?',
                [msg.text, urls, msg.summary, msg.hashtags, msg.headline, msg.id],
            )
            if set_state:
                cur.execute('UPDATE read_messages SET state = ? WHERE id = ?', [set_state, msg.id])
        cur.close()


def batch_update_read_messages_error(error_updates: List[tuple]):
    """Update multiple read messages with errors. error_updates is list of (id, error_text)."""
    if not error_updates:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for read_id, error in error_updates:
            cur.execute('UPDATE read_messages SET error = ?, state = ? WHERE id = ?', [error, 'error', read_id])
        cur.close()


def batch_insert_sent_messages(messages: List[SentMessage]) -> List[str]:
    """Insert multiple sent messages in a single transaction. Returns list of inserted IDs."""
    if not messages:
        return []
    with get_write_connection() as con:
        cur = con.cursor()
        inserted_ids = []
        for msg in messages:
            cur.execute(
                """
                INSERT INTO sent_messages (telegram_id, text, read_message_id, message_dttm, state, sent_at, error, emodji_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                [
                    msg.telegram_id,
                    msg.text,
                    msg.read_message_id,
                    msg.message_dttm,
                    getattr(msg, 'state', 'to_send'),
                    msg.sent_at,
                    msg.error,
                    msg.emodji_count,
                ],
            )
            inserted_ids.append(str(cur.fetchone()[0]))
        cur.close()
        return inserted_ids


def batch_link_read_messages_to_sent(links: List[tuple], set_state: str = None):
    """Link multiple read messages to sent messages. links is list of (read_message_id, sent_message_id)."""
    if not links:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for read_message_id, sent_message_id in links:
            cur.execute('UPDATE read_messages SET sent_message_id = ? WHERE id = ?', [sent_message_id, read_message_id])
            if set_state:
                cur.execute('UPDATE read_messages SET state = ? WHERE id = ?', [set_state, read_message_id])
        cur.close()


def batch_update_sent_messages_state(updates: List[tuple]):
    """Update state for multiple sent messages. updates is list of (sent_id, new_state)."""
    if not updates:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for sent_id, new_state in updates:
            cur.execute('UPDATE sent_messages SET state = ? WHERE id = ?', [new_state, sent_id])
        cur.close()


def batch_update_sent_messages_text(updates: List[tuple]):
    """Update text for multiple sent messages. updates is list of (sent_id, text)."""
    if not updates:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for sent_id, text in updates:
            cur.execute('UPDATE sent_messages SET text = ? WHERE id = ?', [text, sent_id])
        cur.close()


def batch_update_sent_messages_emodji(updates: List[tuple]):
    """Update emodji_count and normalized_score for multiple sent messages.

    updates is list of (sent_id, emodji_count, normalized_score).
    """
    if not updates:
        return
    with get_write_connection() as con:
        cur = con.cursor()
        for sent_id, emodji_count, normalized_score in updates:
            cur.execute(
                'UPDATE sent_messages SET emodji_count = ?, normalized_score = ? WHERE id = ?',
                [emodji_count, normalized_score, sent_id],
            )
        cur.close()


def get_existing_message_keys(telegram_ids_and_channels: List[tuple]) -> set:
    """Check which (telegram_id, channel_id) pairs already exist. Returns set of existing pairs."""
    if not telegram_ids_and_channels:
        return set()
    with get_read_connection() as con:
        cur = con.cursor()
        existing = set()
        # Query in batches to avoid too long queries
        for telegram_id, channel_id in telegram_ids_and_channels:
            cur.execute(
                'SELECT 1 FROM read_messages WHERE telegram_id = ? AND channel_id = ? LIMIT 1',
                [telegram_id, channel_id],
            )
            if cur.fetchone():
                existing.add((telegram_id, channel_id))
        cur.close()
        return existing


def batch_get_read_messages_by_ids(read_ids: List[str]) -> dict:
    """Get multiple read messages by IDs in a single query. Returns dict {id: ReadMessage}."""
    if not read_ids:
        return {}
    placeholders = ', '.join(['?' for _ in read_ids])
    query = f'SELECT * FROM read_messages WHERE id IN ({placeholders})'
    messages = _execute_select_and_map(query, read_ids, _row_to_read_message)
    return {msg.id: msg for msg in messages}


def batch_get_read_messages_by_sent_ids(sent_ids: List[str]) -> dict:
    """Get all read messages linked to sent messages. Returns dict {sent_id: [ReadMessage, ...]}."""
    if not sent_ids:
        return {}
    placeholders = ', '.join(['?' for _ in sent_ids])
    query = f'SELECT * FROM read_messages WHERE sent_message_id IN ({placeholders}) ORDER BY msg_dttm ASC'
    messages = _execute_select_and_map(query, sent_ids, _row_to_read_message)
    result = {sid: [] for sid in sent_ids}
    for msg in messages:
        if msg.sent_message_id in result:
            result[msg.sent_message_id].append(msg)
    return result
