from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


# Определения полей для синхронизации с БД
READ_MESSAGE_FIELDS = {
    'id': 'UUID DEFAULT gen_random_uuid() PRIMARY KEY',
    'telegram_id': 'BIGINT',
    'channel_id': 'TEXT',
    'author': 'TEXT',
    'public_link': 'TEXT',
    'raw_text': 'TEXT',
    'text': 'TEXT',
    'msg_dttm': 'TIMESTAMP',
    'urls': 'VARCHAR[]',
    'summary': 'TEXT',
    'hashtags': 'VARCHAR[]',
    'headline': 'VARCHAR',
    'state': "TEXT DEFAULT 'read'",
    'read_at': 'TIMESTAMP DEFAULT current_timestamp',
    'error': 'TEXT',
    'sent_message_id': 'UUID',
}

SENT_MESSAGE_FIELDS = {
    'id': 'UUID DEFAULT gen_random_uuid() PRIMARY KEY',
    'telegram_id': 'BIGINT',
    'text': 'TEXT',
    'read_message_id': 'UUID',
    'message_dttm': 'TIMESTAMP',  # Original message datetime from primary read_message
    'state': "TEXT DEFAULT 'to_send'",
    'sent_at': 'TIMESTAMP',
    'error': 'TEXT',
    'emodji_count': 'INTEGER',
    'normalized_score': 'FLOAT',  # (emodji_count / subscribers) * 100
    'sent_air': 'TIMESTAMP',  # When message was discussed on air
    'prediction_score': 'FLOAT',
    'bot_reaction': 'TEXT',
}


@dataclass
class ReadMessage:
    """Прочитанное сообщение из канала."""
    id: Optional[str] = None
    telegram_id: Optional[int] = None
    channel_id: Optional[str] = None
    author: Optional[str] = None
    public_link: Optional[str] = None
    raw_text: Optional[str] = None  # Оригинальный текст из Telegram
    text: Optional[str] = None  # Очищенный текст после парсинга
    msg_dttm: Optional[datetime] = None
    urls: Optional[List[str]] = None  # Извлечённые URL после парсинга
    summary: Optional[str] = None
    hashtags: Optional[List[str]] = None
    headline: Optional[str] = None
    state: str = 'read'
    read_at: Optional[datetime] = None
    error: Optional[str] = None
    sent_message_id: Optional[str] = None

    def to_json_lite(self):
        """Return a small JSON-friendly representation used for deduplication prompts."""
        return {
            'id': self.id or self.telegram_id,
            'headline': self.headline
        }


@dataclass
class SentMessage:
    """Отправленное (агрегированное) сообщение."""
    id: Optional[str] = None
    telegram_id: Optional[int] = None
    text: Optional[str] = None
    read_message_id: Optional[str] = None  # link to ReadMessage with summary, hashtags, headline
    message_dttm: Optional[datetime] = None  # Original message datetime from primary read_message
    state: str = 'to_send'
    sent_at: Optional[datetime] = None
    error: Optional[str] = None
    emodji_count: Optional[int] = None
    normalized_score: Optional[float] = None  # (emodji_count / subscribers) * 100
    sent_air: Optional[datetime] = None  # When message was discussed on air
    prediction_score: Optional[float] = None
    bot_reaction: Optional[str] = None
    # Не хранится в БД, заполняется при загрузке связанных сообщений
    read_messages: List['ReadMessage'] = field(default_factory=list)

    def to_json_lite(self):
        return {'id': self.id, 'headline': None}

