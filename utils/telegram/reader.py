import re
from typing import List

from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import PeerChannel

from config import logger
from utils.models import ReadMessage


async def get_channel_subscribers_count(client, channel_id) -> int:
    """
    Получает количество подписчиков канала.

    Обрабатывает разные форматы channel_id:
        - Username (@channel)
        - Числовой ID (-1001234567890)
        - PeerChannel

    Returns:
        int: количество подписчиков или 0 при ошибке
    """
    try:
        # Convert channel_id to proper format
        if isinstance(channel_id, str):
            if channel_id.startswith('@') or not channel_id.lstrip('-').isdigit():
                entity = await client.get_entity(channel_id)
            else:
                channel_id_int = int(channel_id)
                if channel_id_int < 0:
                    actual_id = abs(channel_id_int)
                    if actual_id > 10**12:
                        actual_id = actual_id - 10**12
                    entity = PeerChannel(actual_id)
                else:
                    entity = channel_id_int
        elif isinstance(channel_id, int) and channel_id < 0:
            actual_id = abs(channel_id)
            if actual_id > 10**12:
                actual_id = actual_id - 10**12
            entity = PeerChannel(actual_id)
        else:
            entity = channel_id

        full_channel = await client(GetFullChannelRequest(entity))
        return full_channel.full_chat.participants_count or 0
    except Exception:
        logger.exception(f'Error getting subscribers for channel {channel_id}')
        return 0


async def get_folder_by_name(client, folder_name):
    """
    Находит папку Telegram по имени.

    Raises:
        ValueError: если папка не найдена
    """
    filters = await client(GetDialogFiltersRequest())
    for f in filters.filters or []:
        if f and getattr(getattr(f, 'title', None), 'text', None) == folder_name:
            return f
    raise ValueError(f"Folder '{folder_name}' not found")


async def get_channel_ids_from_folder(client, folder_name):
    """
    Получает ID всех каналов из папки.

    - Включает каналы (Channel) и чаты (Chat)
    - Исключает личные диалоги (User)
    - Возвращает marked IDs (с префиксом -100)

    Returns:
        set: множество channel_id
    """
    folder = await get_folder_by_name(client, folder_name)
    channel_ids = set()
    for peer in getattr(folder, 'include_peers', []):
        peer_type = type(peer).__name__
        # Only include channels and chats, skip users
        # InputPeerChannel/PeerChannel have channel_id
        # InputPeerChat/PeerChat have chat_id
        # InputPeerUser/PeerUser have user_id - skip these
        if 'Channel' in peer_type and hasattr(peer, 'channel_id'):
            # Use marked channel ID format (-100 prefix) for Telethon to correctly resolve
            marked_id = int(f'-100{peer.channel_id}')
            channel_ids.add(marked_id)
            logger.debug(f'Added channel: {peer.channel_id} -> {marked_id}')
        elif 'Chat' in peer_type and hasattr(peer, 'chat_id'):
            # Regular chats use negative IDs without -100 prefix
            marked_id = -peer.chat_id
            channel_ids.add(marked_id)
            logger.debug(f'Added chat: {peer.chat_id} -> {marked_id}')
        else:
            logger.debug(f'Skipping peer of type {peer_type} (not a channel/chat)')
    logger.info(f'Read {len(channel_ids)} channels/chats from {folder_name}')
    return channel_ids


async def fetch_raw_messages(client, channel_id, min_date, max_date=None) -> List[ReadMessage]:
    """
    Читает сообщения из Telegram канала за период.

    Фильтрация:
        - Пропускает сообщения с #реклама
        - Пропускает сообщения короче 100 символов
        - Читает от min_date до max_date (или до конца)

    Формирует public_link:
        - Для публичных каналов: https://t.me/{username}/{message_id}
        - Для приватных: None

    Args:
        client: подключённый Telethon клиент
        channel_id: ID канала (username, -100..., int)
        min_date: начальная дата (offset_date для iter_messages)
        max_date: конечная дата (опционально)

    Returns:
        List[ReadMessage]: список прочитанных сообщений
    """
    if not client.is_connected():
        await client.start()

    messages = []
    async for message in client.iter_messages(channel_id, reverse=True, offset_date=min_date):
        msg_time = message.date

        if msg_time.tzinfo is not None:
            msg_time = msg_time.replace(tzinfo=None)

        if max_date and msg_time > max_date:
            return messages

        raw_text = message.text or ''

        if '#реклама' in raw_text.lower():
            continue

        if len(raw_text) < 100:
            continue

        public_link = None
        username = None
        try:
            if isinstance(channel_id, str) and not channel_id.startswith('-100') and not channel_id.startswith('@'):
                public_link = f'https://t.me/{channel_id}/{message.id}'
                username = str(channel_id)
            elif isinstance(channel_id, str) and channel_id.startswith('@'):
                username = channel_id[1:]
                public_link = f'https://t.me/{username}/{message.id}'
            elif isinstance(channel_id, int) or (isinstance(channel_id, str) and channel_id.startswith('-100')):
                entity = await client.get_entity(channel_id)
                username = getattr(entity, 'username', None)
                if username:
                    public_link = f'https://t.me/{username}/{message.id}'
        except Exception:
            public_link = None
            username = None

        msg = ReadMessage(
            telegram_id=message.id,
            channel_id=str(channel_id),
            raw_text=raw_text,
            msg_dttm=msg_time,
            author=username,
            public_link=public_link,
        )

        messages.append(msg)

    return messages


def parse_message(msg: ReadMessage) -> ReadMessage:
    """
    Парсит сырое сообщение: извлекает URL, очищает текст.

    Операции:
        - Удаляет markdown-разметку (**)
        - Извлекает все URL из текста
        - Очищает URL от trailing символов
        - Удаляет URL и хэштеги из текста
        - Сохраняет результат в msg.text и msg.urls

    Returns:
        ReadMessage: тот же объект с заполненными text и urls
    """
    raw_text = msg.raw_text or ''

    text = raw_text.replace('**', '')

    found_urls = re.findall(r'https?://[^\s\)\]\}\>\(\[\{]+', text)
    clean_urls = set()
    for u in found_urls:
        u_clean = re.sub(r'[\)\]\}\>\.,;:_\]]+$', '', u)
        clean_urls.add(u_clean)
    urls = list(clean_urls)

    text = re.sub(r'https?://\S+', '', text).strip()
    text = re.sub(r'#\w+', '', text).strip()

    msg.text = text
    msg.urls = urls

    return msg
