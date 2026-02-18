import json
import httpx
import asyncio
from datetime import datetime
from typing import List

from jinja2 import Environment, FileSystemLoader
from telethon.tl.types import PeerChannel, ReactionPaid

import config
from utils.models import SentMessage
from utils.sqlite.messages import (
    insert_sent_message,
    update_sent_message_error,
    update_sent_message_telegram_id,
    update_sent_message_state,
    get_top_sent_messages_by_score,
    batch_get_read_messages_by_ids,
)

# Jinja2 template environment for digest
_jinja_env = Environment(loader=FileSystemLoader('static'), autoescape=False)


async def send_sent_message(sent_msg: SentMessage, channel_id, bot_token=config.bot_token) -> SentMessage:
    """
    Отправляет НОВОЕ сообщение в Telegram через Bot API.

    Логика:
        1. Если sent_msg.id отсутствует — сначала вставляет запись в БД
        2. Отправляет сообщение через sendMessage API
        3. При успехе — сохраняет telegram_id в БД
        4. При ошибке — записывает error в БД

    Args:
        sent_msg: сообщение для отправки
        channel_id: ID канала (строка или число)
        bot_token: токен бота Telegram

    Returns:
        SentMessage: обновлённый объект с telegram_id или error
    """
    if not sent_msg.id:
        insert_id = await asyncio.to_thread(insert_sent_message, sent_msg)
        sent_msg.id = insert_id

    error = None
    telegram_id = None
    try:
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        data = {
            'chat_id': channel_id,
            'text': sent_msg.text,
            'parse_mode': 'HTML',
            'link_preview_options': json.dumps({'is_disabled': True}),
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data, timeout=30.0)
        result = response.json()
        if result.get('ok'):
            telegram_id = result.get('result', {}).get('message_id')
        else:
            error = result.get('description', 'Unknown error')
    except Exception as e:
        error = str(e)

    if telegram_id is not None and sent_msg.id:
        await asyncio.to_thread(update_sent_message_telegram_id, sent_msg.id, telegram_id)
        sent_msg.telegram_id = telegram_id
        sent_msg.state = 'sent'
    if error and sent_msg.id:
        await asyncio.to_thread(update_sent_message_error, sent_msg.id, error)
        sent_msg.error = error

    return sent_msg


async def update_sent_message_in_telegram(sent_msg: SentMessage, channel_id, bot_token=config.bot_token) -> SentMessage:
    """
    Редактирует существующее сообщение в Telegram через editMessageText API.

    Требования:
        - sent_msg.telegram_id должен быть установлен
        - Сообщение должно существовать в канале

    Args:
        sent_msg: сообщение с обновлённым текстом
        channel_id: ID канала
        bot_token: токен бота

    Returns:
        SentMessage: обновлённый объект (state='sent' или error)
    """
    if not sent_msg.telegram_id:
        config.logger.warning(f'Cannot update message {sent_msg.id}: no telegram_id')
        await asyncio.to_thread(update_sent_message_error, sent_msg.id, 'No telegram_id for update')
        return sent_msg

    error = None
    try:
        url = f'https://api.telegram.org/bot{bot_token}/editMessageText'
        data = {
            'chat_id': channel_id,
            'message_id': sent_msg.telegram_id,
            'text': sent_msg.text,
            'parse_mode': 'HTML',
            'link_preview_options': json.dumps({'is_disabled': True}),
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data, timeout=30.0)
        result = response.json()
        if not result.get('ok'):
            error = result.get('description', 'Unknown error')
    except Exception as e:
        error = str(e)

    if error and sent_msg.id:
        await asyncio.to_thread(update_sent_message_error, sent_msg.id, error)
        sent_msg.error = error
    elif sent_msg.id:
        await asyncio.to_thread(update_sent_message_state, sent_msg.id, 'sent')
        sent_msg.state = 'sent'

    return sent_msg


async def send_or_update_message(sent_msg: SentMessage, channel_id, bot_token=config.bot_token) -> SentMessage:
    """
    Отправляет новое или редактирует существующее сообщение.

    Выбор действия:
        - Если telegram_id есть → editMessageText (обновление)
        - Если telegram_id нет → sendMessage (новое)

    Returns:
        SentMessage: результат операции
    """
    if sent_msg.telegram_id:
        # Has telegram_id - update existing message
        return await update_sent_message_in_telegram(sent_msg, channel_id, bot_token)
    else:
        # No telegram_id - send as new message
        return await send_sent_message(sent_msg, channel_id, bot_token)


async def read_message_reactions_telethon(client, channel_id, telegram_id: int) -> int:
    """
    Читает количество реакций на сообщение через Telethon.

    Особенности:
        - В mock режиме возвращает 0
        - Обрабатывает разные форматы channel_id (username, -100..., int)
        - Суммирует все типы реакций (эмодзи)

    Args:
        client: подключённый Telethon клиент
        channel_id: ID канала в любом формате
        telegram_id: ID сообщения в Telegram

    Returns:
        int: суммарное количество реакций или 0 при ошибке
    """
    if config.is_mock:
        config.logger.info(f'MOCK READ REACTIONS for message {telegram_id}')
        return 0

    try:
        # Convert channel_id to proper format for Telethon
        # Can be: username (@channel), numeric string ("-1001790123464"), or int
        if isinstance(channel_id, str):
            if channel_id.startswith('@') or not channel_id.lstrip('-').isdigit():
                # Username like @moderndatastack_ru - use as-is
                entity = channel_id
            else:
                # Numeric string like "-1001790123464"
                channel_id_int = int(channel_id)
                if channel_id_int < 0:
                    # Remove -100 prefix to get actual channel ID for PeerChannel
                    actual_channel_id = abs(channel_id_int)
                    if actual_channel_id > 10**12:
                        # Has -100 prefix (e.g., -1001790123464 -> 1790123464)
                        actual_channel_id = actual_channel_id - 10**12
                    entity = PeerChannel(actual_channel_id)
                else:
                    entity = channel_id_int
        elif isinstance(channel_id, int) and channel_id < 0:
            actual_channel_id = abs(channel_id)
            if actual_channel_id > 10**12:
                actual_channel_id = actual_channel_id - 10**12
            entity = PeerChannel(actual_channel_id)
        else:
            entity = channel_id

        messages = await client.get_messages(entity, ids=[telegram_id])
        if not messages or not messages[0]:
            return 0

        message = messages[0]
        reactions = getattr(message, 'reactions', None)
        if not reactions:
            return 0

        total = 0
        for result in getattr(reactions, 'results', []):
            reaction = getattr(result, 'reaction', None)
            if isinstance(reaction, ReactionPaid):
                continue
            total += getattr(result, 'count', 0)

        return total
    except Exception:
        config.logger.exception(f'Error reading reactions for message {telegram_id} in channel {channel_id}')
        return 0


# Веса для разных типов реакций
REACTION_WEIGHTS = {
    '🔥': 10,
    '❤': 5,
    '❤️': 5,
    '👍': 1,
    '👎': -1,
    '💩': -5,
    '🤮': -10,
}
DEFAULT_REACTION_WEIGHT = 1


async def read_message_reactions_weighted(client, channel_id, telegram_id: int) -> int:
    """
    Читает реакции на сообщение и возвращает взвешенную сумму.

    Веса: 🔥=+10, ❤=+5, 👍=+1, 👎=-1, 💩=-5, 🤮=-10, остальные=+1.

    Args:
        client: подключённый Telethon клиент
        channel_id: ID канала в любом формате
        telegram_id: ID сообщения в Telegram

    Returns:
        int: взвешенная сумма реакций или 0 при ошибке
    """
    if config.is_mock:
        config.logger.info(f'MOCK READ WEIGHTED REACTIONS for message {telegram_id}')
        return 0

    try:
        if isinstance(channel_id, str):
            if channel_id.startswith('@') or not channel_id.lstrip('-').isdigit():
                entity = channel_id
            else:
                channel_id_int = int(channel_id)
                if channel_id_int < 0:
                    actual_channel_id = abs(channel_id_int)
                    if actual_channel_id > 10**12:
                        actual_channel_id = actual_channel_id - 10**12
                    entity = PeerChannel(actual_channel_id)
                else:
                    entity = channel_id_int
        elif isinstance(channel_id, int) and channel_id < 0:
            actual_channel_id = abs(channel_id)
            if actual_channel_id > 10**12:
                actual_channel_id = actual_channel_id - 10**12
            entity = PeerChannel(actual_channel_id)
        else:
            entity = channel_id

        messages = await client.get_messages(entity, ids=[telegram_id])
        if not messages or not messages[0]:
            return 0

        message = messages[0]
        reactions = getattr(message, 'reactions', None)
        if not reactions:
            return 0

        weighted_total = 0
        for result in getattr(reactions, 'results', []):
            reaction = getattr(result, 'reaction', None)
            if isinstance(reaction, ReactionPaid):
                continue
            count = getattr(result, 'count', 0)
            emoticon = getattr(reaction, 'emoticon', None) if reaction else None
            weight = REACTION_WEIGHTS.get(emoticon, DEFAULT_REACTION_WEIGHT)
            weighted_total += weight * count

        return weighted_total
    except Exception:
        config.logger.exception(f'Error reading weighted reactions for message {telegram_id} in channel {channel_id}')
        return 0


async def read_message_reactions_detailed(client, channel_id, telegram_id: int) -> list[tuple[str, int]]:
    """
    Читает реакции на сообщение и возвращает список пар (emoji, count).

    Args:
        client: подключённый Telethon клиент
        channel_id: ID канала в любом формате
        telegram_id: ID сообщения в Telegram

    Returns:
        list[(str, int)]: список пар (emoji, count) или пустой список
    """
    if config.is_mock:
        config.logger.info(f'MOCK READ DETAILED REACTIONS for message {telegram_id}')
        return []

    try:
        if isinstance(channel_id, str):
            if channel_id.startswith('@') or not channel_id.lstrip('-').isdigit():
                entity = channel_id
            else:
                channel_id_int = int(channel_id)
                if channel_id_int < 0:
                    actual_channel_id = abs(channel_id_int)
                    if actual_channel_id > 10**12:
                        actual_channel_id = actual_channel_id - 10**12
                    entity = PeerChannel(actual_channel_id)
                else:
                    entity = channel_id_int
        elif isinstance(channel_id, int) and channel_id < 0:
            actual_channel_id = abs(channel_id)
            if actual_channel_id > 10**12:
                actual_channel_id = actual_channel_id - 10**12
            entity = PeerChannel(actual_channel_id)
        else:
            entity = channel_id

        messages = await client.get_messages(entity, ids=[telegram_id])
        if not messages or not messages[0]:
            return []

        message = messages[0]
        reactions = getattr(message, 'reactions', None)
        if not reactions:
            return []

        result = []
        for r in getattr(reactions, 'results', []):
            count = getattr(r, 'count', 0)
            reaction = getattr(r, 'reaction', None)
            emoticon = getattr(reaction, 'emoticon', None) if reaction else None
            if emoticon and count > 0:
                result.append((emoticon, count))
        return result
    except Exception:
        config.logger.exception(f'Error reading detailed reactions for message {telegram_id} in channel {channel_id}')
        return []


async def send_or_update_sent_messages_concurrent(
    sent_messages: List[SentMessage],
    channel_id,
    concurrency: int = None,
) -> List[SentMessage]:
    """
    Отправляет/обновляет несколько сообщений параллельно.

    Особенности:
        - Использует Semaphore для ограничения параллелизма
        - Автоматически выбирает send или update по наличию telegram_id
        - При ошибке записывает её в БД и продолжает обработку

    Args:
        sent_messages: список сообщений для обработки
        channel_id: ID канала
        concurrency: макс. параллельных запросов (по умолчанию из config)

    Returns:
        List[SentMessage]: список обработанных сообщений
    """
    concurrency = concurrency or config.send_concurrency
    sem = asyncio.Semaphore(concurrency)

    async def _process_one(smsg: SentMessage):
        async with sem:
            try:
                return await send_or_update_message(smsg, channel_id)
            except Exception as e:
                config.logger.exception('Error processing message')
                try:
                    if smsg.id:
                        await asyncio.to_thread(update_sent_message_error, smsg.id, str(e))
                        smsg.error = str(e)
                except Exception:
                    config.logger.exception('Failed to persist error')
                return smsg

    tasks = [asyncio.create_task(_process_one(m)) for m in sent_messages]
    results = await asyncio.gather(*tasks)
    return results


def _get_message_link(telegram_id: int, channel_id: str) -> str:
    """
    Формирует ссылку на сообщение в Telegram.

    Формат: https://t.me/{channel_name}/{message_id}
    """
    # Remove @ from channel username if present
    channel_name = channel_id.lstrip('@') if channel_id.startswith('@') else channel_id
    return f'https://t.me/{channel_name}/{telegram_id}'


async def send_daily_digest(
    from_date: datetime,
    to_date: datetime,
    channel_id: str = None,
    bot_token: str = None,
    limit: int = 10,
) -> bool:
    """
    Отправляет ежедневный дайджест топ-новостей.

    Алгоритм:
        1. Получает топ сообщений по normalized_score за период
        2. Загружает связанные ReadMessage для получения заголовков
        3. Рендерит шаблон daily_digest_template.txt через Jinja2
        4. Отправляет сообщение в канал

    Args:
        from_date: начало периода (включительно)
        to_date: конец периода (исключительно)
        channel_id: канал для отправки (по умолчанию config.output_channel_id)
        bot_token: токен бота (по умолчанию config.bot_token)
        limit: максимум новостей в дайджесте (по умолчанию 10)

    Returns:
        bool: True если отправлено успешно, False при ошибке
    """
    channel_id = channel_id or config.output_channel_id
    bot_token = bot_token or config.bot_token

    # Get top messages by score
    top_messages = await asyncio.to_thread(
        get_top_sent_messages_by_score,
        from_date.isoformat(),
        to_date.isoformat(),
        limit,
    )

    if not top_messages:
        config.logger.info(f'No messages for digest from {from_date} to {to_date}')
        return False

    # Get linked read messages to get headlines
    read_msg_ids = [msg.read_message_id for msg in top_messages if msg.read_message_id]
    read_msgs_by_id = await asyncio.to_thread(batch_get_read_messages_by_ids, read_msg_ids)

    # Build items for template
    items = []
    for i, sent_msg in enumerate(top_messages, 1):
        read_msg = read_msgs_by_id.get(sent_msg.read_message_id)
        headline = read_msg.headline if read_msg and read_msg.headline else 'Без заголовка'
        link = _get_message_link(sent_msg.telegram_id, channel_id) if sent_msg.telegram_id else '#'
        emodji_count = sent_msg.emodji_count or 0

        # Format emodji count as emoji
        if emodji_count > 0:
            emodji_str = f'🔥 {emodji_count}'
        else:
            emodji_str = ''

        author = read_msg.author if read_msg and read_msg.author else None
        author_link = f'https://t.me/{author}' if author else None
        tags = read_msg.hashtags if read_msg and read_msg.hashtags else []
        tags_str = ' '.join(f'#{t.lstrip("#")}' for t in tags) if tags else ''

        items.append({
            'number': i,
            'headline': headline,
            'link': link,
            'emodji': emodji_str,
            'author': author,
            'author_link': author_link,
            'tags': tags_str,
        })

    # Render template
    try:
        template = _jinja_env.get_template('daily_digest_template.txt')
        text = template.render(
            date=from_date.strftime('%d.%m.%Y'),
            items=items,
        )
    except Exception as e:
        config.logger.exception(f'Failed to render digest template: {e}')
        return False

    # Send message
    try:
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        subscribe_link = f'https://t.me/{str(channel_id).lstrip("@")}'
        inline_keyboard = {
            'inline_keyboard': [[
                {'text': '\U0001f4e2 \u041f\u043e\u0434\u043f\u0438\u0441\u0430\u0442\u044c\u0441\u044f', 'url': subscribe_link},
            ]],
        }
        data = {
            'chat_id': channel_id,
            'text': text,
            'parse_mode': 'HTML',
            'link_preview_options': json.dumps({'is_disabled': True}),
            'reply_markup': json.dumps(inline_keyboard),
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data, timeout=30.0)
        result = response.json()
        if result.get('ok'):
            message_id = result['result']['message_id']
            config.logger.info(f'Daily digest sent successfully for {from_date.date()}, message_id={message_id}')

            # Pin the digest message
            try:
                pin_url = f'https://api.telegram.org/bot{bot_token}/pinChatMessage'
                pin_data = {
                    'chat_id': channel_id,
                    'message_id': message_id,
                    'disable_notification': True,
                }
                async with httpx.AsyncClient() as pin_client:
                    pin_response = await pin_client.post(pin_url, data=pin_data, timeout=30.0)
                pin_result = pin_response.json()
                if pin_result.get('ok'):
                    config.logger.info(f'Digest pinned successfully')
                else:
                    config.logger.warning(f'Failed to pin digest: {pin_result.get("description")}')
            except Exception as e:
                config.logger.warning(f'Error pinning digest: {e}')

            return True
        else:
            config.logger.error(f'Failed to send digest: {result.get("description")}')
            return False
    except Exception as e:
        config.logger.exception(f'Error sending daily digest: {e}')
        return False
