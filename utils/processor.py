import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from typing import List

from telethon import TelegramClient

import config
from utils.gpt_utils import check_pair_duplicate, deduplicate_messages_by_ai, summarize_message_text
from utils.models import ReadMessage, SentMessage
from utils.msg_helper import format_telegram_summary
from utils.scorer.reaction import choose_bot_reaction, compute_weighted_score_excluding_bot, set_message_reaction
from utils.scorer.scorer import NewsScorer
from utils.sqlite.messages import (
    batch_get_read_messages_by_ids,
    batch_get_read_messages_by_sent_ids,
    batch_insert_read_messages,
    batch_insert_sent_messages,
    batch_link_read_messages_to_sent,
    batch_update_read_messages_error,
    batch_update_read_messages_parsed,
    batch_update_sent_messages_emodji,
    batch_update_sent_messages_prediction,
    batch_update_sent_messages_state,
    batch_update_sent_messages_text,
    get_existing_message_keys,
    get_messages_by_state,
    get_sent_messages,
    get_sent_messages_by_states,
    get_sent_messages_for_dedup,
    get_sent_messages_for_training,
    get_summarized_unlinked_messages,
)
from utils.subscribers_cache import get_subscribers_cache
from utils.telegram.reader import (
    fetch_raw_messages,
    get_channel_ids_from_folder,
    get_channel_subscribers_count,
    parse_message,
)
from utils.telegram.sender import (
    read_message_reactions_detailed,
    read_message_reactions_telethon,
    read_message_reactions_weighted,
    send_daily_digest,
    send_or_update_sent_messages_concurrent,
)
from utils.text_similarity import find_similar_pairs

log = config.logger

# Step tracking callback (set by main.py cron_worker)
_step_callback = None


def set_step_callback(callback):
    global _step_callback
    _step_callback = callback


def _report_step(step: str):
    if _step_callback:
        _step_callback(step)


# Scorer singleton
_scorer_instance = None


def _get_scorer() -> NewsScorer:
    global _scorer_instance
    if _scorer_instance is None:
        data_dir = os.path.dirname(config.duckdb_path) or './data'
        _scorer_instance = NewsScorer(data_dir)
    return _scorer_instance


async def _train_scorer(client, scorer):
    """Обучает скорер на всех sent-сообщениях с реакциями."""
    training_sent = await asyncio.to_thread(get_sent_messages_for_training)
    if not training_sent:
        return 0

    train_read_ids = [s.read_message_id for s in training_sent if s.read_message_id]
    train_read_msgs = await asyncio.to_thread(batch_get_read_messages_by_ids, train_read_ids)

    training_data = []
    for sent in training_sent:
        if not sent.telegram_id:
            continue
        read_msg = train_read_msgs.get(sent.read_message_id) if sent.read_message_id else None
        if not read_msg:
            continue

        reactions = await read_message_reactions_detailed(
            client, config.output_channel_id, sent.telegram_id,
        )
        if not reactions:
            continue

        weighted = compute_weighted_score_excluding_bot(reactions, sent.bot_reaction)
        label = 1 if weighted > 0 else 0

        features = {
            'headline': read_msg.headline or '',
            'summary': read_msg.summary or '',
            'hashtags': read_msg.hashtags,
            'text_length': len(read_msg.text or ''),
            'msg_dttm': read_msg.msg_dttm,
        }
        training_data.append((features, label))

    if training_data:
        await asyncio.to_thread(scorer.train, training_data)

    return len(training_data)


# ============== STEP 1: READ AND SAVE RAW MESSAGES ==============

async def fetch_and_save_raw_messages(client: TelegramClient, from_datetime, to_datetime) -> int:
    """
    Step 1: Читает сырые сообщения из Telegram каналов и сохраняет в БД.

    - Получает список каналов из папки folder_name
    - Для каждого канала читает сообщения в указанном диапазоне дат
    - Фильтрует существующие сообщения по (telegram_id, channel_id)
    - Сохраняет новые сообщения с state='read'

    Args:
        client: Telethon клиент
        from_datetime: начало диапазона
        to_datetime: конец диапазона

    Returns:
        int: количество сохранённых сообщений
    """
    log.info(f'Step 1: Fetching raw messages from {from_datetime} to {to_datetime}')
    channel_ids = await get_channel_ids_from_folder(client, config.folder_name)
    log.info(f'Found {len(channel_ids)} channels in folder "{config.folder_name}"')

    all_new_messages = []
    for channel_id in channel_ids:
        raw_messages = await fetch_raw_messages(client, channel_id, min_date=from_datetime, max_date=to_datetime)
        if raw_messages:
            # Check which messages already exist
            keys_to_check = [(msg.telegram_id, msg.channel_id) for msg in raw_messages]
            existing_keys = await asyncio.to_thread(get_existing_message_keys, keys_to_check)
            # Filter out existing messages
            new_messages = [msg for msg in raw_messages if (msg.telegram_id, msg.channel_id) not in existing_keys]
            if new_messages:
                all_new_messages.extend(new_messages)
                log.info(f'  Channel {channel_id}: found {len(new_messages)} new messages')
        await asyncio.sleep(config.poll_delay)

    # Batch insert all new messages
    if all_new_messages:
        await asyncio.to_thread(batch_insert_read_messages, all_new_messages)

    log.info(f'Step 1 complete: saved {len(all_new_messages)} new messages total')
    return len(all_new_messages)


# ============== STEP 2: CLEAN MESSAGES ==============

async def parse_and_update_messages(limit=1000) -> int:
    """
    Step 2: Очищает сообщения: извлекает URL, нормализует текст.

    - Выбирает сообщения с state='read'
    - Удаляет markdown-разметку
    - Извлекает и очищает URL
    - Удаляет хэштеги из текста
    - Обновляет state='clean'

    Args:
        limit: максимальное количество сообщений для обработки

    Returns:
        int: количество обработанных сообщений
    """
    to_parse = await asyncio.to_thread(get_messages_by_state, 'read', None, limit)
    log.info(f'Step 2: Cleaning {len(to_parse)} messages')

    parsed_messages = [parse_message(msg) for msg in to_parse]

    if parsed_messages:
        await asyncio.to_thread(batch_update_read_messages_parsed, parsed_messages, set_state='clean')

    log.info(f'Step 2 complete: cleaned {len(to_parse)} messages')
    return len(to_parse)


# ============== STEP 3: SUMMARIZE AND TAG ==============


def extract_json_from_response(response: str) -> dict:
    """
    Извлекает JSON из ответа GPT, обрабатывая markdown code blocks.

    Args:
        response: строка ответа от GPT

    Returns:
        dict: распарсенный словарь или None при ошибке
    """
    if not response:
        return None

    text = response.strip()

    # Remove markdown code blocks if present
    if text.startswith('```'):
        lines = text.split('\n')
        # Remove first line (```json or ```)
        lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        text = '\n'.join(lines)

    # Try to find JSON object in the text
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]

    return json.loads(text)


async def summarize_read_message(msg: ReadMessage) -> ReadMessage:
    """
    Step 3: Суммаризирует сообщение через GPT.

    - Отправляет текст в GPT для суммаризации
    - Получает summary, headline, hashtags
    - Нормализует хэштеги (добавляет # если отсутствует)

    Args:
        msg: сообщение для суммаризации

    Returns:
        ReadMessage: обновлённое сообщение или None при ошибке
    """
    try:
        summary_json = await summarize_message_text(msg.text)
        if not summary_json or not summary_json.strip():
            log.warning(f'Empty response from GPT for message {msg.id}')
            return None

        summary_data = extract_json_from_response(summary_json)
        if summary_data is None:
            log.warning(f'Could not extract JSON from GPT response for message {msg.id}')
            return None

        msg.summary = summary_data.get('text', '')
        # Normalize hashtags: ensure each tag starts with #
        raw_hashtags = summary_data.get('hashtags', [])
        msg.hashtags = [tag if tag.startswith('#') else f'#{tag}' for tag in raw_hashtags]
        msg.headline = summary_data.get('headline', '')
        return msg
    except json.JSONDecodeError as e:
        log.warning(f'JSON parse error for message {msg.id}: {e}')
        return None
    except Exception:
        log.exception(f'Summarization failed for message {msg.id}')
        return None


# ============== STEP 4: CHECK FOR DUPLICATES ==============

async def check_duplicate_tfidf_gpt(
    msg: ReadMessage,
    existing_sent: List[SentMessage],
    read_msgs_by_id: dict,
    similarity_threshold: float = 0.3,
) -> str:
    """
    Step 4: Проверяет дубликат через TF-IDF + ChatGPT.

    Алгоритм:
        1. Строит список (sent_id, headline) для существующих сообщений
        2. Находит похожие заголовки через TF-IDF (порог similarity_threshold)
        3. Для каждой похожей пары вызывает ChatGPT для верификации
        4. Возвращает sent_id первого подтверждённого дубликата

    Args:
        msg: проверяемое сообщение
        existing_sent: список существующих SentMessage
        read_msgs_by_id: словарь {id: ReadMessage}
        similarity_threshold: порог схожести TF-IDF (0-1)

    Returns:
        str: sent_id дубликата или пустая строка
    """
    if not existing_sent or not msg.headline:
        return ''

    # Build list of (sent_id, headline) for existing messages
    existing_headlines = []
    for sent in existing_sent:
        if sent.read_message_id:
            read_msg = read_msgs_by_id.get(sent.read_message_id)
            if read_msg and read_msg.headline:
                existing_headlines.append((sent.id, read_msg.headline))

    if not existing_headlines:
        return ''

    # Find similar headlines using TF-IDF
    similar_pairs = find_similar_pairs(msg.headline, existing_headlines, threshold=similarity_threshold)

    if not similar_pairs:
        return ''

    log.debug(f'  Found {len(similar_pairs)} similar headlines for message {msg.id}')

    # Check each similar pair with ChatGPT (most similar first)
    for sent_id, similarity in similar_pairs:
        # Get the read message for comparison
        sent_msg = next((s for s in existing_sent if s.id == sent_id), None)
        if not sent_msg or not sent_msg.read_message_id:
            continue

        existing_read = read_msgs_by_id.get(sent_msg.read_message_id)
        if not existing_read:
            continue

        # Call ChatGPT to verify if they are duplicates
        is_dup = await check_pair_duplicate(
            headline1=msg.headline,
            text1=msg.summary or msg.text or '',
            headline2=existing_read.headline or '',
            text2=existing_read.summary or existing_read.text or '',
        )

        if is_dup:
            log.info(f'  Message {msg.id} is duplicate of {sent_id} (similarity={similarity:.2f})')
            return sent_id

    return ''


async def check_duplicate(msg: ReadMessage, existing_messages: List) -> str:
    """
    Альтернативная проверка дубликата через AI (старый метод).

    Args:
        msg: проверяемое сообщение
        existing_messages: список существующих сообщений для сравнения

    Returns:
        str: id дубликата или пустая строка
    """
    if not existing_messages:
        return ''

    ded_res = await deduplicate_messages_by_ai(msg, existing_messages)
    try:
        parsed = json.loads(ded_res)
        msg_id = parsed.get('id', '')
        if isinstance(msg_id, (list, tuple)):
            msg_id = msg_id[0] if msg_id else ''
        if msg_id is None:
            msg_id = ''
        msg_id = str(msg_id) if msg_id != '' else ''
    except Exception:
        msg_id = ''

    return msg_id


# ============== MAIN PROCESS ==============

async def process_messages(from_datetime, to_datetime):
    """
    Главный пайплайн обработки сообщений.

    Шаги:
        1. Read: Читает сообщения из каналов → state='read'
        2. Clean: Очищает текст, извлекает URL → state='clean'
        3. Summarize: Суммаризирует через GPT → state='summarized' или 'error'
        4. Deduplicate: Проверяет дубликаты через TF-IDF + GPT → создаёт SentMessage
        5. Generate text: Генерирует текст для отправки → state='to_send'
        6. Send: Отправляет/обновляет в Telegram → state='sent' или 'error'
        7. Read emodji: Читает реакции с учётом подписчиков
        8. Daily digest: Отправляет дайджест при смене даты

    Args:
        from_datetime: начало диапазона
        to_datetime: конец диапазона

    Returns:
        dict: {'read': int, 'sent': int, 'to_send': int} — статистика воронки
    """
    log.info('=' * 60)
    log.info(f'Starting process_messages: {from_datetime} -> {to_datetime}')
    log.info('=' * 60)

    # Счётчики воронки
    funnel = {'read': 0, 'sent': 0, 'to_send': 0}

    client = TelegramClient('session', config.api_id, config.api_hash)
    try:
        if not client.is_connected():
            await client.start()
            log.info('Telegram client connected')

        # Step 1: Read and save raw messages
        _report_step('Step 1: Reading')
        funnel['read'] = await fetch_and_save_raw_messages(client, from_datetime, to_datetime)

        # Step 2: Clean messages
        _report_step('Step 2: Cleaning')
        await parse_and_update_messages()

        # Step 3: Summarize messages
        _report_step('Step 3: Summarizing')
        to_summarize = await asyncio.to_thread(get_messages_by_state, 'clean', None, 1000, 50)
        log.info(f'Step 3: Summarizing {len(to_summarize)} messages')

        summarized_messages = []
        error_updates = []
        for i, msg in enumerate(to_summarize):
            summarized_msg = await summarize_read_message(msg)
            if summarized_msg is None:
                error_updates.append((msg.id, 'Summarization returned None'))
            else:
                msg.summary = summarized_msg.summary
                msg.hashtags = summarized_msg.hashtags
                msg.headline = summarized_msg.headline
                summarized_messages.append(msg)

            # Progress log every 10 messages
            if (i + 1) % 10 == 0:
                log.info(f'  Step 3 progress: {i + 1}/{len(to_summarize)} messages processed')

        # Batch update all at once
        if summarized_messages:
            await asyncio.to_thread(batch_update_read_messages_parsed, summarized_messages, set_state='summarized')
        if error_updates:
            await asyncio.to_thread(batch_update_read_messages_error, error_updates)

        log.info(f'Step 3 complete: {len(summarized_messages)} summarized, {len(error_updates)} errors')

        # Step 4: Deduplicate and prepare for sending
        _report_step('Step 4: Deduplicating')
        messages_to_process = await asyncio.to_thread(get_summarized_unlinked_messages)
        log.info(f'Step 4: Deduplicating {len(messages_to_process)} messages')

        new_sent_messages = []
        dup_links = []  # (read_id, sent_id)
        new_links = []  # (read_id, sent_id) - will be filled after insert
        dup_sent_state_updates = []  # (sent_id, 'to_update')

        if messages_to_process:
            # Get sent messages for deduplication by message_dttm (any state)
            dedup_from_date = (to_datetime - timedelta(days=config.dedup_window_days)).isoformat()
            existing_sent = await asyncio.to_thread(get_sent_messages_for_dedup, dedup_from_date)
            log.info(f'  Loaded {len(existing_sent)} existing sent messages for dedup check (by message_dttm)')

            # Batch load read messages for existing sent messages (needed for headline/text comparison)
            existing_read_ids = [s.read_message_id for s in existing_sent if s.read_message_id]
            existing_read_msgs_by_id = await asyncio.to_thread(batch_get_read_messages_by_ids, existing_read_ids)
            log.info(f'  Loaded {len(existing_read_msgs_by_id)} read messages for dedup comparison')

            new_msg_to_sent = []  # list of (msg, SentMessage) for new messages

            for i, msg in enumerate(messages_to_process):
                dup_sent_id = ''

                if len(msg.hashtags or []) > 0:
                    # Use TF-IDF + ChatGPT for deduplication
                    dup_sent_id = await check_duplicate_tfidf_gpt(
                        msg, existing_sent, existing_read_msgs_by_id, similarity_threshold=0.01
                    )

                if dup_sent_id:
                    dup_links.append((msg.id, dup_sent_id))
                    dup_sent_state_updates.append((dup_sent_id, 'to_update'))
                else:
                    # Create new sent message with message_dttm from the read message
                    new_sent = SentMessage(read_message_id=msg.id, message_dttm=msg.msg_dttm, state='new')
                    new_sent_messages.append(new_sent)
                    new_msg_to_sent.append((msg, new_sent))

                    # Add to existing lists for dedup check of subsequent messages in this batch
                    existing_sent.append(new_sent)
                    existing_read_msgs_by_id[msg.id] = msg

                # Progress log every 10 messages
                if (i + 1) % 10 == 0:
                    log.info(f'  Step 4 progress: {i + 1}/{len(messages_to_process)} messages processed')

            # Batch insert new sent messages
            if new_sent_messages:
                inserted_ids = await asyncio.to_thread(batch_insert_sent_messages, new_sent_messages)
                # Map inserted IDs back to read messages
                for i, (msg, sent) in enumerate(new_msg_to_sent):
                    sent.id = inserted_ids[i]
                    new_links.append((msg.id, inserted_ids[i]))

            # Batch update links and states
            if dup_links:
                await asyncio.to_thread(batch_link_read_messages_to_sent, dup_links, set_state='deduplicated')
            if new_links:
                await asyncio.to_thread(batch_link_read_messages_to_sent, new_links, set_state='deduplicated')
            if dup_sent_state_updates:
                await asyncio.to_thread(batch_update_sent_messages_state, dup_sent_state_updates)

        funnel['sent'] = len(new_links) + len(dup_links)
        log.info(f'Step 4 complete: {len(new_links)} new, {len(dup_links)} duplicates')

        # Step 5: Generate text for sent messages
        _report_step('Step 5: Generating')
        to_generate = await asyncio.to_thread(get_sent_messages_by_states, ['new', 'to_update'])
        log.info(f'Step 5: Generating text for {len(to_generate)} messages')

        # Batch load all read messages by their IDs
        read_msg_ids = [sent.read_message_id for sent in to_generate if sent.read_message_id]
        sent_ids = [sent.id for sent in to_generate]
        read_msgs_by_id = await asyncio.to_thread(batch_get_read_messages_by_ids, read_msg_ids)
        linked_msgs_by_sent_id = await asyncio.to_thread(batch_get_read_messages_by_sent_ids, sent_ids)

        text_updates = []  # (sent_id, text)
        state_updates = []  # (sent_id, state)

        for sent in to_generate:
            if sent.read_message_id:
                read_msg = read_msgs_by_id.get(sent.read_message_id)
                if read_msg:
                    # Get all read messages linked to this sent message
                    linked_messages = linked_msgs_by_sent_id.get(sent.id, [])
                    formatted_text = await format_telegram_summary(read_msg, linked_messages)
                    text_updates.append((sent.id, formatted_text))

                    # Check if message has required hashtags
                    # Normalize both sides: remove leading # for comparison
                    def normalize_tag(t):
                        return t.lstrip('#').lower()
                    msg_tags_normalized = [normalize_tag(h) for h in (read_msg.hashtags or [])]
                    has_required_hashtag = any(
                        normalize_tag(tag) in msg_tags_normalized
                        for tag in config.required_hashtags
                    )

                    if has_required_hashtag:
                        state_updates.append((sent.id, 'to_send'))
                    else:
                        log.info(f'  Message {sent.id} missing required hashtags {config.required_hashtags}, marking as no_send')
                        state_updates.append((sent.id, 'no_send'))
                else:
                    # If read message not found, mark as no_send
                    log.warning(f'  Read message not found for sent message {sent.id}')
                    state_updates.append((sent.id, 'no_send'))

        # Batch update text and state
        if text_updates:
            await asyncio.to_thread(batch_update_sent_messages_text, text_updates)
        if state_updates:
            await asyncio.to_thread(batch_update_sent_messages_state, state_updates)

        log.info(f'Step 5 complete: {len(to_generate)} messages processed')

        # Step 5.5: Pre-send scoring — filter out predicted dislikes
        _report_step('Step 5.5: Pre-scoring')
        scorer = _get_scorer()

        # Cold start: train before scoring if no model exists
        if scorer.sample_count == 0:
            log.info('Step 5.5: Cold start — training scorer before scoring')
            trained = await _train_scorer(client, scorer)
            log.info(f'Step 5.5: Cold start training complete: {trained} examples (total={scorer.sample_count})')

        prescore_msgs = await asyncio.to_thread(get_sent_messages, state='to_send', order_asc=True)
        log.info(f'Step 5.5: Pre-scoring {len(prescore_msgs)} messages (scorer samples={scorer.sample_count})')

        if prescore_msgs:
            prescore_read_ids = [s.read_message_id for s in prescore_msgs if s.read_message_id]
            prescore_read_msgs = await asyncio.to_thread(batch_get_read_messages_by_ids, prescore_read_ids)

            prescore_updates = []  # (sent_id, prediction_score, bot_reaction=None)
            low_score_updates = []  # (sent_id, 'low_score')
            kept_count = 0

            for sent in prescore_msgs:
                read_msg = prescore_read_msgs.get(sent.read_message_id) if sent.read_message_id else None
                if not read_msg:
                    continue

                score = await asyncio.to_thread(
                    scorer.predict,
                    read_msg.headline or '',
                    read_msg.summary or '',
                    read_msg.hashtags,
                    len(read_msg.text or ''),
                    read_msg.msg_dttm,
                )
                prescore_updates.append((sent.id, score, None))

                if score <= config.scorer_neg_threshold:
                    if random.random() < config.low_score_send_probability:
                        kept_count += 1
                        log.info(f'  Message {sent.id} low score={score:.3f} but kept (random pass)')
                    else:
                        low_score_updates.append((sent.id, 'low_score'))
                        log.info(f'  Message {sent.id} low score={score:.3f} -> low_score')

            if prescore_updates:
                await asyncio.to_thread(batch_update_sent_messages_prediction, prescore_updates)
            if low_score_updates:
                await asyncio.to_thread(batch_update_sent_messages_state, low_score_updates)

            filtered = len(low_score_updates)
            log.info(
                f'Step 5.5 complete: scored {len(prescore_updates)}, '
                f'filtered {filtered}, kept despite low score {kept_count}'
            )
        else:
            log.info('Step 5.5 complete: no messages to pre-score')

        # Step 6: Send/update messages (sorted by date, oldest first)
        _report_step('Step 6: Sending')
        to_send = await asyncio.to_thread(get_sent_messages, state='to_send', order_asc=True)
        funnel['to_send'] = len(to_send)
        log.info(f'Step 6: Sending/updating {len(to_send)} messages')

        if to_send and not config.is_mock:
            # Send in batches of 10 with progress logging
            batch_size = 10
            for i in range(0, len(to_send), batch_size):
                batch = to_send[i:i + batch_size]
                await send_or_update_sent_messages_concurrent(
                    batch,
                    config.output_channel_id,
                    concurrency=config.send_concurrency,
                )
                log.info(f'  Step 6 progress: {min(i + batch_size, len(to_send))}/{len(to_send)} messages sent')
            log.info(f'Step 6 complete: sent {len(to_send)} messages')
        elif to_send and config.is_mock:
            for i, sent in enumerate(to_send):
                log.info(f'  MOCK: Would send message {sent.id}')
                if (i + 1) % 10 == 0:
                    log.info(f'  Step 6 progress: {i + 1}/{len(to_send)} messages (mock)')
            log.info(f'Step 6 complete: {len(to_send)} messages marked as sent (mock mode)')
        else:
            log.info('Step 6 complete: no messages to send')

        # Step 6.5: Set bot reactions (prediction_score already set in Step 5.5)
        _report_step('Step 6.5: Reactions')
        emodji_window = datetime.now() - timedelta(days=config.emodji_window_days)
        recently_sent = await asyncio.to_thread(
            get_sent_messages, state='sent', from_date=emodji_window.isoformat(),
        )
        # Filter to sent messages that have prediction_score but no bot_reaction yet
        to_react = [s for s in recently_sent if s.prediction_score is not None and s.bot_reaction is None and s.telegram_id]
        log.info(f'Step 6.5: Setting bot reactions for {len(to_react)} messages')

        if to_react:
            reaction_updates = []  # (sent_id, prediction_score, bot_reaction)
            for sent in to_react:
                reaction_emoji = choose_bot_reaction(sent.prediction_score)
                if reaction_emoji and not config.is_mock:
                    ok = await set_message_reaction(
                        config.output_channel_id, sent.telegram_id, reaction_emoji,
                    )
                    if not ok:
                        reaction_emoji = None
                elif reaction_emoji and config.is_mock:
                    log.info(f'  MOCK: Would set reaction {reaction_emoji} on message {sent.telegram_id} (score={sent.prediction_score:.3f})')

                if reaction_emoji:
                    reaction_updates.append((sent.id, sent.prediction_score, reaction_emoji))

            if reaction_updates:
                await asyncio.to_thread(batch_update_sent_messages_prediction, reaction_updates)
            log.info(f'Step 6.5 complete: set reactions for {len(reaction_updates)} messages')
        else:
            log.info('Step 6.5 complete: no messages need bot reactions')

        # Step 7: Read emodji counts and calculate normalized score
        _report_step('Step 7: Reactions')
        # Formula: normalized_score = 10 * (sent_emodji / output_subscribers) + sum(read_emodji / read_channel_subscribers)
        # Each term is normalized by its channel's subscriber count, then multiplied by 100
        sent_for_emodji = await asyncio.to_thread(
            get_sent_messages,
            from_date=emodji_window.isoformat(),
            state='sent',
        )
        log.info(f'Step 7: Reading emodji for {len(sent_for_emodji)} messages')

        if sent_for_emodji:
            # Batch load all linked read messages
            sent_ids = [sent.id for sent in sent_for_emodji]
            linked_msgs_by_sent_id = await asyncio.to_thread(batch_get_read_messages_by_sent_ids, sent_ids)

            # Collect unique source channel IDs from linked read messages + output channel
            source_channel_ids = set()
            source_channel_ids.add(config.output_channel_id)  # Add output channel for sent messages
            for sent_id, read_msgs in linked_msgs_by_sent_id.items():
                for read_msg in read_msgs:
                    if read_msg.channel_id:
                        source_channel_ids.add(read_msg.channel_id)

            # Fetch subscribers count using cache
            subs_cache = get_subscribers_cache()
            need_full_refresh = subs_cache.should_refresh(to_datetime)

            if need_full_refresh:
                # Full refresh: fetch all channels
                log.info('  Refreshing all channel subscribers...')
                for channel_id in source_channel_ids:
                    try:
                        subs = await get_channel_subscribers_count(client, channel_id)
                        subs_cache.set(channel_id, subs)
                        log.info(f'  Channel {channel_id}: {subs} subscribers')
                    except Exception as e:
                        log.warning(f'  Could not get subscribers for channel {channel_id}: {e}')
                        subs_cache.set(channel_id, 0)
                subs_cache.set_date_range(from_datetime, to_datetime)
            else:
                # Incremental: only fetch new channels not in cache
                missing_channels = subs_cache.get_missing_channels(source_channel_ids)
                if missing_channels:
                    log.info(f'  Fetching subscribers for {len(missing_channels)} new channels...')
                    for channel_id in missing_channels:
                        try:
                            subs = await get_channel_subscribers_count(client, channel_id)
                            subs_cache.set(channel_id, subs)
                            log.info(f'  Channel {channel_id}: {subs} subscribers')
                        except Exception as e:
                            log.warning(f'  Could not get subscribers for channel {channel_id}: {e}')
                            subs_cache.set(channel_id, 0)
                else:
                    log.info('  Using cached channel subscribers')

            channel_subscribers = subs_cache.get_all()
            output_subscribers = channel_subscribers.get(str(config.output_channel_id), 0)

            emodji_updates = []  # (sent_id, emodji_count, normalized_score)
            for sent in sent_for_emodji:
                total_emodji = 0
                normalized_score = 0.0

                # Read emodji for sent message
                if sent.telegram_id:
                    # Plain count for emodji_count
                    sent_emodji_plain = await read_message_reactions_telethon(
                        client,
                        config.output_channel_id,
                        sent.telegram_id,
                    )
                    total_emodji += sent_emodji_plain
                    # Weighted sum for normalized_score
                    sent_emodji_weighted = await read_message_reactions_weighted(
                        client,
                        config.output_channel_id,
                        sent.telegram_id,
                    )
                    if output_subscribers > 0:
                        normalized_score += sent_emodji_weighted / output_subscribers

                # Read emodji for linked read messages: sum(read_emodji / read_channel_subscribers)
                linked_messages = linked_msgs_by_sent_id.get(sent.id, [])
                for read_msg in linked_messages:
                    if read_msg.telegram_id and read_msg.channel_id:
                        read_emodji = await read_message_reactions_telethon(
                            client,
                            read_msg.channel_id,
                            read_msg.telegram_id,
                        )
                        total_emodji += read_emodji
                        read_subs = channel_subscribers.get(str(read_msg.channel_id), 0)
                        if read_subs > 0:
                            normalized_score += read_emodji / read_subs

                # Multiply by 100 for readability
                normalized_score = round(normalized_score * 100, 2)

                # Only update if something changed
                if total_emodji != sent.emodji_count or normalized_score != sent.normalized_score:
                    emodji_updates.append((sent.id, total_emodji, normalized_score))

            # Batch update emodji counts and normalized scores
            if emodji_updates:
                await asyncio.to_thread(batch_update_sent_messages_emodji, emodji_updates)

            log.info(f'Step 7 complete: updated emodji for {len(emodji_updates)} messages')

            # Step 7.5: Train scorer on all sent messages
            _report_step('Step 7.5: Training')
            scorer = _get_scorer()
            if need_full_refresh or scorer.sample_count == 0:
                log.info('Step 7.5: Training scorer on all sent messages')
                trained = await _train_scorer(client, scorer)
                if trained:
                    log.info(f'Step 7.5 complete: trained on {trained} examples (total={scorer.sample_count})')
                else:
                    log.info('Step 7.5 complete: no training data available')
        else:
            log.info('Step 7 complete: no messages to check')

        # Step 8: Send daily digest if date changed
        _report_step('Step 8: Digest')
        subs_cache = get_subscribers_cache()
        digest_range = subs_cache.should_send_digest(from_datetime, to_datetime)
        if digest_range and not config.is_mock:
            digest_from, digest_to = digest_range
            log.info(f'Step 8: Sending daily digest for {digest_from.date()}')
            success = await send_daily_digest(digest_from, digest_to)
            if success:
                subs_cache.mark_digest_sent(digest_from.date())
            else:
                log.warning('Failed to send daily digest')
        elif digest_range and config.is_mock:
            digest_from, digest_to = digest_range
            log.info(f'Step 8: MOCK - Would send daily digest for {digest_from.date()}')
            subs_cache.mark_digest_sent(digest_from.date())
        else:
            log.info('Step 8: No digest needed (12:00 not in range or already sent)')

        log.info('=' * 60)
        log.info('process_messages complete')
        log.info(f'Funnel: {funnel["read"]} / {funnel["sent"]} / {funnel["to_send"]}')
        log.info('=' * 60)

        return funnel

    finally:
        await client.disconnect()
        log.info('Telegram client disconnected')


def process_messages_sync(from_datetime, to_datetime):
    """
    Синхронная обёртка для process_messages().

    Используется из CLI или синхронного контекста.
    """
    asyncio.run(process_messages(from_datetime, to_datetime))


# ============== SEPARATE PROCESSES ==============

async def run_fetch_only(from_datetime, to_datetime):
    """Запускает только шаг 1 (чтение сообщений из Telegram)."""
    client = TelegramClient('session', config.api_id, config.api_hash)
    try:
        if not client.is_connected():
            await client.start()
        await fetch_and_save_raw_messages(client, from_datetime, to_datetime)
    finally:
        await client.disconnect()


async def run_parse_only(limit=1000):
    """Запускает только шаг 2 (парсинг сообщений)."""
    return await parse_and_update_messages(limit=limit)
