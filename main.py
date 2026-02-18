import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from jinja2 import Environment, FileSystemLoader
from telethon import TelegramClient

import config
from utils.sqlite.messages import (
    get_read_messages,
    get_sent_messages,
    get_sent_message_by_telegram_id,
    get_max_read_message_date,
    insert_sent_message,
    update_sent_message_air,
)
from utils.models import SentMessage


app = FastAPI()

# Jinja2 template environment
jinja_env = Environment(loader=FileSystemLoader('static'), autoescape=True)

# Глобальные переменные для управления кроном
cron_task = None
cron_status = {
    'running': False,
    'last_run': None,
    'started_at': None,
    'interval': None,
    'funnel': '- / - / -',
    'current_step': None,
}
cron_running_flag = False
cron_interval = 10


async def cron_worker():
    """
    Фоновый воркер для периодической обработки сообщений.

    - Запускается при вызове /start_cron
    - Выполняет process_messages() с интервалом cron_interval минут
    - Автоматически определяет временной диапазон: от последнего запуска до текущего момента
    - При первом запуске берёт дату из get_max_read_message_date() или default_start_date
    """
    global cron_running_flag, cron_status, cron_interval
    prev_run = cron_status.get('last_run')
    if isinstance(prev_run, str):
        prev_run = datetime.fromisoformat(prev_run)
    if not prev_run:
        prev_run = await asyncio.to_thread(get_max_read_message_date)
        if isinstance(prev_run, str):
            prev_run = datetime.fromisoformat(prev_run)
        if prev_run:
            logging.info(f'Using max msg_dttm from read_messages: {prev_run}')
    if not prev_run:
        prev_run = datetime.fromisoformat(config.default_start_date)
        logging.info(f'No messages in database, using default_start_date: {prev_run}')
    while cron_running_flag:
        cron_status['running'] = True
        now = datetime.now()
        next_run = now if prev_run + timedelta(hours=48) > now else prev_run + timedelta(hours=48)
        funnel = {'read': 0, 'sent': 0, 'to_send': 0}
        try:
            # Import and call async process_messages directly - avoid nested event loops
            from utils.processor import process_messages, set_step_callback
            set_step_callback(lambda step: cron_status.update({'current_step': step}))
            funnel = await process_messages(prev_run, next_run) or funnel
        except Exception:
            logging.exception('Cron job failed')
        cron_status['current_step'] = None
        prev_run = next_run if isinstance(next_run, datetime) else datetime.fromisoformat(next_run)
        cron_status['last_run'] = prev_run.isoformat()
        cron_status['funnel'] = f'{funnel["read"]} / {funnel["sent"]} / {funnel["to_send"]}'
        for _ in range(int(cron_interval * 60)):
            if not cron_running_flag:
                break
            await asyncio.sleep(1)
    cron_status['running'] = False


@app.post('/start_cron')
async def start_cron(request: Request):
    """
    Запускает фоновый cron worker.

    Body (JSON):
        interval (float, optional): интервал в минутах между запусками

    Returns:
        {"status": "started", "interval": <число>}

    Raises:
        HTTPException 400: если воркер уже запущен
    """
    global cron_task, cron_running_flag, cron_interval, cron_status
    if cron_task and not cron_task.done():
        raise HTTPException(status_code=400, detail='already running')
    data = await request.json() if request.headers.get('content-type') == 'application/json' else {}
    cron_interval = float(data.get('interval', cron_interval)) if data else cron_interval
    cron_running_flag = True
    cron_status['started_at'] = datetime.now().isoformat()
    cron_status['interval'] = cron_interval
    cron_task = asyncio.create_task(cron_worker())
    return JSONResponse(content={'status': 'started', 'interval': cron_interval})


@app.post('/stop_cron')
async def stop_cron():
    """
    Останавливает cron worker.

    Returns:
        {"status": "stopped"}

    Raises:
        HTTPException 400: если воркер не запущен
    """
    global cron_task, cron_running_flag, cron_status
    if not cron_task or cron_task.done():
        raise HTTPException(status_code=400, detail='not running')
    cron_running_flag = False
    await cron_task
    cron_status['started_at'] = None
    return JSONResponse(content={'status': 'stopped'})


@app.get('/cron_status')
async def cron_status_view():
    """
    Возвращает статус cron worker.

    Returns:
        {
            "running": true|false,
            "last_run": "2025-01-26T12:00:00",
            "started_at": "2025-01-26T10:00:00",
            "interval": 10,
            "funnel": "10 / 5 / 3"  # read / sent / to_send
        }
    """
    return JSONResponse(content=cron_status)


@app.get('/read_messages')
async def read_messages_endpoint(
    from_date: Optional[str] = Query(None, description='ISO8601 date'),
    to_date: Optional[str] = Query(None, description='ISO8601 date'),
    status: Optional[str] = Query(None, description='Message state filter'),
):
    """
    Get read messages from database.
    Example: /read_messages?from_date=2025-09-18T00:00:00&status=read
    """
    result = await asyncio.to_thread(get_read_messages, from_date=from_date, to_date=to_date, state=status)
    return JSONResponse(content=[asdict(msg) for msg in result])


@app.get('/sent_messages')
async def sent_messages_endpoint(
    from_date: Optional[str] = Query(None, description='ISO8601 date'),
    to_date: Optional[str] = Query(None, description='ISO8601 date'),
    status: Optional[str] = Query(None, description='Message state filter'),
):
    """
    Get sent messages from database.
    Example: /sent_messages?from_date=2025-09-18T00:00:00&status=sent
    """
    result = await asyncio.to_thread(get_sent_messages, from_date=from_date, to_date=to_date, state=status)
    return JSONResponse(content=[asdict(msg) for msg in result])


@app.get('/api/messages')
async def api_messages_endpoint(
    from_date: Optional[str] = Query(None, description='ISO8601 date'),
    to_date: Optional[str] = Query(None, description='ISO8601 date'),
    status: Optional[str] = Query(None, description='Message state filter'),
    exclude_tags: Optional[str] = Query(None, description='Comma-separated hashtags to exclude'),
    sort_by: Optional[str] = Query('score', description='Sort by: score, date, emodji'),
    sort_order: Optional[str] = Query('desc', description='Sort order: asc, desc'),
    hide_discussed: bool = Query(False, description='Hide messages that were discussed on air'),
    discussed_only: bool = Query(False, description='Show only discussed messages'),
    bot_reaction: Optional[str] = Query(None, description='Bot reaction filter: liked, disliked, none'),
):
    """
    API эндпоинт для получения сообщений с расширенной фильтрацией и сортировкой.

    Query параметры:
        from_date, to_date: диапазон дат (ISO8601)
        status: фильтр по состоянию (to_send, sent, error, renew)
        exclude_tags: теги для исключения через запятую
        sort_by: поле сортировки (score, date, emodji)
        sort_order: направление (asc, desc)
        hide_discussed: скрыть обсуждённые в эфире
        discussed_only: показать только обсуждённые

    Returns:
        {
            "messages": [...],
            "stats": {"total_count": 100, "total_emodji": 500, "avg_normalized": 2.5},
            "all_tags": ["news", "tech"]
        }
    """
    from utils.sqlite.messages import batch_get_read_messages_by_ids

    result = await asyncio.to_thread(
        get_sent_messages, from_date=from_date, to_date=to_date, state=status,
        limit=500, hide_discussed=hide_discussed, discussed_only=discussed_only,
        bot_reaction_filter=bot_reaction,
    )

    # Load read messages to get hashtags
    read_msg_ids = [msg.read_message_id for msg in result if msg.read_message_id]
    read_msgs_by_id = await asyncio.to_thread(batch_get_read_messages_by_ids, read_msg_ids)

    # Collect all unique tags and headlines
    all_tags = set()
    msg_tags_map = {}
    msg_headline_map = {}
    for msg in result:
        if msg.read_message_id:
            read_msg = read_msgs_by_id.get(msg.read_message_id)
            if read_msg:
                if read_msg.hashtags:
                    tags = [tag.lower().lstrip('#') for tag in read_msg.hashtags]
                    msg_tags_map[msg.id] = tags
                    all_tags.update(tags)
                if read_msg.headline:
                    msg_headline_map[msg.id] = read_msg.headline

    # Filter by excluded tags if provided
    if exclude_tags:
        exclude_set = {tag.strip().lower().lstrip('#') for tag in exclude_tags.split(',')}
        filtered_result = []
        for msg in result:
            msg_tags = set(msg_tags_map.get(msg.id, []))
            if not (msg_tags & exclude_set):
                filtered_result.append(msg)
        result = filtered_result

    # Sort
    reverse = sort_order != 'asc'
    if sort_by == 'date':
        result = sorted(result, key=lambda x: x.sent_at or datetime.min, reverse=reverse)
    elif sort_by == 'emodji':
        result = sorted(result, key=lambda x: x.emodji_count or 0, reverse=reverse)
    else:  # score
        result = sorted(result, key=lambda x: x.normalized_score or 0, reverse=reverse)

    result = result[:1000]

    # Calculate stats
    total_count = len(result)
    total_emodji = sum(msg.emodji_count or 0 for msg in result)
    total_normalized = sum(msg.normalized_score or 0 for msg in result)
    avg_normalized = round(total_normalized / total_count, 2) if total_count > 0 else 0

    # Extract channel ID for links
    channel_link = str(config.output_channel_id).lstrip('@')
    if channel_link.lstrip('-').isdigit():
        channel_id_int = int(channel_link)
        if channel_id_int < 0:
            channel_link = str(abs(channel_id_int))
            if int(channel_link) > 10**12:
                channel_link = str(int(channel_link) - 10**12)

    # Prepare messages for response
    messages_data = []
    for msg in result:
        sent_at_formatted = ''
        sent_at_iso = ''
        if msg.sent_at:
            if isinstance(msg.sent_at, str):
                try:
                    dt = datetime.fromisoformat(msg.sent_at)
                    sent_at_formatted = dt.strftime('%d.%m.%Y %H:%M')
                    sent_at_iso = dt.isoformat()
                except ValueError:
                    sent_at_formatted = msg.sent_at
                    sent_at_iso = msg.sent_at
            else:
                sent_at_formatted = msg.sent_at.strftime('%d.%m.%Y %H:%M')
                sent_at_iso = msg.sent_at.isoformat()

        # Format message_dttm (original message date)
        message_dttm_formatted = ''
        if msg.message_dttm:
            if isinstance(msg.message_dttm, str):
                try:
                    dt = datetime.fromisoformat(msg.message_dttm)
                    message_dttm_formatted = dt.strftime('%d.%m.%Y %H:%M')
                except ValueError:
                    message_dttm_formatted = msg.message_dttm
            else:
                message_dttm_formatted = msg.message_dttm.strftime('%d.%m.%Y %H:%M')

        # Format sent_air if exists
        sent_air_formatted = ''
        if msg.sent_air:
            if isinstance(msg.sent_air, str):
                try:
                    dt = datetime.fromisoformat(msg.sent_air)
                    sent_air_formatted = dt.strftime('%d.%m.%Y %H:%M')
                except ValueError:
                    sent_air_formatted = msg.sent_air
            else:
                sent_air_formatted = msg.sent_air.strftime('%d.%m.%Y %H:%M')

        messages_data.append({
            'id': msg.id,
            'telegram_id': msg.telegram_id,
            'text': msg.text or '',
            'emodji_count': msg.emodji_count or 0,
            'normalized_score': msg.normalized_score or 0,
            'state': msg.state,
            'sent_at_formatted': sent_at_formatted,
            'sent_at_iso': sent_at_iso,
            'message_dttm_formatted': message_dttm_formatted,
            'channel_link': channel_link,
            'tags': msg_tags_map.get(msg.id, []),
            'headline': msg_headline_map.get(msg.id, ''),
            'sent_air': sent_air_formatted,
            'prediction_score': round(msg.prediction_score, 3) if msg.prediction_score is not None else None,
            'bot_reaction': msg.bot_reaction,
        })

    return JSONResponse(content={
        'messages': messages_data,
        'stats': {
            'total_count': total_count,
            'total_emodji': total_emodji,
            'avg_normalized': avg_normalized,
        },
        'all_tags': sorted(list(all_tags)),
    })


@app.get('/', response_class=HTMLResponse)
async def root_page():
    """Main dashboard page."""
    template = jinja_env.get_template('dashboard.html')
    return HTMLResponse(content=template.render())


@app.get('/sent_messages_html', response_class=HTMLResponse)
async def sent_messages_html_redirect():
    """Redirect old URL to root."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url='/')


@app.post('/api/mark_discussed/{message_id}')
async def mark_message_discussed(message_id: str):
    """
    Отмечает сообщение как обсуждённое в эфире.

    Path параметры:
        message_id: UUID сообщения

    Returns:
        {"status": "ok", "message_id": "..."}
    """
    try:
        await asyncio.to_thread(update_sent_message_air, message_id)
        return JSONResponse(content={'status': 'ok', 'message_id': message_id})
    except Exception as e:
        logging.exception('Error marking message as discussed')
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/api/download_db')
async def download_database():
    """
    Скачивает файл SQLite базы данных.

    Returns:
        FileResponse с файлом БД (application/octet-stream)
    """
    from utils.sqlite.connection import get_db_path
    import os

    db_path = get_db_path()
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail='Database file not found')

    filename = f'telegram_helper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
    return FileResponse(
        path=db_path,
        filename=filename,
        media_type='application/octet-stream'
    )


@app.post('/renew_msg_data')
async def renew_msg_data(request: Request):
    """
    Синхронизирует сообщения из выходного канала в БД.

    Скачивает все сообщения из output_channel_id начиная с указанной даты
    и добавляет в БД те, которых ещё нет (по telegram_id).

    Body (JSON):
        from_date (required): ISO8601 дата начала

    Returns:
        {"added_message_ids": [...], "count": 5}
    """
    client = TelegramClient('renew_msg_data', config.api_id, config.api_hash)
    added_ids = []
    try:
        await client.start()
        data = await request.json()
        from_date_str = data.get('from_date')
        if not from_date_str:
            raise HTTPException(status_code=400, detail='from_date is required')

        from_date = datetime.fromisoformat(from_date_str)

        # Read messages from output channel using Telethon
        async for message in client.iter_messages(config.output_channel_id, reverse=True, offset_date=from_date):
            if not message.text:
                continue

            msg_time = message.date
            if msg_time.tzinfo is not None:
                msg_time = msg_time.replace(tzinfo=None)

            telegram_id = message.id

            # Check if message already exists in DB
            existing = await asyncio.to_thread(get_sent_message_by_telegram_id, telegram_id)
            if existing:
                continue

            # Create new sent_message with state='renew' (synced from channel)
            new_sent = SentMessage(
                telegram_id=telegram_id,
                text=message.text,
                state='renew',
                sent_at=msg_time,
            )
            await asyncio.to_thread(insert_sent_message, new_sent)
            added_ids.append(telegram_id)

        return JSONResponse(content={'added_message_ids': added_ids, 'count': len(added_ids)})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception('Error in renew_msg_data')
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.disconnect()


@app.on_event('startup')
async def startup_event():
    """Хук запуска приложения. Инициализирует схему БД."""
    from utils.sqlite.connection import ensure_schema_once
    await asyncio.to_thread(ensure_schema_once)


@app.on_event('shutdown')
async def shutdown_event():
    """Хук остановки приложения. OpenAI клиенты создаются per-request, очистка не требуется."""
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
