import asyncio
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from jinja2 import Template

from utils.models import ReadMessage
from typing import List, Optional


async def get_page_metadata_from_url(url, timeout=5):
    """
    Получает метаданные страницы по URL.

    Ищет:
        - og:description, meta description, twitter:description
        - og:title, <title>

    Args:
        url: URL страницы
        timeout: таймаут запроса в секундах

    Returns:
        dict: {'description': str|None, 'title': str|None}
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()

        # Parse HTML in thread pool to avoid blocking
        def parse_html(content):
            soup = BeautifulSoup(content, 'html.parser')
            result = {'description': None, 'title': None}

            # Try to get description from og:description or meta description
            desc_tags = ['og:description', 'description', 'twitter:description']
            for desc_name in desc_tags:
                meta = soup.find('meta', property=desc_name) or soup.find('meta', attrs={'name': desc_name})
                if meta and meta.get('content'):
                    result['description'] = meta.get('content').strip()
                    break

            # Try to get title from og:title, then <title> tag
            title_meta = soup.find('meta', property='og:title') or soup.find('meta', attrs={'name': 'title'})
            if title_meta and title_meta.get('content'):
                result['title'] = title_meta.get('content').strip()
            else:
                title_tag = soup.find('title')
                if title_tag and title_tag.string:
                    result['title'] = title_tag.string.strip()

            return result

        return await asyncio.to_thread(parse_html, response.content)
    except Exception:
        return {'description': None, 'title': None}


async def get_url_display_name(url, max_description_length=100):
    """
    Возвращает отображаемое имя для URL.

    Приоритет:
        1. Description (если ≤ max_description_length)
        2. Title
        3. Domain

    Args:
        url: URL страницы
        max_description_length: максимальная длина description

    Returns:
        str: отображаемое имя
    """
    metadata = await get_page_metadata_from_url(url)

    # Prefer description if it's not too long
    description = metadata.get('description')
    if description and len(description) <= max_description_length:
        return description

    # Fall back to title
    title = metadata.get('title')
    if title:
        return title

    # Fallback to domain
    domain = re.sub(r'^https?://(www\.)?', '', url).split('/')[0]
    return domain


async def format_telegram_summary(msg: ReadMessage, linked_messages: List[ReadMessage] = None):
    """
    Форматирует ReadMessage в HTML для Telegram.

    Использует шаблон static/telegram_summary_template.html.

    Args:
        msg: основное сообщение с summary, headline, hashtags
        linked_messages: все связанные read_messages (для списка источников)

    Returns:
        str: HTML строка для отправки в Telegram
    """
    summary = msg.summary or ''
    summary = re.sub(r'</?body/?>', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'</?br/?>', '\n', summary, flags=re.IGNORECASE)

    hashtags = msg.hashtags or []
    for tag in hashtags:
        summary = re.sub(rf'\s*{re.escape(tag)}\b', '', summary)

    urls = msg.urls or []
    urls_block = ''
    if urls:
        if len(urls) == 1:
            urls_block = f'🌎  <a href="{urls[0]}">Источник</a>\n'
        elif len(urls) > 1:
            urls_block = '🌎 Источники:\n'
            for u in urls:
                display_name = await get_url_display_name(u)
                if u:
                    urls_block += f'• <a href="{u}"> {display_name} </a>\n'

    hashtags_block = ''
    if hashtags:
        hashtags_block = ' '.join(f"{tag if tag.startswith('#') else '#'+tag}" for tag in hashtags)

    headline = msg.headline or ''
    public_link = msg.public_link or ''

    summary_date = msg.msg_dttm.strftime('%Y-%m-%d') if isinstance(msg.msg_dttm, datetime) else ''

    # Build sources list from linked messages (or just the primary message)
    all_messages = linked_messages if linked_messages else [msg]
    sources = []
    for m in all_messages:
        author = m.author or ''
        link = m.public_link or ''
        if author:
            sources.append({'author': author, 'link': link})

    # Read template file in thread pool to avoid blocking
    def read_template():
        with open('static/telegram_summary_template.html', encoding='utf-8') as f:
            return Template(f.read())

    template = await asyncio.to_thread(read_template)

    text = template.render(
        public_link=public_link,
        headline=headline,
        summary_date=summary_date,
        summary=summary,
        urls_block=urls_block,
        sources=sources,
        hashtags_block=hashtags_block,
    )
    text = re.sub(r'\n\n\n', '\n', text).strip()
    return text

