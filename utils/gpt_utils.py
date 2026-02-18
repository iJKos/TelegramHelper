import asyncio
import openai
import config


async def call_gpt(prompt, system_message, openai_api_key=config.openai_api_key, max_tokens=1024, json_mode=False):
    """
    Универсальный вызов OpenAI API.

    - Создаёт новый клиент для каждого запроса (избегает проблем с event loop)
    - Поддерживает JSON mode для структурированных ответов

    Args:
        prompt: текст промпта
        system_message: системное сообщение
        openai_api_key: API ключ OpenAI
        max_tokens: лимит токенов ответа
        json_mode: если True, включает response_format: json_object

    Returns:
        str: текст ответа GPT
    """
    # Create a new client for each call - avoids cross-event-loop issues
    # AsyncOpenAI is lightweight and handles connection pooling internally
    client = openai.AsyncOpenAI(api_key=openai_api_key)
    try:
        kwargs = {
            'model': 'gpt-4.1-mini',
            'messages': [{'role': 'system', 'content': system_message}, {'role': 'user', 'content': prompt}],
            'max_tokens': max_tokens,
        }
        if json_mode:
            kwargs['response_format'] = {'type': 'json_object'}
        response = await client.chat.completions.create(**kwargs)
        return response.choices[0].message.content.strip()
    finally:
        await client.close()


async def summarize_message_text(text, openai_api_key=config.openai_api_key):
    """
    Суммаризирует текст сообщения через GPT.

    Использует шаблон static/summarize_prompt.txt.

    Args:
        text: текст для суммаризации
        openai_api_key: API ключ OpenAI

    Returns:
        str: JSON строка с полями text, hashtags, headline
    """
    # Read file in thread pool to avoid blocking
    def read_prompt():
        with open('static/summarize_prompt.txt', 'r', encoding='utf-8') as f:
            return f.read()

    prompt_template = await asyncio.to_thread(read_prompt)
    prompt = prompt_template.format(text=text)
    return await call_gpt(
        prompt,
        'Ты помощник для выделения сути сообщений. Всегда отвечай валидным JSON.',
        openai_api_key=openai_api_key,
        max_tokens=256,
        json_mode=True,
    )


async def deduplicate_messages_by_ai(message, messages, openai_api_key=config.openai_api_key):
    """
    AI-дедупликация (старый метод): отправляет все сообщения в GPT одним запросом.

    Использует шаблон static/deduplicate_prompt.txt.

    Args:
        message: проверяемое сообщение
        messages: список существующих сообщений для сравнения
        openai_api_key: API ключ OpenAI

    Returns:
        str: JSON строка с полем id (ID дубликата или пусто)
    """
    messages_str = '\n'.join(str(msg.to_json_lite()) for msg in messages)

    # Read file in thread pool to avoid blocking
    def read_prompt():
        with open('static/deduplicate_prompt.txt', 'r', encoding='utf-8') as f:
            return f.read()

    prompt_template = await asyncio.to_thread(read_prompt)
    prompt = prompt_template.format(messages=messages_str, message=message.to_json_lite())
    return await call_gpt(
        prompt,
        'Ты помощник для выделения сути сообщений и удаления дубликатов по смыслу. Всегда отвечай валидным JSON.',
        openai_api_key=openai_api_key,
        max_tokens=100,
        json_mode=True,
    )


async def check_pair_duplicate(
    headline1: str,
    text1: str,
    headline2: str,
    text2: str,
    openai_api_key=config.openai_api_key,
) -> bool:
    """
    Проверяет пару сообщений на дубликат через GPT.

    Использует шаблон static/deduplicate_pair_prompt.txt.

    Args:
        headline1: заголовок первого сообщения
        text1: текст/summary первого сообщения
        headline2: заголовок второго сообщения
        text2: текст/summary второго сообщения
        openai_api_key: API ключ OpenAI

    Returns:
        bool: True если дубликат, False иначе
    """
    # Read file in thread pool to avoid blocking
    def read_prompt():
        with open('static/deduplicate_pair_prompt.txt', 'r', encoding='utf-8') as f:
            return f.read()

    prompt_template = await asyncio.to_thread(read_prompt)
    prompt = prompt_template.format(
        headline1=headline1 or '',
        text1=text1 or '',
        headline2=headline2 or '',
        text2=text2 or '',
    )

    try:
        response = await call_gpt(
            prompt,
            'Ты помощник для проверки дубликатов новостей. Всегда отвечай валидным JSON.',
            openai_api_key=openai_api_key,
            max_tokens=50,
            json_mode=True,
        )
        import json
        result = json.loads(response)
        return result.get('is_duplicate', False)
    except Exception:
        config.logger.exception('Error parsing duplicate check response')
        return False
