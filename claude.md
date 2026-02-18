# Telegram Helper

FastAPI-сервис для автоматической агрегации новостей из Telegram-каналов с AI-суммаризацией, дедупликацией и публикацией.

## Tech Stack

- **Python 3.11+**
- **FastAPI** — Web framework для REST API
- **Telethon** — Telegram client library
- **OpenAI API** — Суммаризация и дедупликация
- **SQLite** — Локальная БД (thread-safe)
- **scikit-learn** — TF-IDF для предфильтрации дубликатов
- **Jinja2** — Шаблоны сообщений
- **Ruff** — Линтинг и форматирование

## Project Structure

```
├── main.py                  # FastAPI app, cron worker, endpoints
├── config.py                # Конфигурация из env / GCP Secret Manager
├── utils/
│   ├── models.py            # Dataclasses: ReadMessage, SentMessage + схема БД
│   ├── processor.py         # Основная логика обработки (7 шагов pipeline)
│   ├── gpt_utils.py         # OpenAI API: суммаризация, дедупликация
│   ├── text_similarity.py   # TF-IDF + Cosine Similarity
│   ├── msg_helper.py        # Форматирование сообщений для Telegram
│   ├── subscribers_cache.py # In-memory кэш подписчиков + дайджест
│   ├── sqlite/
│   │   ├── connection.py    # Thread-safe соединения с БД
│   │   ├── schema.py        # Создание таблиц и миграции
│   │   └── messages.py      # CRUD операции для read/sent messages
│   └── telegram/
│       ├── reader.py        # Чтение каналов из папки
│       └── sender.py        # Отправка сообщений и дайджеста
├── static/
│   ├── summarize_prompt.txt           # Промпт суммаризации
│   ├── deduplicate_prompt.txt         # Промпт дедупликации (batch)
│   ├── deduplicate_pair_prompt.txt    # Промпт проверки пары
│   ├── telegram_summary_template.html # Шаблон сообщения
│   └── daily_digest_template.txt      # Шаблон дайджеста
```

## Database

- SQLite с thread-safe блокировкой для записи
- Путь: `{TGHELPER_DUCKDB_PATH}_sqlite.db`
- Автоматические миграции при старте

## Commands

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск (development)
uvicorn main:app --reload

# Запуск (Docker)
docker-compose up -d

# Линтинг
ruff check .

# Форматирование
ruff format .

# Автофикс
ruff check --fix .
```

## Data Models

### ReadMessage
Сообщение, прочитанное из Telegram-канала.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | Уникальный ID (auto) |
| `telegram_id` | int | ID сообщения в Telegram |
| `channel_id` | str | ID канала |
| `author` | str | Username канала |
| `public_link` | str | Ссылка на сообщение |
| `raw_text` | str | Исходный текст |
| `text` | str | Очищенный текст |
| `urls` | list[str] | Извлечённые URL |
| `summary` | str | AI-саммари |
| `headline` | str | Заголовок |
| `hashtags` | list[str] | Хештеги |
| `state` | str | Статус: `read` → `clean` → `summarized` → `deduplicated` → `linked` |
| `msg_dttm` | datetime | Время сообщения |
| `sent_message_id` | UUID | Связь с SentMessage |

### SentMessage
Сообщение для отправки в выходной канал.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | Уникальный ID (auto) |
| `telegram_id` | int | ID после отправки |
| `text` | str | Текст для отправки |
| `read_message_id` | UUID | Основной ReadMessage |
| `state` | str | Статус: `new` → `to_send` → `sent` / `error` / `to_update` |
| `sent_at` | datetime | Время отправки |
| `message_dttm` | datetime | Время исходного сообщения |
| `emodji_count` | int | Количество реакций |
| `normalized_score` | float | Нормализованный score для дайджеста |

## Pipeline (7 шагов)

### Step 1: Read Messages
Чтение сообщений из всех каналов папки за период `[from_datetime, to_datetime]`.
- Фильтрация: пропуск сообщений с `#реклама` и короче 100 символов
- Сохранение с `state='read'`

### Step 2: Clean Messages
Очистка текста для сообщений с `state='read'`.
- Удаление markdown-разметки
- Извлечение URL
- Удаление хештегов из текста
- `state='clean'`

### Step 3: Summarize
AI-суммаризация для сообщений с `state='clean'`.
- Генерация: summary, headline, hashtags
- Параллельная обработка (batch)
- `state='summarized'` или `state='error'`

### Step 4: Deduplicate (TF-IDF + GPT)
Двухэтапная дедупликация для `state='summarized'`:
1. **TF-IDF** — быстрый поиск похожих по headline (threshold=0.3)
2. **GPT** — точная проверка пар-кандидатов

Если дубликат найден:
- Линковка к существующему SentMessage
- `state='deduplicated'`

Если уникальный:
- Создание нового SentMessage с `state='new'`
- `state='linked'`

### Step 5: Generate Text
Форматирование текста для `state='new'` и `state='to_update'`.
- Jinja2 шаблон `telegram_summary_template.html`
- Список источников из всех linked ReadMessage
- `state='to_send'`

### Step 6: Send Messages
Отправка/обновление для `state='to_send'`.
- Новые: `sendMessage` API
- Обновление: `editMessageText` API
- Параллельная отправка с Semaphore
- `state='sent'` или `state='error'`

### Step 7: Read Reactions
Подсчёт реакций для `state='sent'` за последние N дней.
- Telethon API для чтения реакций
- Обновление `emodji_count`
- Расчёт `normalized_score` для дайджеста

### Daily Digest
При смене даты отправляется дайджест:
- Топ-10 новостей по `normalized_score`
- Шаблон `daily_digest_template.txt`

## API Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `POST` | `/start_cron` | Запустить cron worker |
| `POST` | `/stop_cron` | Остановить cron worker |
| `GET` | `/cron_status` | Статус cron worker |
| `GET` | `/read_messages` | Получить прочитанные сообщения |
| `GET` | `/sent_messages` | Получить отправленные сообщения |
| `POST` | `/renew_msg_data` | Синхронизация с каналом |

### Параметры запросов
```
?from_date=2024-01-01&to_date=2024-01-31&status=summarized
```

## Environment Variables

### Обязательные
| Переменная | Описание |
|------------|----------|
| `TGHELPER_API_ID` | Telegram API ID |
| `TGHELPER_API_HASH` | Telegram API Hash |
| `TGHELPER_BOT_TOKEN` | Токен бота |
| `OPENAI_API_KEY` | OpenAI API ключ |
| `TGHELPER_FOLDER_NAME` | Имя папки с каналами |
| `TGHELPER_OUTPUT_CHANNEL_ID` | Канал для публикации |

### Опциональные
| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `TGHELPER_MOCK` | `true` | Mock-режим (без отправки) |
| `TGHELPER_MESSAGE_LIMIT` | `100` | Лимит сообщений на канал |
| `TGHELPER_POLL_DELAY` | `0.7` | Задержка между каналами (сек) |
| `TGHELPER_DUCKDB_PATH` | `./.data/duckdb` | Путь к БД |
| `TGHELPER_CRON_INTERVAL` | `10` | Интервал cron (минуты) |
| `TGHELPER_DEDUP_WINDOW_HOURS` | `336` | Окно дедупликации (часы) |
| `TGHELPER_REACTIONS_WINDOW_DAYS` | `14` | Окно подсчёта реакций (дни) |
| `TGHELPER_SEND_CONCURRENCY` | `3` | Параллельных отправок |

## Code Style

- Одинарные кавычки для строк
- Максимальная длина строки: 120 символов
- Docstrings в Google-стиле (на русском)
- `ruff check --fix .` перед коммитом
