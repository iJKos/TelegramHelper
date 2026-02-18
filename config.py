

from dotenv import load_dotenv
load_dotenv()  # автоматически загрузит переменные из .env-файла

import os
from datetime import datetime, timedelta

import logging
logger= logging.getLogger("uvicorn")


def get_secret_or_env(var_name, default=None):
    """
    Получает значение конфигурации из переменных окружения или Google Secret Manager.

    Порядок поиска:
        1. Переменная окружения
        2. Google Secret Manager (если доступен)
        3. Значение по умолчанию

    Args:
        var_name: имя переменной
        default: значение по умолчанию

    Returns:
        str: строковое значение конфигурации
    """
    # 1. Пробуем взять из окружения
    value = os.getenv(var_name)
    if value is not None:
        return value
    # 2. Пробуем взять из Google Secret Manager
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
        if project_id:
            secret_path = f"projects/{project_id}/secrets/{var_name}/versions/latest"
            response = client.access_secret_version(request={"name": secret_path})
            return response.payload.data.decode("UTF-8")
    except Exception:
        pass
    # 3. Если ничего не найдено — возвращаем значение по умолчанию
    return default

api_id = get_secret_or_env('TGHELPER_API_ID', 'YOUR_API_ID')
api_hash = get_secret_or_env('TGHELPER_API_HASH', 'YOUR_API_HASH')
message_limit = int(os.getenv('TGHELPER_MESSAGE_LIMIT', 100))
poll_delay = int(os.getenv('TGHELPER_POLL_DELAY', 0.7))
openai_api_key = get_secret_or_env('OPENAI_API_KEY', 'YOUR_OPENAI_API_KEY')
folder_name = os.getenv('TGHELPER_FOLDER_NAME', 'YOUR_FOLDER_NAME')
bot_token = get_secret_or_env('TGHELPER_BOT_TOKEN', 'YOUR_BOT_TOKEN')
output_channel_id = os.getenv('TGHELPER_OUTPUT_CHANNEL_ID', '@your_output_channel')
duckdb_path = os.getenv('TGHELPER_DUCKDB_PATH', './data/main')
is_mock = os.getenv('TGHELPER_MOCK', 'true').lower() == 'false'

# Concurrency for outbound Telegram sends
send_concurrency = int(os.getenv('TGHELPER_SEND_CONCURRENCY', '5'))
# Window in days for reading emodji counts on sent messages
emodji_window_days = int(os.getenv('TGHELPER_EMODJI_WINDOW_DAYS', '10'))
# Window in days for deduplication check
dedup_window_days = int(os.getenv('TGHELPER_DEDUP_WINDOW_DAYS', '4'))
# Default start date if no date in database (ISO8601 format)
default_start_date = os.getenv('TGHELPER_DEFAULT_START_DATE', '2025-10-01')
# Required hashtags for sending messages (comma-separated)
required_hashtags = [tag.strip() for tag in os.getenv('TGHELPER_REQUIRED_HASHTAGS', '#news').split(',')]

# Scorer settings
scorer_min_samples = int(os.getenv('TGHELPER_SCORER_MIN_SAMPLES', '30'))
scorer_training_days = int(os.getenv('TGHELPER_SCORER_TRAINING_DAYS', '30'))
scorer_pos_threshold = float(os.getenv('TGHELPER_SCORER_POS_THRESHOLD', '0.6'))
scorer_neg_threshold = float(os.getenv('TGHELPER_SCORER_NEG_THRESHOLD', '0.25'))
# Probability of sending a message predicted to get a dislike (0.0–1.0)
low_score_send_probability = float(os.getenv('TGHELPER_LOW_SCORE_SEND_PROBABILITY', '0.3'))

