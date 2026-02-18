"""
Кэш количества подписчиков каналов и отслеживание ежедневного дайджеста.

Правила инвалидации кэша подписчиков (should_refresh):
    - Когда кэш пуст
    - Когда в диапазоне дат сменился день (to_datetime.date() изменился)
    - Новые каналы подгружаются инкрементально через get_missing_channels

Отслеживание дайджеста:
    - Хранит последнюю дату отправки дайджеста
    - Возвращает предыдущий диапазон дат при смене даты (для дайджеста)
"""

from datetime import datetime, date
from typing import Dict, Optional, Tuple

from config import logger

log = logger


class SubscribersCache:
    """
    In-memory кэш для количества подписчиков каналов и отслеживания дайджеста.

    Атрибуты:
        _cache: Dict[str, int] - {channel_id: subscribers_count}
        _last_date_range: последний диапазон дат
        _last_digest_date: дата последнего отправленного дайджеста
    """

    def __init__(self):
        """Инициализирует пустой кэш."""
        self._cache: Dict[str, int] = {}
        self._last_date_range: Optional[Tuple[datetime, datetime]] = None
        self._last_digest_date: Optional[date] = None

    def should_refresh(self, to_datetime: datetime) -> bool:
        """
        Проверяет, нужно ли полное обновление кэша.

        Условия обновления:
            - Кэш пустой
            - День в to_datetime сменился

        Новые каналы, которых нет в кэше, подгружаются инкрементально
        через get_missing_channels.

        Args:
            to_datetime: конец периода (для проверки смены дня)

        Returns:
            bool: True если нужно обновить кэш
        """
        # 1. Кэш пустой
        if not self._cache:
            log.info('Subscribers cache: empty, will fetch all')
            return True

        # 2. День сменился
        if self._last_date_range is not None:
            prev_date = self._last_date_range[1].date()
            current_date = to_datetime.date()
            if current_date != prev_date:
                log.info(f'Subscribers cache: date changed from {prev_date} to {current_date}, will refresh')
                return True

        return False

    def should_send_digest(self, from_datetime: datetime, to_datetime: datetime) -> Optional[Tuple[datetime, datetime]]:
        """
        Проверяет, нужно ли отправить дайджест.

        Триггер: 12:00 находится между from_datetime и to_datetime.
        Дайджест отправляется за вчерашний день (00:00 - 23:59:59).

        Returns:
            Tuple[datetime, datetime]: (digest_from, digest_to) за вчера
            None: если дайджест не нужен
        """
        from datetime import timedelta

        # Триггер — 12:00 сегодня
        today = to_datetime.date()
        trigger_time = datetime.combine(today, datetime.min.time().replace(hour=12))

        # Проверяем, что 12:00 попадает в диапазон [from_datetime, to_datetime]
        if not (from_datetime <= trigger_time <= to_datetime):
            return None

        # Вчерашний день
        yesterday = today - timedelta(days=1)

        # Уже отправляли дайджест за вчера?
        if self._last_digest_date == yesterday:
            return None

        log.info(f'Trigger 12:00 in range [{from_datetime}, {to_datetime}], digest needed for {yesterday}')

        # Диапазон за вчера: 00:00:00 - 23:59:59
        digest_from = datetime.combine(yesterday, datetime.min.time())
        digest_to = datetime.combine(yesterday, datetime.max.time())

        return (digest_from, digest_to)

    def mark_digest_sent(self, digest_date: date):
        """Отмечает, что дайджест отправлен за указанную дату."""
        self._last_digest_date = digest_date
        log.info(f'Marked digest sent for {digest_date}')

    def get(self, channel_id: str) -> Optional[int]:
        """Получает количество подписчиков канала из кэша."""
        return self._cache.get(str(channel_id))

    def has(self, channel_id: str) -> bool:
        """Проверяет, есть ли канал в кэше."""
        return str(channel_id) in self._cache

    def set(self, channel_id: str, count: int):
        """Устанавливает количество подписчиков для канала."""
        self._cache[str(channel_id)] = count

    def set_date_range(self, from_datetime: datetime, to_datetime: datetime):
        """Обновляет диапазон дат для отслеживания инвалидации."""
        self._last_date_range = (from_datetime, to_datetime)

    def get_missing_channels(self, channel_ids: set) -> set:
        """Возвращает каналы, которых нет в кэше."""
        return {str(cid) for cid in channel_ids if str(cid) not in self._cache}

    def clear(self):
        """Очищает кэш."""
        self._cache.clear()
        self._last_date_range = None

    def get_all(self) -> Dict[str, int]:
        """Возвращает копию всего кэша."""
        return self._cache.copy()


# Global cache instance
_subscribers_cache = SubscribersCache()


def get_subscribers_cache() -> SubscribersCache:
    """Возвращает глобальный экземпляр кэша."""
    return _subscribers_cache
