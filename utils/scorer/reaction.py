"""
utils.scorer.reaction

Выбор и постановка реакции бота + расчёт weighted score без учёта бота.
"""

import json

import httpx

import config
from utils.telegram.sender import DEFAULT_REACTION_WEIGHT, REACTION_WEIGHTS


def choose_bot_reaction(score: float) -> str | None:
    """
    Выбирает реакцию бота по prediction score.

    Returns:
        '👍' при score >= pos_threshold,
        '👎' при score <= neg_threshold,
        None между порогами (cold start тоже сюда попадает при 0.5).
    """
    pos = getattr(config, 'scorer_pos_threshold', 0.6)
    neg = getattr(config, 'scorer_neg_threshold', 0.4)
    if score >= pos:
        return '\U0001f44d'  # 👍
    if score <= neg:
        return '\U0001f44e'  # 👎
    return None


async def set_message_reaction(
    channel_id: str,
    message_id: int,
    emoji: str,
    bot_token: str = None,
) -> bool:
    """
    Ставит реакцию на сообщение через Bot API setMessageReaction.

    Returns:
        True если успешно.
    """
    bot_token = bot_token or config.bot_token
    url = f'https://api.telegram.org/bot{bot_token}/setMessageReaction'
    data = {
        'chat_id': channel_id,
        'message_id': message_id,
        'reaction': json.dumps([{'type': 'emoji', 'emoji': emoji}]),
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data, timeout=30.0)
        result = response.json()
        if result.get('ok'):
            return True
        config.logger.warning(f'setMessageReaction failed: {result.get("description")}')
        return False
    except Exception:
        config.logger.exception('Error setting message reaction')
        return False


def compute_weighted_score_excluding_bot(
    reactions: list[tuple[str, int]],
    bot_reaction: str | None,
) -> int:
    """
    Считает взвешенный score реакций, вычитая 1 из count emoji бота.

    Args:
        reactions: [(emoji, count), ...]
        bot_reaction: emoji, которую поставил бот (или None)

    Returns:
        int: взвешенная сумма
    """
    total = 0
    bot_subtracted = False
    for emoji, count in reactions:
        adjusted_count = count
        if not bot_subtracted and bot_reaction and emoji == bot_reaction:
            adjusted_count = max(0, count - 1)
            bot_subtracted = True
        weight = REACTION_WEIGHTS.get(emoji, DEFAULT_REACTION_WEIGHT)
        total += weight * adjusted_count
    return total
