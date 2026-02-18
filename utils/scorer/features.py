"""
utils.scorer.features

Сборка feature-вектора для NewsScorer.
"""

import re
from datetime import datetime

import numpy as np

from utils.scorer.embedder import embed_texts


def _count_text_features(text: str) -> dict:
    """Извлекает числовые характеристики из текста."""
    words = text.split()
    sentences = re.split(r'[.!?]+', text)
    sentences = [s for s in sentences if s.strip()]
    urls = re.findall(r'https?://\S+', text)
    digits = sum(c.isdigit() for c in text)
    uppers = sum(c.isupper() for c in text)
    alpha = sum(c.isalpha() for c in text) or 1

    unique_words = set(w.lower() for w in words)

    return {
        'word_count': len(words),
        'sentence_count': len(sentences),
        'avg_word_len': np.mean([len(w) for w in words]) if words else 0.0,
        'url_count': len(urls),
        'digit_ratio': digits / len(text) if text else 0.0,
        'upper_ratio': uppers / alpha,
        'question_marks': text.count('?'),
        'exclamation_marks': text.count('!'),
        'type_token_ratio': len(unique_words) / len(words) if words else 0.0,
    }


def _cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance [0..2] между двумя векторами."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 1.0
    return 1.0 - float(np.dot(a, b) / (norm_a * norm_b))


def build_feature_vector(
    headline: str,
    summary: str,
    hashtags: list[str] | None,
    text_length: int,
    msg_dttm: datetime | None,
    known_tags: list[str],
    centroid: np.ndarray | None = None,
) -> np.ndarray:
    """
    Собирает feature-вектор из текста, метаданных и категориальных фич.

    Структура (fixed-size first, growable last):
        - MiniLM embedding (384 dims)
        - Numeric: text_length, hour, day_of_week, is_weekend,
          word_count, sentence_count, avg_word_len, url_count,
          digit_ratio, upper_ratio, question_marks, exclamation_marks,
          headline_len, hashtag_count, type_token_ratio,
          centroid_distance (16 dims)
        - One-hot hashtags (len(known_tags) dims) — growable

    Returns:
        np.ndarray shape (384 + 16 + len(known_tags),)
    """
    # Embedding
    text = f'{headline or ""} {summary or ""}'.strip() or 'empty'
    embedding = embed_texts([text])[0]  # (384,)

    # Text features from summary
    tf = _count_text_features(summary or '')

    # One-hot hashtags
    tag_set = set(hashtags or [])
    tag_features = np.array([1.0 if t in tag_set else 0.0 for t in known_tags], dtype=np.float32)

    # Numeric
    hour = msg_dttm.hour / 24.0 if msg_dttm else 0.5
    dow = msg_dttm.weekday() / 7.0 if msg_dttm else 0.5
    is_weekend = 1.0 if (msg_dttm and msg_dttm.weekday() >= 5) else 0.0
    tl = min(text_length, 10000) / 10000.0

    # Cosine distance from centroid (0.5 = no centroid available)
    centroid_dist = _cosine_distance(embedding, centroid) if centroid is not None else 0.5

    numeric = np.array([
        tl,
        hour,
        dow,
        is_weekend,
        min(tf['word_count'], 500) / 500.0,
        min(tf['sentence_count'], 30) / 30.0,
        min(tf['avg_word_len'], 20) / 20.0,
        min(tf['url_count'], 10) / 10.0,
        tf['digit_ratio'],
        tf['upper_ratio'],
        min(tf['question_marks'], 5) / 5.0,
        min(tf['exclamation_marks'], 5) / 5.0,
        min(len(headline or ''), 200) / 200.0,
        min(len(hashtags or []), 10) / 10.0,
        tf['type_token_ratio'],
        centroid_dist,
    ], dtype=np.float32)

    # Numeric + embedding first (fixed size), then one-hot (growable) at the end
    return np.concatenate([embedding, numeric, tag_features])
