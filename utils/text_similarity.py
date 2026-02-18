"""
TF-IDF + Cosine Similarity для сравнения текстов.

Используется для предварительной фильтрации дубликатов перед отправкой в ChatGPT.
Это экономит API-вызовы, отсекая явно непохожие пары.
"""

from typing import List, Tuple

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from config import logger

log = logger


def find_similar_pairs(
    new_headline: str,
    existing_headlines: List[Tuple[str, str]],  # list of (id, headline)
    threshold: float = 0.3,
) -> List[Tuple[str, float]]:
    """
    Находит похожие заголовки через TF-IDF + Cosine Similarity.

    Алгоритм:
        1. Создаёт TF-IDF векторы (unigrams + bigrams)
        2. Вычисляет косинусное сходство между новым и существующими
        3. Фильтрует пары выше порога

    Args:
        new_headline: проверяемый заголовок
        existing_headlines: список кортежей (id, headline)
        threshold: минимальный порог схожести (0-1)

    Returns:
        List[Tuple[str, float]]: список (id, score) отсортированный по убыванию score
    """
    if not new_headline or not existing_headlines:
        return []

    # Filter out empty headlines
    valid_headlines = [(id_, h) for id_, h in existing_headlines if h and h.strip()]
    if not valid_headlines:
        return []

    # Combine all texts for vectorization
    all_texts = [new_headline] + [h for _, h in valid_headlines]

    try:
        # Create TF-IDF vectors
        vectorizer = TfidfVectorizer(
            lowercase=True,
            analyzer='word',
            ngram_range=(1, 2),  # unigrams and bigrams
            min_df=1,
            max_df=0.95,
        )
        tfidf_matrix = vectorizer.fit_transform(all_texts)

        # Calculate cosine similarity between new headline (index 0) and all others
        similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()

        # Find pairs above threshold
        similar_pairs = []
        for i, score in enumerate(similarities):
            if score >= threshold:
                similar_pairs.append((valid_headlines[i][0], float(score)))

        # Sort by similarity score descending
        similar_pairs.sort(key=lambda x: x[1], reverse=True)

        if similar_pairs:
            log.debug(f'Found {len(similar_pairs)} similar headlines (threshold={threshold})')

        return similar_pairs

    except Exception as e:
        log.warning(f'TF-IDF similarity calculation failed: {e}')
        return []


def calculate_pairwise_similarity(headline1: str, headline2: str) -> float:
    """
    Вычисляет схожесть между двумя заголовками.

    Args:
        headline1: первый заголовок
        headline2: второй заголовок

    Returns:
        float: score от 0 до 1
    """
    if not headline1 or not headline2:
        return 0.0

    try:
        vectorizer = TfidfVectorizer(
            lowercase=True,
            analyzer='word',
            ngram_range=(1, 2),
            min_df=1,
        )
        tfidf_matrix = vectorizer.fit_transform([headline1, headline2])
        similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()[0]
        return float(similarity)
    except Exception as e:
        log.warning(f'Pairwise similarity calculation failed: {e}')
        return 0.0
