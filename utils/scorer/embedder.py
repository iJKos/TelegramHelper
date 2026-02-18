"""
utils.scorer.embedder

Lazy-loaded sentence-transformers embedder (paraphrase-multilingual-MiniLM-L12-v2).
"""

import threading

import numpy as np

_model = None
_lock = threading.Lock()


def _get_model():
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                from sentence_transformers import SentenceTransformer
                _model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    return _model


def embed_texts(texts: list[str]) -> np.ndarray:
    """
    Возвращает эмбеддинги текстов через paraphrase-multilingual-MiniLM-L12-v2.

    Args:
        texts: список строк

    Returns:
        np.ndarray shape (n, 384)
    """
    model = _get_model()
    return model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
