"""
utils.scorer.scorer

NewsScorer — ML-предсказание качества новости [0..1].
SGDClassifier с online-обучением через partial_fit.
"""

import json
import os
import threading
from datetime import datetime, timezone

import numpy as np
from sklearn.linear_model import SGDClassifier

import config
from utils.scorer.embedder import embed_texts
from utils.scorer.features import build_feature_vector

try:
    import joblib
except ImportError:
    joblib = None


class NewsScorer:
    """ML-скорер новостей с online-обучением."""

    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._lock = threading.Lock()
        self._model: SGDClassifier | None = None
        self._known_tags: list[str] = []
        self._sample_count: int = 0
        self._last_trained_at: str | None = None  # ISO datetime of last training
        self._centroid: np.ndarray | None = None  # mean embedding (384,)
        self._min_samples = int(getattr(config, 'scorer_min_samples', 30))
        self._load()

    # ---- persistence ----

    def _model_path(self) -> str:
        return os.path.join(self._data_dir, 'scorer_model.joblib')

    def _meta_path(self) -> str:
        return os.path.join(self._data_dir, 'scorer_meta.json')

    def _load(self):
        meta_path = self._meta_path()
        if os.path.exists(meta_path):
            with open(meta_path, encoding='utf-8') as f:
                meta = json.load(f)
            self._known_tags = meta.get('known_tags', [])
            self._sample_count = meta.get('sample_count', 0)
            self._last_trained_at = meta.get('last_trained_at')
            centroid_list = meta.get('centroid')
            if centroid_list:
                self._centroid = np.array(centroid_list, dtype=np.float32)

        model_path = self._model_path()
        if os.path.exists(model_path) and joblib is not None:
            self._model = joblib.load(model_path)

    def _save(self):
        os.makedirs(self._data_dir, exist_ok=True)
        meta = {
            'known_tags': self._known_tags,
            'sample_count': self._sample_count,
            'last_trained_at': self._last_trained_at,
            'centroid': self._centroid.tolist() if self._centroid is not None else None,
        }
        with open(self._meta_path(), 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)

        if self._model is not None and joblib is not None:
            joblib.dump(self._model, self._model_path())

    # ---- public API ----

    @property
    def sample_count(self) -> int:
        return self._sample_count

    @property
    def last_trained_at(self) -> str | None:
        return self._last_trained_at

    def predict(
        self,
        headline: str,
        summary: str,
        hashtags: list[str] | None,
        text_length: int,
        msg_dttm=None,
    ) -> float:
        """
        Предсказывает score [0..1] для новости.

        Возвращает 0.5 при cold start (< min_samples обучающих примеров).
        """
        with self._lock:
            if self._sample_count < self._min_samples or self._model is None:
                return 0.5

            vec = build_feature_vector(
                headline, summary, hashtags,
                text_length, msg_dttm,
                self._known_tags,
                centroid=self._centroid,
            )
            # Pad/truncate to match model dimensions
            expected = self._model.coef_.shape[1]
            vec = self._pad_or_truncate(vec, expected)
            proba = self._model.predict_proba(vec.reshape(1, -1))
            # Return probability of class 1 (positive)
            idx = list(self._model.classes_).index(1) if 1 in self._model.classes_ else -1
            return float(proba[0][idx]) if idx >= 0 else 0.5

    def train(self, training_data: list[tuple[dict, int]]):
        """
        Online-обучение на batch данных.

        Args:
            training_data: list of (features_dict, label)
                features_dict keys: headline, summary, hashtags, text_length, msg_dttm
                label: 0 или 1
        """
        if not training_data:
            return

        with self._lock:
            # Update known_tags — append new items at the end
            # to preserve positional correspondence with model weights
            existing_tags = set(self._known_tags)
            for feat, _label in training_data:
                for tag in feat.get('hashtags') or []:
                    if tag not in existing_tags:
                        self._known_tags.append(tag)
                        existing_tags.add(tag)

            # Compute centroid from all training texts
            texts = [
                f'{feat.get("headline", "")} {feat.get("summary", "")}'.strip() or 'empty'
                for feat, _label in training_data
            ]
            embeddings = embed_texts(texts)  # (n, 384)
            self._centroid = embeddings.mean(axis=0).astype(np.float32)

            # Build feature matrix
            X_list = []
            y_list = []
            for feat, label in training_data:
                vec = build_feature_vector(
                    feat.get('headline', ''),
                    feat.get('summary', ''),
                    feat.get('hashtags'),
                    feat.get('text_length', 0),
                    feat.get('msg_dttm'),
                    self._known_tags,
                    centroid=self._centroid,
                )
                X_list.append(vec)
                y_list.append(label)

            X = np.array(X_list, dtype=np.float32)
            y = np.array(y_list, dtype=np.int32)

            if self._model is None:
                self._model = SGDClassifier(loss='log_loss', random_state=42, warm_start=True)
                self._model.partial_fit(X, y, classes=np.array([0, 1]))
            else:
                # Pad coef_ if feature dimensions grew
                expected_dim = X.shape[1]
                current_dim = self._model.coef_.shape[1]
                if expected_dim > current_dim:
                    pad_width = expected_dim - current_dim
                    self._model.coef_ = np.pad(self._model.coef_, ((0, 0), (0, pad_width)), constant_values=0)
                    self._model.n_features_in_ = expected_dim
                elif expected_dim < current_dim:
                    # Pad X to match model (shouldn't happen normally)
                    X = np.pad(X, ((0, 0), (0, current_dim - expected_dim)), constant_values=0)

                self._model.partial_fit(X, y)

            self._sample_count += len(training_data)
            self._last_trained_at = datetime.now(timezone.utc).isoformat()
            self._save()

    # ---- helpers ----

    @staticmethod
    def _pad_or_truncate(vec: np.ndarray, expected: int) -> np.ndarray:
        if vec.shape[0] == expected:
            return vec
        if vec.shape[0] < expected:
            return np.pad(vec, (0, expected - vec.shape[0]), constant_values=0)
        return vec[:expected]
