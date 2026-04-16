from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from project.features.event_scoring import select_model_feature_frame, split_feature_columns

_DEFAULT_LABEL_CANDIDATES: tuple[str, ...] = (
    "genuine_edge",
    "promoted_edge",
    "is_genuine_edge",
    "is_promoted_edge",
    "edge_label",
    "label",
    "target",
)

_DEFAULT_SPLIT_ORDER: tuple[str, ...] = ("train", "validation", "test")
_SKLEARN_DEPENDENCY_ERROR = "scikit-learn is required for event confidence modeling."


def _load_sklearn_objects() -> dict[str, Any]:
    try:
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder
    except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
        raise ModuleNotFoundError(_SKLEARN_DEPENDENCY_ERROR) from exc
    return {
        "CalibratedClassifierCV": CalibratedClassifierCV,
        "ColumnTransformer": ColumnTransformer,
        "HistGradientBoostingClassifier": HistGradientBoostingClassifier,
        "SimpleImputer": SimpleImputer,
        "average_precision_score": average_precision_score,
        "brier_score_loss": brier_score_loss,
        "log_loss": log_loss,
        "roc_auc_score": roc_auc_score,
        "Pipeline": Pipeline,
        "OneHotEncoder": OneHotEncoder,
    }


@dataclass
class EventConfidenceModel:
    estimator: Any
    feature_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    label_column: str
    split_column: str = "split_label"
    calibration_method: str = "sigmoid"
    training_summary: dict[str, Any] = field(default_factory=dict)

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        matrix = _prepare_feature_frame(
            frame,
            feature_columns=self.feature_columns,
            numeric_columns=self.numeric_columns,
            categorical_columns=self.categorical_columns,
        )
        if matrix.empty:
            return np.full(len(frame), 0.5, dtype=float)
        probs = self.estimator.predict_proba(matrix)
        if probs.ndim == 1:
            return probs.astype(float)
        if probs.shape[1] == 1:
            return probs[:, 0].astype(float)
        return probs[:, -1].astype(float)

    def score_frame(self, frame: pd.DataFrame, *, output_column: str = "event_confidence") -> pd.DataFrame:
        out = frame.copy()
        out[output_column] = self.predict_proba(out)
        return out

    def save(self, path: str | Path) -> Path:
        resolved = Path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        with resolved.open("wb") as handle:
            pickle.dump(self, handle)
        return resolved

    @classmethod
    def load(cls, path: str | Path) -> "EventConfidenceModel":
        with Path(path).open("rb") as handle:
            loaded = pickle.load(handle)
        if not isinstance(loaded, cls):
            raise TypeError(f"Unexpected model object: {type(loaded)!r}")
        return loaded


def _resolve_label_column(frame: pd.DataFrame, label_column: str | None = None) -> str:
    if label_column and label_column in frame.columns:
        return label_column
    for candidate in _DEFAULT_LABEL_CANDIDATES:
        if candidate in frame.columns:
            return candidate
    raise ValueError(
        "No supported label column found. Expected one of: " + ", ".join(_DEFAULT_LABEL_CANDIDATES)
    )


def _coerce_binary_target(series: pd.Series) -> pd.Series:
    values = series.copy()
    if pd.api.types.is_bool_dtype(values):
        return values.astype(int)
    if pd.api.types.is_numeric_dtype(values):
        return values.fillna(0).astype(float).clip(0, 1).round().astype(int)
    normalized = values.astype(str).str.strip().str.lower()
    truthy = {"1", "true", "t", "yes", "y", "edge", "promoted", "genuine"}
    falsy = {"0", "false", "f", "no", "n", "", "none", "nan"}
    return normalized.map(lambda item: 1 if item in truthy else 0 if item in falsy else np.nan).fillna(0).astype(int)


def _split_frame_by_label(frame: pd.DataFrame, split_column: str) -> dict[str, pd.DataFrame]:
    if split_column not in frame.columns:
        return {"train": frame.copy()}
    split_values = frame[split_column].astype(str).str.strip().str.lower()
    out: dict[str, pd.DataFrame] = {}
    for split in _DEFAULT_SPLIT_ORDER:
        mask = split_values == split
        if mask.any():
            out[split] = frame.loc[mask].copy()
    if "train" not in out:
        out["train"] = frame.copy()
    return out


def _prepare_feature_frame(
    frame: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    numeric_columns: Sequence[str],
    categorical_columns: Sequence[str],
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(index=frame.index)
    cols = [column for column in feature_columns if column in frame.columns]
    features = select_model_feature_frame(frame.loc[:, cols], candidate_columns=cols, include_registry_only=True)
    if features.empty:
        return features

    out = pd.DataFrame(index=features.index)
    for column in numeric_columns:
        if column not in features.columns:
            continue
        out[column] = pd.to_numeric(features[column], errors="coerce")
    for column in categorical_columns:
        if column not in features.columns:
            continue
        out[column] = features[column].astype("object").where(features[column].notna(), "__MISSING__").astype(str)
    return out


def _build_pipeline(
    *,
    numeric_columns: Sequence[str],
    categorical_columns: Sequence[str],
    random_state: int = 42,
) -> Any:
    sklearn = _load_sklearn_objects()
    ColumnTransformer = sklearn["ColumnTransformer"]
    HistGradientBoostingClassifier = sklearn["HistGradientBoostingClassifier"]
    Pipeline = sklearn["Pipeline"]
    SimpleImputer = sklearn["SimpleImputer"]
    OneHotEncoder = sklearn["OneHotEncoder"]
    transformers: list[tuple[str, Any, list[str]]] = []
    if numeric_columns:
        transformers.append(("num", SimpleImputer(strategy="median"), list(numeric_columns)))
    if categorical_columns:
        transformers.append(
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                list(categorical_columns),
            )
        )

    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        sparse_threshold=0.0,
        verbose_feature_names_out=False,
    )
    classifier = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=5,
        max_iter=250,
        min_samples_leaf=20,
        l2_regularization=0.1,
        random_state=random_state,
    )
    return Pipeline([("preprocessor", preprocessor), ("classifier", classifier)])


def _metrics(y_true: Sequence[int], y_prob: Sequence[float]) -> dict[str, float]:
    sklearn = _load_sklearn_objects()
    average_precision_score = sklearn["average_precision_score"]
    brier_score_loss = sklearn["brier_score_loss"]
    log_loss = sklearn["log_loss"]
    roc_auc_score = sklearn["roc_auc_score"]
    y_arr = np.asarray(list(y_true), dtype=int)
    p_arr = np.asarray(list(y_prob), dtype=float)
    classes = set(y_arr.tolist())
    out = {
        "n": float(len(y_arr)),
        "positive_rate": float(np.mean(y_arr)) if len(y_arr) else 0.0,
        "brier_score": float(brier_score_loss(y_arr, p_arr)) if len(classes) > 1 else 0.0,
        "average_precision": float(average_precision_score(y_arr, p_arr)) if len(classes) > 1 else 0.0,
        "roc_auc": float(roc_auc_score(y_arr, p_arr)) if len(classes) > 1 else 0.0,
        "log_loss": float(log_loss(y_arr, np.column_stack([1.0 - p_arr, p_arr]), labels=[0, 1]))
        if len(classes) > 1
        else 0.0,
    }
    return out


def train_event_confidence_model(
    frame: pd.DataFrame,
    *,
    label_column: str | None = None,
    split_column: str = "split_label",
    candidate_feature_columns: Sequence[str] | None = None,
    random_state: int = 42,
    calibration_method: str = "sigmoid",
    min_train_rows: int = 50,
) -> EventConfidenceModel:
    if frame.empty:
        raise ValueError("Cannot train an event confidence model on an empty dataframe")

    resolved_label = _resolve_label_column(frame, label_column=label_column)
    _ = _coerce_binary_target(frame[resolved_label])

    split_frames = _split_frame_by_label(frame, split_column)
    train_df = split_frames.get("train", frame).copy()
    valid_df = split_frames.get("validation", pd.DataFrame(index=frame.index[:0])).copy()
    test_df = split_frames.get("test", pd.DataFrame(index=frame.index[:0])).copy()

    feature_source = train_df if not train_df.empty else frame
    feature_report = split_feature_columns(
        feature_source,
        candidate_columns=candidate_feature_columns,
        include_registry_only=True,
    )
    feature_columns = tuple(column for column in feature_report["all"] if column not in {resolved_label, split_column})
    if not feature_columns:
        raise ValueError("No PIT-safe feature columns available for event confidence training")

    prepared_train = _prepare_feature_frame(
        train_df,
        feature_columns=feature_columns,
        numeric_columns=feature_report["numeric"],
        categorical_columns=feature_report["categorical"],
    )
    if len(prepared_train) < min_train_rows:
        raise ValueError(
            f"Insufficient train rows for event confidence model: {len(prepared_train)} < {min_train_rows}"
        )

    sklearn = _load_sklearn_objects()
    CalibratedClassifierCV = sklearn["CalibratedClassifierCV"]
    y_train = _coerce_binary_target(train_df.loc[prepared_train.index, resolved_label])
    pipeline = _build_pipeline(
        numeric_columns=feature_report["numeric"],
        categorical_columns=feature_report["categorical"],
        random_state=random_state,
    )
    pipeline.fit(prepared_train, y_train)

    estimator: Any = pipeline
    calibration_summary: dict[str, float] = {}
    if not valid_df.empty and resolved_label in valid_df.columns:
        prepared_valid = _prepare_feature_frame(
            valid_df,
            feature_columns=feature_columns,
            numeric_columns=feature_report["numeric"],
            categorical_columns=feature_report["categorical"],
        )
        if not prepared_valid.empty and prepared_valid.shape[0] >= 10:
            y_valid = _coerce_binary_target(valid_df.loc[prepared_valid.index, resolved_label])
            try:
                calibrated = CalibratedClassifierCV(estimator=pipeline, method=calibration_method, cv="prefit")
                calibrated.fit(prepared_valid, y_valid)
            except Exception:
                calibrated = None
            if calibrated is not None:
                estimator = calibrated
                calibration_summary = {"validation_rows": float(len(prepared_valid))}

    model = EventConfidenceModel(
        estimator=estimator,
        feature_columns=feature_columns,
        numeric_columns=tuple(feature_report["numeric"]),
        categorical_columns=tuple(feature_report["categorical"]),
        label_column=resolved_label,
        split_column=split_column,
        calibration_method=calibration_method,
        training_summary={},
    )
    model.training_summary.update(
        {
            "train": _metrics(y_train, model.predict_proba(prepared_train)),
            "validation": {},
            "test": {},
            **calibration_summary,
        }
    )

    if not test_df.empty and resolved_label in test_df.columns:
        prepared_test = _prepare_feature_frame(
            test_df,
            feature_columns=feature_columns,
            numeric_columns=feature_report["numeric"],
            categorical_columns=feature_report["categorical"],
        )
        if not prepared_test.empty:
            y_test = _coerce_binary_target(test_df.loc[prepared_test.index, resolved_label])
            model.training_summary["test"] = _metrics(
                y_test,
                model.predict_proba(prepared_test),
            )

    return model


def load_event_confidence_model(path: str | Path) -> EventConfidenceModel:
    return EventConfidenceModel.load(path)


def score_detected_events(frame: pd.DataFrame, model: EventConfidenceModel, *, output_column: str = "event_confidence") -> pd.DataFrame:
    return model.score_frame(frame, output_column=output_column)
