# ml_service/predictor.py
"""
Модуль предсказания веса насосов.
Загружает обученную модель GradientBoostingRegressor и PolynomialFeatures
из файлов joblib и предоставляет интерфейс для предсказания.
"""

import numpy as np
import joblib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Порядок параметров, которые ожидает модель (должен совпадать с обучением)
FEATURE_COLUMNS = [
    "flow_rate",
    "fluid_head",
    "rpm",
    "spec_gravity",
    "power_kw",
    "diameter_proxy",
    "useful_kw_log",
]

# Путь к папке с моделью
MODELS_DIR = Path(__file__).resolve().parent / "models" / "pump"


class PumpPredictor:
    """Предиктор веса насоса с помощью GradientBoosting и PolynomialFeatures."""

    def __init__(self, models_dir: Path = MODELS_DIR):
        model_path = models_dir / "model_gb.joblib"
        poly_path = models_dir / "poly_features.joblib"

        if not model_path.exists():
            raise FileNotFoundError(f"Файл модели не найден: {model_path}")
        if not poly_path.exists():
            raise FileNotFoundError(f"Файл PolynomialFeatures не найден: {poly_path}")

        self.model = joblib.load(model_path)
        self.poly = joblib.load(poly_path)
        logger.info("Модель насоса загружена успешно (model_gb + poly_features).")

    def predict(self, features_dict: dict) -> float:
        # Собираем вектор параметров в нужном порядке
        raw_features = []
        for col in FEATURE_COLUMNS:
            value = features_dict.get(col)
            if value is None:
                raise ValueError(f"Отсутствует обязательный параметр для модели: '{col}'")
            raw_features.append(float(value))

        X = np.array([raw_features])

        # Полиномиальные / кросс-признаки
        X_poly = self.poly.transform(X)

        # Предсказание (модель возвращает weight_log)
        weight_log = self.model.predict(X_poly)[0]

        # Обратное преобразование: exp(weight_log) = вес в кг
        weight_kg = np.exp(weight_log)

        logger.info(f"Предсказание: weight_log={weight_log:.4f}, weight_kg={weight_kg:.2f}")
        return float(weight_kg)

# --- VESSEL PREDICTOR ---

VESSEL_FEATURE_COLUMNS = [
    "log_liq_volume",
    "log_diameter",
    "log_ss_distance",
    "log_p_abs",
    "log_area_calc",
    "log_thick_proxy",
    "des_temp",
    "sk_height",
    "leg_height",
    "has_skirt",
    "has_legs"
]

VESSEL_MODELS_DIR = Path(__file__).resolve().parent / "models" / "vessel"

class VesselPredictor:
    """Предиктор веса сосуда (Vessel) с помощью GradientBoosting."""

    def __init__(self, models_dir: Path = VESSEL_MODELS_DIR):
        model_path = models_dir / "vessel_model_final.joblib"

        if not model_path.exists():
            # Если нет в папке models/vessel, проверим в корне ml_service
            alt_path = Path(__file__).resolve().parent / "vessel_model_final.joblib"
            if alt_path.exists():
                model_path = alt_path
            else:
                raise FileNotFoundError(f"Файл модели не найден: {model_path}")

        self.model = joblib.load(model_path)
        logger.info("Модель сосуда загружена успешно (vessel_model_final).")

    def predict(self, features_dict: dict) -> float:
        # Собираем вектор параметров в нужном порядке
        raw_features = []
        for col in VESSEL_FEATURE_COLUMNS:
            value = features_dict.get(col)
            if value is None:
                raise ValueError(f"Отсутствует обязательный параметр для модели: '{col}'")
            raw_features.append(float(value))

        X = np.array([raw_features])

        # Предсказание (модель возвращает weight_log)
        weight_log = self.model.predict(X)[0]

        # Обратное преобразование: exp(weight_log) = вес в кг
        weight_kg = np.exp(weight_log)

        logger.info(f"Предсказание (Vessel): weight_log={weight_log:.4f}, weight_kg={weight_kg:.2f}")
        return float(weight_kg)
