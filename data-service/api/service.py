from pathlib import Path
import logging
import sys

# импортируем наш новый легковесный сервис вместо тяжелого пайплайна
from pipelines.api_pipeline import EquipmentAPIService, VesselAPIService
from configs.config_loader import config

# добавляем корень проекта в sys.path для импорта ml_service
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ml_service.predictor import PumpPredictor, VesselPredictor

logger = logging.getLogger(__name__)

# определяем пути
BASE_DIR = Path(__file__).resolve().parent.parent
DATASETS_DIR = BASE_DIR / 'datasets'

# инициализируем сервис
pump_service = EquipmentAPIService(
    output_folder_path=DATASETS_DIR,
    config=config['api']
)

# инициализируем предиктор (загрузка модели один раз при старте)
pump_predictor = PumpPredictor()

# инициализируем сервис для сосудов
vessel_service = VesselAPIService(
    output_folder_path=DATASETS_DIR,
    config=config.get('equipment', {}).get('vessel_inference', {}) # Передаем конфиг для инференса
)

vessel_predictor = VesselPredictor()

def get_pump_estimation(input_data: dict) -> dict:
    """Прослойка между API и расчетами для насосов"""
    try:
        # 1. прогоняем через пайплайн (очистка + энричинг + FE)
        processed_features = pump_service.process_request(input_data)

        # 2. вызов ML-модели
        predicted_weight = pump_predictor.predict(processed_features)

        return {
            "weight": round(float(predicted_weight), 2),
            "features": processed_features  # возвращаем названия колонок для отладки
        }
    except Exception as e:
        logger.error(f"Ошибка в сервисе оценки насоса: {e}")
        # прокидываем ошибку наверх, чтобы FastAPI мог вернуть понятный 400 статус
        raise ValueError(f"Ошибка обработки данных: {str(e)}")

def get_vessel_estimation(input_data: dict) -> dict:
    """Прослойка между API и расчетами для сосудов (Vessel)"""
    try:
        # маппинг полей из API в формат сервиса (если нужно)
        # У нас API отдает vessel_diameter, vessel_tangent_to_tangent_height и т.д.
        # преобразуем к названиям для сервиса
        mapped_data = {
            "tag": input_data.get("tag"),
            "diameter": input_data.get("vessel_diameter"),
            "ss_distance": input_data.get("vessel_tangent_to_tangent_height"),
            "pressure": input_data.get("design_gauge_pressure", 0),
            "sk_height": input_data.get("skirt_height", 0),
            "leg_height": input_data.get("vessel_leg_height", 0),
            "temp": input_data.get("design_temperature", 0)
        }

        # 1. прогоняем через пайплайн (очистка + FE)
        processed_features = vessel_service.process_request(mapped_data)

        # 2. вызов ML-модели
        predicted_weight = vessel_predictor.predict(processed_features)

        return {
            "weight": round(float(predicted_weight), 2),
            "features": processed_features  # возвращаем названия колонок для отладки
        }
    except Exception as e:
        logger.error(f"Ошибка в сервисе оценки сосуда: {e}")
        raise ValueError(f"Ошибка обработки данных: {str(e)}")