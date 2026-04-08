from pathlib import Path
import logging

# импортируем наш новый легковесный сервис вместо тяжелого пайплайна
from pipelines.api_pipeline import EquipmentAPIService
from configs.app_config import APP_CONFIG

logger = logging.getLogger(__name__)

# определяем пути
BASE_DIR = Path(__file__).resolve().parent.parent
DATASETS_DIR = BASE_DIR / 'datasets'

# инициализируем сервис
pump_service = EquipmentAPIService(
    output_folder_path=DATASETS_DIR,
    config=APP_CONFIG
)

def get_pump_estimation(input_data: dict) -> dict:
    """Прослойка между API и расчетами для насосов"""
    try:
        # 1. прогоняем через пайплайн (очистка + энричинг + FE)
        processed_features = pump_service.process_request(input_data)

        # 2. ТУТ БУДЕТ ВЫЗОВ МОДЕЛИ
        mock_weight = 100.0
        if 'useful_kw_log' in processed_features:
            mock_weight = processed_features['useful_kw_log'] * 50

        return {
            "weight": round(float(mock_weight), 2),
            "features": processed_features  # возвращаем фичи для отладки
        }
    except Exception as e:
        logger.error(f"Ошибка в сервисе оценки насоса: {e}")
        # прокидываем ошибку наверх, чтобы FastAPI мог вернуть понятный 400 статус
        raise ValueError(f"Ошибка обработки данных: {str(e)}")