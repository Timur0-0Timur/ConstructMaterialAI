from pathlib import Path
from pipelines.app_pipeline import PumpAppPipeline
from configs.app_config import APP_CONFIG

# определяем пути
BASE_DIR = Path(__file__).resolve().parent.parent
DATASETS_DIR = BASE_DIR / 'datasets'

# инициализируем пайплайн
app_pipeline = PumpAppPipeline(
    output_folder_path=DATASETS_DIR,
    config=APP_CONFIG
)


def get_pump_estimation(input_data: dict) -> dict:
    """Прослойка между API и расчетами"""
    # 1. прогоняем через пайплайн (очистка + энричинг + FE)
    processed_features = app_pipeline.run_api_inference(input_data)

    # 2. ТУТ БУДЕТ ВЫЗОВ МОДЕЛИ
    # Пока модели нет, мы просто имитируем её работу.
    # Допустим, модель берет рассчитанный useful_kw_log и накидывает коэффициент
    mock_weight = 100.0
    if 'useful_kw_log' in processed_features:
        mock_weight = processed_features['useful_kw_log'] * 50

    return {
        "weight": round(float(mock_weight), 2),
        "features": processed_features  # возвращаем фичи для отладки
    }