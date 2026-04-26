# pipelines/api_pipeline.py
import pandas as pd
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from utils.cleaners import vectorized_numeric_clean
from domain.pump_features import PumpFeatureEngineer
from utils.enricher import PumpEnricher
from domain.vessel_features import VesselFeatureEngineer

logger = logging.getLogger(__name__)


class EquipmentAPIService:
    """Легковесный сервис для обработки одиночных запросов из API"""

    def __init__(self, output_folder_path: Path, config: dict):
        self.output_folder = output_folder_path
        self.config = config
        # В будущем здесь можно сделать фабрику, которая выдает нужный Engineer
        # в зависимости от типа оборудования (насос/двигатель)
        self.feature_engineer = PumpFeatureEngineer(config)

    def process_request(self, input_dict: dict) -> dict:
        """Главный метод для API"""
        logger.info('\n---ОБРАБОТКА ЗАПРОСА ИЗ API---')

        # 1. из словаря в DataFrame
        df = pd.DataFrame([input_dict])

        # 2. валидация
        critical_cols = self.config['critical_cols']
        df = self.feature_engineer.filter_critical_data(df, critical_cols)
        if df.empty:
            raise ValueError("Данные не прошли валидацию (нет подачи или напора).")

        # 3. очистка
        for col in self.config['cols_to_clean']:
            if col in df.columns:
                df[col] = vectorized_numeric_clean(df[col])
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. обогащение
        try:
            base_path = self.output_folder / 'dataset_ml.csv'
            if base_path.exists():
                df_base = pd.read_csv(base_path)
                enricher = PumpEnricher(
                    search_features=self.config['search_features'],
                    target_features=self.config['target_features']
                )
                df = enricher.enrich(df, df_base)
            else:
                logger.warning("Эталонный датасет не найден, пропуск шага обогащения.")
        except Exception as e:
            logger.error(f"Ошибка при обогащении данных: {e}")

        # 5. физика
        df = self.feature_engineer.add_physics_features(df, is_inference=True)

        # 6. возвращаем чистый словарь
        return df.to_dict(orient='records')[0]

class VesselAPIService:
    """Легковесный сервис для обработки одиночных запросов из API для сосудов"""

    def __init__(self, output_folder_path: Path, config: dict):
        self.output_folder = output_folder_path
        self.config = config
        self.feature_engineer = VesselFeatureEngineer(config)

    def process_request(self, input_dict: dict) -> dict:
        logger.info('\n---ОБРАБОТКА ЗАПРОСА ИЗ API (VESSEL)---')

        # 1. из словаря в DataFrame
        df = pd.DataFrame([input_dict])

        # 2. очистка
        cols_to_clean = self.config.get('cols_to_clean', ['diameter', 'ss_distance', 'pressure', 'sk_height', 'leg_height', 'temp'])
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = vectorized_numeric_clean(df[col])
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 3. валидация
        critical_cols = self.config.get('critical_cols', ['diameter', 'ss_distance'])
        df = self.feature_engineer.filter_critical_data(df, critical_cols)
        if df.empty:
            raise ValueError("Данные не прошли валидацию (нет диаметра или высоты).")

        # 4. физика
        df = self.feature_engineer.add_physics_features(df, is_inference=True)

        return df.to_dict(orient='records')[0]