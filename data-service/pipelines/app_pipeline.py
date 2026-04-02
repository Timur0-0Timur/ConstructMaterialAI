import pandas as pd
import datetime
import logging
from pathlib import Path
from sqlalchemy import create_engine
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from pipelines.etl_pipeline import PumpETLPipeline
from features.enricher import PumpEnricher

from configs.app_config import APP_CONFIG

logger = logging.getLogger(__name__)


class PumpAppPipeline(PumpETLPipeline):
    def __init__(self, output_folder_path, config):
        if 'cols_to_convert' not in config:
            config['cols_to_convert'] = []
        if 'rename_map' not in config:
            config['rename_map'] = {}

        # вызываем родительский инит
        super().__init__(input_file_path="", output_folder_path=output_folder_path, config=config)

    def transform_app_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформация данных пользователя"""
        logger.info('Запуск трансформации из API...')

        # доп проверка на заполнение ключевых колонок
        critical_cols = self.config['critical_cols']
        df = df.dropna(subset=critical_cols, how='any').copy()

        # векторизованная очистка
        for col in self.config['cols_to_clean']:
            if col in df.columns:
                df[col] = self._vectorized_numeric_clean(df[col])
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # энричинг
        try:
            # читаем эталонный датасет
            base_path = self.output_folder / 'dataset_ml.csv'
            if base_path.exists():
                df_base = pd.read_csv(base_path)

                # создаем обогатитель
                enricher = PumpEnricher(
                    search_features=self.config['search_features'],
                    target_features=self.config['target_features']
                )

                # запуск процесса
                df = enricher.enrich(df, df_base)
            else:
                logger.warning("Эталонный датасет не найден, пропуск шага обогащения.")
        except Exception as e:
            logger.error(f"Ошибка при обогащении данных: {e}")

        df = self._add_features(df, is_inference=True)
        return df

    def run_api_inference(self, input_dict: dict) -> dict:
        """
        Главный метод для API
        Принимает словарь, возвращает обработанную строку
        """
        logger.info('\n---ОБРАБОТКА ЗАПРОСА ИЗ API---')

        # конвертация входного словаря в DataFrame
        df_raw = pd.DataFrame([input_dict])

        # трансформация
        df_processed = self.transform_app_data(df_raw)

        if df_processed.empty:
            raise ValueError("Данные не прошли валидацию или очистку.")

        # выделяем результат в виде словаря
        result_row = df_processed.to_dict(orient='records')[0]

        return result_row