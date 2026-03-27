import pandas as pd
import numpy as np
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from configs.inf_config import INFERENCE_CONFIG
from pipelines.etl_pipeline import PumpETLPipeline
from features.enricher import PumpEnricher

# настройка логгера
logger = logging.getLogger(__name__)

class PumpInferencePipeline(PumpETLPipeline):
    """Пайплайн для очистки реальных данных"""

    def transform_data(self, df_features: pd.DataFrame, df_weight: pd.DataFrame) -> pd.DataFrame:
        """Переопределение метода: сохраняем пропуски и делаем Outer Join"""
        logger.info('Начало трансформации (режим инференса)...')

        raw = self.config['raw_names']

        # Outer Join
        logger.warning('ВНИМАНИЕ: Теги в файлах различаются.')
        df_merge = pd.merge(
            df_features,
            df_weight,
            left_on=raw['tag'],
            right_on=raw['weight_tag'],
            how='outer'
        )

        df_merge = df_merge.dropna(subset=[raw['tag']])

        df_merge[raw['tag']] = df_merge[raw['tag']].fillna(df_merge[raw['weight_tag']])
        df_merge = df_merge.drop(columns=[raw['weight_tag']], errors='ignore')

        columns_to_drop = self.config['weight_col_to_drop']
        df_merge = df_merge.drop(columns=columns_to_drop, errors='ignore')
        # очистка числовых колонок
        cols_to_clean = self.cols_to_convert

        for col in cols_to_clean:
            if col in df_merge.columns:
                df_merge[col] = self._vectorized_numeric_clean(df_merge[col])
                df_merge[col] = pd.to_numeric(df_merge[col], errors='coerce')

        # удаляем насосы без flow_rate и fluid_head
        critical_pumps_cols = [raw['flow'], raw['head']]
        df_merge = self._filter_critical_data(df_merge, critical_pumps_cols)

        if df_merge.empty:
            logger.error(f'ОШИБКА: После очистки данных датасет оказался пустым.')
            raise ValueError('Пустой датасет после трансформации.')

        # переименуем по конфигу
        df_merge = df_merge.rename(columns=self.rename_map)

        logger.info(f'Трансформация завершена. Всего строк: {len(df_merge)}')
        return df_merge

    def load(self, df: pd.DataFrame) -> None:
        """Переопределяем метод сохранения"""
        self.output_folder.mkdir(parents=True, exist_ok=True)
        output_path = self.output_folder / 'dataset_inference.csv'
        df.to_csv(output_path, index=False)
        logger.info(f'Файл для инференса сохранен: {output_path}')


    def run(self):
        """Переопределяем метод запуска"""
        logger.info('\n---ЗАПУСК INFERENCE ПАЙПЛАЙНА---')
        df_features, df_weight = self.extract()
        df_transformed = self.transform_data(df_features, df_weight)

        # обогащение данных
        try:
            # читаем эталонный датасет
            base_path = self.output_folder / 'dataset_ml.csv'
            if base_path.exists():
                df_base = pd.read_csv(base_path)

                # создаем обогатитель
                enricher = PumpEnricher(
                    search_features=['flow_rate', 'fluid_head'],
                    target_features=['rpm', 'spec_gravity', 'power_kw']
                )

                # запуск процесса
                df_transformed = enricher.enrich(df_transformed, df_base)
            else:
                logger.warning("Эталонный датасет не найден, пропуск шага обогащения.")
        except Exception as e:
            logger.error(f"Ошибка при обогащении данных: {e}")

        df_transformed = self._add_features(df_transformed, is_inference=True)
        self.load(df_transformed)
        self.load_to_db(df_transformed)
        logger.info('Inference пайплайн завершен.')

if __name__ == '__main__':
    BASE_DIR=Path(__file__).resolve().parent.parent

    pipeline = PumpInferencePipeline(
        input_file_path=BASE_DIR / 'data' / 'Реальные насосные.xlsx',
        output_folder_path=BASE_DIR / 'datasets',
        config=INFERENCE_CONFIG,
        weight_file_path=BASE_DIR / 'data' / 'Реалные_насосные_альтернативный_лист_с_весом.xlsx'
    )
    pipeline.run()