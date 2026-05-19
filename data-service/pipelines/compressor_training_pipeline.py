# pipelines/compressor_heat_training_pipeline.py
import pandas as pd
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from configs.config_loader import config
from pipelines.base_etl import BaseETLPipeline
#from domain.vessel_features import VesselFeatureEngineer
from utils.cleaners import vectorized_numeric_clean

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompressorTrainingPipeline(BaseETLPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
#        self.feature_engineer = VesselFeatureEngineer(self.config)

    def transform(self, df_features: pd.DataFrame, df_weight: None) -> pd.DataFrame:
        raw = self.config['raw_names']
        cols = self.config['col_names']
        logger.info('Трансформация данных центробежных компрессоров...')

        # 1. считываем страницу с файла
        df_merge = df_features.copy()

        # 2. переименование (сразу приводим к нормальным именам)
        df_merge = df_merge.rename(columns=self.get_rename_map())

        # очистка от дубликатов
        duplicated_cols = df_merge.columns[df_merge.columns.duplicated()].unique().tolist()
        if duplicated_cols:
            logger.warning(
                f"ВНИМАНИЕ! Найдены дублирующиеся колонки: {duplicated_cols}. Берем только первые вхождения.")
            df_merge = df_merge.loc[:, ~df_merge.columns.duplicated()]

        # 3. очистка чисел (теперь все вычисления будут безопасными)
        for col in self.config['cols_to_convert']:
            df_merge[col] = vectorized_numeric_clean(df_merge[col])
            df_merge[col] = pd.to_numeric(df_merge[col], errors='coerce')

        df_merge.dropna(how='all', inplace=True)

        logger.info("ФАКТИЧЕСКИЕ КОЛОНКИ: %s", df_merge.columns.tolist())

        return df_merge

    def load(self, df: pd.DataFrame):
        super().load(df, filename='compressor_dataset_ml.csv')

if __name__ == '__main__':
    compressor_ml_config = config['equipment']['compressor_ml']
    pipeline = CompressorTrainingPipeline(
        input_file_path=BASE_DIR / 'data' / 'Centrifugal compressor-horizontal.xlsx',
        output_folder_path=BASE_DIR / 'datasets',
        config=compressor_ml_config
    )
    pipeline.run()