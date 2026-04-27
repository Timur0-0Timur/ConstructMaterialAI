# pipelines/vessel_training_pipeline.py
import pandas as pd
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from configs.config_loader import config
from pipelines.base_etl import BaseETLPipeline
from domain.vessel_features import VesselFeatureEngineer
from utils.cleaners import vectorized_numeric_clean
from utils.cleaners import clean_vessel_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VesselTrainingPipeline(BaseETLPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.feature_engineer = VesselFeatureEngineer(self.config)

    def transform(self, df_features: pd.DataFrame, df_weight: None) -> pd.DataFrame:
        raw = self.config['raw_names']
        cols = self.config['col_names']
        logger.info('Трансформация данных сосудов...')

        # 1. считываем страницу с файла
        df_merge = df_features.copy()

        # 2. переименование (сразу приводим к нормальным именам)
        df_merge = df_merge.rename(columns=self.get_rename_map())

        # 3. очистка чисел (теперь все вычисления будут безопасными)
        for col in self.config['cols_to_convert']:
            df_merge[col] = vectorized_numeric_clean(df_merge[col])
            df_merge[col] = pd.to_numeric(df_merge[col], errors='coerce')

        # 4. физическая очистка от аномалий генератора
        df_merge = clean_vessel_data(df_merge, cols)

        # 5. валидация на NaN/Null
        critical = [cols['diameter'], cols['ss_dist']]
        df_merge = self.feature_engineer.filter_critical_data(df_merge, critical)

        logger.info("ФАКТИЧЕСКИЕ КОЛОНКИ: %s", df_merge.columns.tolist())

        # 6. физика (feature engineering)
        df_merge = self.feature_engineer.add_vessel_features(df_merge)

        return df_merge

    def load(self, df: pd.DataFrame):
        super().load(df, filename='vessel_dataset_ml_2.csv')

if __name__ == '__main__':
    vessel_ml_config = config['equipment']['vessel_ml']
    pipeline = VesselTrainingPipeline(
        input_file_path=BASE_DIR / 'data' / 'dataset_v_vessel_ml_2.xlsx',
        output_folder_path=BASE_DIR / 'datasets',
        config=vessel_ml_config
    )
    pipeline.run()