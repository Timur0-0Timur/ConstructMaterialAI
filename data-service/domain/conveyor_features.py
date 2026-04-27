import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ConveyorFeatureEngineer:
    """Доменная логика для ленточных конвейеров"""

    def __init__(self, config: dict):
        self.config = config

    def filter_critical_data(self, df: pd.DataFrame, critical_cols: list) -> pd.DataFrame:
        """Удаление строк без базовой геометрии"""
        return df.dropna(subset=critical_cols, how='any').reset_index(drop=True)

    def add_conveyor_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Генерация инженерных признаков для конвейера"""
        c_len = self.config['col_names']['length']
        c_width = self.config['col_names']['width']

        logger.info("Расчет характеристик конвейера...")

        # 1. Расчет условной площади ленты
        df['belt_area'] = df[c_len] * df[c_width]

        # # 2. Собираем реальные колонки датафрейма для логарифмирования
        # log_cols = [col for col in df.columns if col != c_tag]
        #
        # # 3. Векторная фильтрация
        # df = df[(df[log_cols] > 0).all(axis=1)].reset_index(drop=True)
        #
        # # 4. Векторное логарифмирование
        # # Генерируем имена для новых колонок
        # log_col_names = [f"{col}_log" for col in log_cols]
        #
        # # Считаем логарифм сразу для всей матрицы данных
        # df[log_col_names] = np.log(df[log_cols].astype(float))
        #
        # # 5. Дропаем все старые колонки разом
        # df = df.drop(columns=log_cols)
        #
        df['weight_kg'] = df.pop('weight_kg')

        logger.info("Рассчет инженерных признаков завершен.")

        return df