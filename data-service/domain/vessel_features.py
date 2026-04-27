# domain/vessel_features.py
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class VesselFeatureEngineer:
    """Доменная логика для сосудов (vessels)"""

    def __init__(self, config: dict):
        self.config = config

    def filter_critical_data(self, df: pd.DataFrame, critical_cols: list) -> pd.DataFrame:
        """Удаление строк без диаметра или высоты"""
        return df.dropna(subset=critical_cols, how='any').reset_index(drop=True)

    def add_vessel_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Генерация признаков сосуда"""
        c_diam = self.config['col_names']['diameter']
        c_dist = self.config['col_names']['ss_dist']
        c_pres = self.config['col_names']['pressure']
        c_weight = self.config['col_names']['weight_kg']
        c_weight_log = self.config['feat_eng']['weight_log']

        logger.info("Расчет характеристик сосуда (физика)...")

        # 1. Прокси площади поверхности (самая сильная базовая фича для веса)
        df[self.config['feat_eng']['surface_area_proxy']] = df[c_diam] * df[c_dist]

        # 2. Прокси внутреннего объема
        df[self.config['feat_eng']['volume_proxy']] = (df[c_diam] ** 2) * df[c_dist]

        # 3. Соотношение сторон (гибкость/парусность). Безопасно, так как мы отфильтровали D=0
        df[self.config['feat_eng']['aspect_ratio']] = np.where(
            df[c_diam] > 0,
            df[c_dist] / df[c_diam],
            np.nan
        )

        # # 3. логарифмирование параметров
        # log_cols = [i for i in self.config['col_names'] if i in df.columns]
        #
        # for col in log_cols:
        #     if col in df.columns:
        #         df = df[df[col] > 0]
        #
        # # делаем сброс индексов один раз для финального чистого датасета
        # df = df.reset_index(drop=True)
        #
        # # вычисляем все логарифмы
        # for col in log_cols:
        #     if col in df.columns:
        #         df[f"{col}_log"] = np.log(df[col].astype(float))
        #         if col != 'weight_kg':
        #             df = df.drop(columns=[col], errors='ignore')

        df['weight_kg'] = df.pop('weight_kg')

        return df