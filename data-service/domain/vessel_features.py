import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class VesselFeatureEngineer:
    """Доменная логика для сосудов под давлением"""

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

        # 1. прокси объема (D^2 * L)
        df[self.config['feat_eng']['volume_proxy']] = (df[c_diam] ** 2) * df[c_dist]

        # 2. фактор давления (влияет на толщину стенки)
        df[self.config['feat_eng']['pressure_factor']] = df[c_pres] * df[c_diam]

        # 3. логарифмирование веса
        if c_weight in df.columns:
            df[c_weight_log] = np.log(df[c_weight].astype(float).replace(0, np.nan))
            df = df.drop(columns=[c_weight])

        return df