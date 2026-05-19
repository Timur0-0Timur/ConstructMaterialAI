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
        """Удаление строк, в которых отсутствуют критически важные параметры."""
        before_drop = len(df)
        df = df.dropna(subset=critical_cols, how='any').reset_index(drop=True)
        dropped = before_drop - len(df)

        if dropped > 0:
            logger.info(f"Валидация: удалено {dropped} строк из-за отсутствия критических параметров.")
        return df

    def add_vessel_features(self, df: pd.DataFrame, is_inference: bool = False) -> pd.DataFrame:
        """Генерация физических признаков, необходимых для ML модели."""
        logger.info("Расчет инженерных признаков для сосуда...")

        # Получаем названия колонок из конфига или используем дефолтные
        col_names = self.config.get('col_names', {})
        c_diameter = col_names.get('diameter', 'diameter')
        c_ss_dist = col_names.get('ss_distance', 'ss_distance')
        c_pressure = col_names.get('pressure', 'pressure')
        c_sk_height = col_names.get('sk_height', 'sk_height')
        c_leg_height = col_names.get('leg_height', 'leg_height')
        c_weight = col_names.get('weight_kg', 'weight_kg')
        c_temp = col_names.get('temp', 'temp')

        df_out = df.copy()

        # А. Коррекция давления (MPa -> kPa)
        mask_mpa = (df_out[c_pressure] > 0) & (df_out[c_pressure] < 20)
        df_out.loc[mask_mpa, c_pressure] *= 1000

        # Б. Абсолютное давление и заполнение пропусков
        df_out['p_abs'] = df_out[c_pressure].fillna(0) + 101.3
        df_out.loc[df_out['p_abs'] < 50, 'p_abs'] = 101.3

        # В. Восстановление объема (геометрический расчет)
        if 'liq_volume' not in df_out.columns:
            df_out['liq_volume'] = np.nan

        mask_vol = df_out['liq_volume'].isna() | (df_out['liq_volume'] <= 0)
        df_out.loc[mask_vol, 'liq_volume'] = (np.pi * (df_out[c_diameter] ** 2) / 4) * df_out[c_ss_dist]

        # Г. Новые физические признаки
        # Площадь поверхности (стенки + днища)
        df_out['area_calc'] = (np.pi * df_out[c_diameter] * df_out[c_ss_dist]) + (
                    1.5 * np.pi * (df_out[c_diameter] ** 2) / 4)
        # Прокси толщины стенки (P * D)
        df_out['thick_proxy'] = df_out['p_abs'] * df_out[c_diameter]

        # Д. Опоры (бинарные признаки)
        df_out['has_skirt'] = df_out[c_sk_height].notna().astype(int)
        df_out['has_legs'] = df_out[c_leg_height].notna().astype(int)
        df_out[c_sk_height] = df_out[c_sk_height].fillna(0)
        df_out[c_leg_height] = df_out[c_leg_height].fillna(0)

        # Маппинг температуры
        df_out['des_temp'] = df_out[c_temp].fillna(0)

        # Обработка целевой переменной (только для ML, не для инференса)
        if c_weight in df_out.columns:
            if not is_inference:
                invalid_weight = (df_out[c_weight] <= 0) | df_out[c_weight].isna()
                if invalid_weight.sum() > 0:
                    logger.warning(f"Удаляем {invalid_weight.sum()} строк без целевой переменной (веса).")
                    df_out = df_out[~invalid_weight]
            else:
                invalid_input = df_out[c_weight] <= 0
                if invalid_input.sum() > 0:
                    df_out.loc[invalid_input, c_weight] = np.nan

        required_cols = [
            'liq_volume', 'diameter', 'ss_distance', 'p_abs',
            'area_calc', 'thick_proxy', 'des_temp',
            'sk_height', 'leg_height', 'has_skirt', 'has_legs'
        ]

        if 'tag' in df_out.columns:
            required_cols = ['tag'] + required_cols

        for col in required_cols:
            if col not in df_out.columns:
                df_out[col] = 0

        result_cols = [col for col in required_cols if col in df_out.columns]
        df_out = df_out[result_cols]

        logger.info('Расчет инженерных признаков для сосуда завершен.')
        return df_out