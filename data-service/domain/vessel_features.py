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

    def add_physics_features(self, df: pd.DataFrame, is_inference: bool = False) -> pd.DataFrame:
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

        df_out = df.copy()

        # Приводим типы
        for col in [c_diameter, c_ss_dist, c_pressure, c_sk_height, c_leg_height]:
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors='coerce').fillna(0)
            else:
                df_out[col] = 0

        # Расчет базовых геометрических признаков (volume_proxy, surface_area_proxy)
        # Если liq_volume не передан, то объем равен объему цилиндра
        if 'liq_volume' not in df_out.columns:
            df_out['liq_volume'] = np.pi * ((df_out[c_diameter] / 2) ** 2) * df_out[c_ss_dist]
        
        # volume_proxy
        df_out['volume_proxy'] = np.pi * ((df_out[c_diameter] / 2) ** 2) * df_out[c_ss_dist]
        df_out['volume_proxy_log'] = np.log1p(df_out['volume_proxy'])

        # surface_area_proxy (боковая поверхность + 2 основания)
        df_out['surface_area_proxy'] = np.pi * df_out[c_diameter] * df_out[c_ss_dist] + 2 * np.pi * ((df_out[c_diameter] / 2) ** 2)
        df_out['surface_area_proxy_log'] = np.log1p(df_out['surface_area_proxy'])
        
        # Инженерия признаков как в обученной модели
        # 1. Обработка давления
        df_out['abs_pressure'] = df_out[c_pressure].abs()
        
        # 2. Логарифмический прокси веса стенки (P * D)
        df_out['p_d_logic'] = np.log1p(df_out['abs_pressure'] * df_out[c_diameter])
        
        # 3. Коэффициент заполнения
        df_out['v_ratio'] = df_out['volume_proxy_log'] - np.log(df_out['liq_volume'] + 0.1)

        # Обработка целевой переменной (только для ML, не для инференса)
        if c_weight in df_out.columns:
            if not is_inference:
                invalid_weight = (df_out[c_weight] <= 0) | df_out[c_weight].isna()
                if invalid_weight.sum() > 0:
                    logger.warning(f"Удаляем {invalid_weight.sum()} строк без целевой переменной (веса).")
                    df_out = df_out[~invalid_weight]
                df_out['weight_kg_log'] = np.log(df_out[c_weight])
            else:
                invalid_input = df_out[c_weight] <= 0
                if invalid_input.sum() > 0:
                    df_out.loc[invalid_input, c_weight] = np.nan
                df_out['weight_kg_log'] = np.log(df_out[c_weight].astype(float))
                
            df_out = df_out.drop(columns=[c_weight], errors='ignore')

        # Оставляем только нужные колонки для модели + служебные
        required_cols = [
            "liq_volume", "diameter", "ss_distance", "pressure", 
            "sk_height", "leg_height", "p_d_logic", "v_ratio",
            "surface_area_proxy_log", "volume_proxy_log"
        ]
        
        # Если есть tag, оставляем
        if 'tag' in df_out.columns:
            required_cols = ['tag'] + required_cols
            
        # Добавляем таргет, если он есть
        if 'weight_kg_log' in df_out.columns:
            required_cols.append('weight_kg_log')
            
        # Убедимся, что все колонки есть, недостающие заполняем нулями
        for col in required_cols:
            if col not in df_out.columns:
                df_out[col] = 0

        # Возвращаем нужные колонки
        result_cols = [col for col in required_cols if col in df_out.columns]
        df_out = df_out[result_cols]

        logger.info('Расчет инженерных признаков для сосуда завершен.')
        return df_out