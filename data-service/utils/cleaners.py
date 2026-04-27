# utils/cleaners.py
import pandas as pd

import pandas as pd
import numpy as np


def vectorized_numeric_clean(series: pd.Series) -> pd.Series:
    """Векторизованная очистка всей колонки данных (с поддержкой экспоненциальной записи)"""
    s = series.astype(str)
    s = s.str.replace(r'\s+', '', regex=True)
    s = s.str.replace(r'\.(?=.*,)', '', regex=True)
    s = s.str.replace(r'\.(?=.*\.)', '', regex=True)
    s = s.str.replace(',', '.', regex=False)

    # извлекаем число (включая отрицательные) + опциональную дробную часть + опциональную e-экспоненту
    s = s.str.extract(r'(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)', expand=False)

    return s

def clean_vessel_data(df: pd.DataFrame, config) -> pd.DataFrame:
    # плотность углеродистой стали (кг/м^3)
    STEEL_DENSITY = 7850

    # 1. максимальный теоретический вес
    df['Max_Possible_Weight'] = (
            np.pi
            * (df[config['diameter']] / 2) ** 2
            * df[config['ss_dist']]
            * STEEL_DENSITY
    )

    # накидываем 20% запаса на толстые эллиптические днища, внутренние устройства и тяжелые опоры
    df['Max_Possible_Weight'] = df['Max_Possible_Weight'] * 1.2

    # 2. минимальный адекватный вес (эвристика "бумажных стенок")
    df['Min_Possible_Weight'] = df['Max_Possible_Weight'] * 0.03

    # 3. применяем физические фильтры
    df_cleaned = df[
        (df[config['weight_kg']] <= df['Max_Possible_Weight']) &
        (df[config['weight_kg']] >= df['Min_Possible_Weight'])
        ].copy()

    # убираем технические колонки, чтобы не мусорить на выходе
    df_cleaned = df_cleaned.drop(columns=['Max_Possible_Weight', 'Min_Possible_Weight'])

    return df_cleaned