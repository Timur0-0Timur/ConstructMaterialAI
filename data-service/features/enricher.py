import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class PumpEnricher:
    """
    Класс для восстановления пропущенных данных на основе
    поиска ближайших аналогов в эталонном датасете.
    """

    def __init__(self, search_features: list, target_features: list):
        self.search_features = search_features
        self.target_features = target_features

    def enrich(self, df_real: pd.DataFrame, df_base: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Запуск обогащения данных. Поиск по признакам: {self.search_features}")

        df_result = df_real.copy()

        # Переводим базовый датасет в numpy для скорости
        # Оставляем только нужные колонки и убираем строки, где все поисковые фичи пустые
        base_clean = df_base.dropna(subset=self.search_features)
        base_values = base_clean[self.search_features].values
        base_targets = base_clean[self.target_features].values

        enriched_count = 0

        for i, row in df_result.iterrows():
            # 1. Проверка на наличие данных для поиска
            real_search_vals = row[self.search_features].values.astype(float)
            mask = ~np.isnan(real_search_vals)

            if not np.any(mask):
                continue

            # 2. Векторный расчет дистанций (NumPy)
            # Считаем относительное отклонение: (Real - Base) / Base
            # Используем только те колонки, которые есть в реальной строке (mask)
            diff = (real_search_vals[mask] - base_values[:, mask]) / (base_values[:, mask] + 1e-9)
            distances = np.sum(np.abs(diff), axis=1)

            # 3. Находим лучшие совпадения
            min_dist = np.min(distances)
            best_indices = np.where(np.abs(distances - min_dist) < 1e-6)[0]

            # 4. Усреднение кандидатов и расчет коррекции (sumDevFrac)
            # Берем среднее по найденным строкам в базе
            avg_base_search = np.mean(base_values[best_indices], axis=0)
            avg_base_targets = np.mean(base_targets[best_indices], axis=0)

            # Среднее относительное отклонение по известным признакам
            # Это и есть наш коэффициент "насколько реальный насос больше/меньше эталона"
            valid_diffs = (real_search_vals[mask] - avg_base_search[mask]) / (avg_base_search[mask] + 1e-9)
            sum_dev_frac = np.mean(valid_diffs)

            # 5. Заполнение пропусков
            for j, target_f in enumerate(self.target_features):
                # Заполняем только если в оригинале NaN
                if pd.isna(row[target_f]):
                    val_from_base = avg_base_targets[j]

                    if not np.isnan(val_from_base):
                        # Применяем коррекцию
                        calculated_val = val_from_base * (1.0 + sum_dev_frac)
                        df_result.at[i, target_f] = calculated_val
                        enriched_count += 1

        logger.info(f"Обогащение завершено. Восстановлено значений: {enriched_count}")
        return df_result