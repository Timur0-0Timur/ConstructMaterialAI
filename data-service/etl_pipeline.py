import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from config import CONFIG

# настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class PumpETLPipeline:
    def __init__(self, input_file_path: str, output_folder_path: str, config: dict):
        # сохраняем пути к файлам
        self.input_file = Path(input_file_path)
        self.output_folder = Path(output_folder_path)
        self.config = config

        # инициализация ML-моделей
        self.iso_forest = IsolationForest(
            contamination=self.config['contamination'],
            random_state=self.config['ml_params']['random_state']
        )
        self.scaler = StandardScaler()

        self.cols_to_convert = self.config['cols_to_convert']
        self.rename_map = self.config['rename_map']
        self.anomaly_check_cols = self.config['anomaly_check_cols']
        self.features_to_scale = self.config['features_to_scale']

    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Безопасное чтение данных с кэшированием в CSV для скорости."""
        logger.info(f"Чтение данных (источник: {self.input_file})...")

        # ищем директорию файла и создаем скрытую папку .cache
        input_dir = self.input_file.parent
        cache_dir = input_dir / '.cache'
        cache_dir.mkdir(parents=True, exist_ok=True)

        # задаем пути для кэша
        tag_features = cache_dir / 'tag_features.csv'
        tag_weight = cache_dir / 'tag_weight.csv'

        try:
            # проверяем есть ли исходный файл
            if not self.input_file.exists():
                raise FileNotFoundError()

            # флаг для обновления кэша
            need_update = True

            # если оба файла кэша существуют
            if tag_features.exists() and tag_weight.exists():
                # получаем время последнего изменения файла
                excel_mtime = self.input_file.stat().st_mtime
                cache_features_mtime = tag_features.stat().st_mtime
                cache_weight_mtime = tag_weight.stat().st_mtime

                # если файлы кэша новее чем xlsx, то не надо менять
                if cache_features_mtime > excel_mtime and cache_weight_mtime > excel_mtime:
                    need_update = False

            if need_update:
                logger.info("Читаем Excel файл (первый запуск или файл был обновлен)...")

                # читаем xlsx файл
                df_features_excel = pd.read_excel(self.input_file, sheet_name=self.config['sheets']['features'])
                df_weight_excel = pd.read_excel(self.input_file, sheet_name=self.config['sheets']['weight'])

                logger.info(f"Создаем/обновляем CSV-кэш в скрытой папке '{cache_dir}'...")

                # сохраняем в csv файлы
                df_features_excel.to_csv(tag_features, index=False, encoding='utf-8-sig')
                df_weight_excel.to_csv(tag_weight, index=False, encoding='utf-8-sig')

            else:
                logger.info("Найден свежий кэш, выполняется быстрое чтение...")

            # читаем данные из файлов csv
            df_features = pd.read_csv(tag_features, encoding='utf-8-sig')
            df_weight = pd.read_csv(tag_weight, encoding='utf-8-sig')

            logger.info("Данные успешно загружены.\n")
            return df_features, df_weight

        # обработка ошибок
        except FileNotFoundError:
            logger.error(f"\nОШИБКА: Файл '{self.input_file}' не найден!")
            logger.info("Проверьте, правильно ли указан путь и лежит ли файл в нужной папке.")
            raise

        except ValueError as e:
            logger.error(f"\nОШИБКА СТРУКТУРЫ ФАЙЛА: Не найден нужный лист в Excel.")
            logger.info(f"Убедитесь, что в '{self.input_file}' точно есть вкладки 'TAG_WEIGHT' и 'TAG_FEATURES'.")
            logger.info(f"Технические детали: {e}")
            raise

        except Exception as e:
            logger.error(f"\nНЕИЗВЕСТНАЯ ОШИБКА при извлечении данных: {e}")
            raise

    @staticmethod
    def vectorized_numeric_clean(series: pd.Series) -> pd.Series:
        """Векторизованная очистка всей колонки данных."""
        # принудительно переводим в строку
        s = series.astype(str)

        # убираем все виды пробелов
        s = s.str.replace(r'\s+', '', regex=True)

        # удаляем точки, которые служат разделителями тысяч (напр. 1.250,50 -> 1250,50)
        s = s.str.replace(r'\.(?=.*,)', '', regex=True)

        # удаляем лишние точки если их несколько (напр. 1.234.567 -> 1234567)
        s = s.str.replace(r'\.(?=.*\.)', '', regex=True)

        # заменяем запятую на точку
        s = s.str.replace(',', '.', regex=False)

        # извлекаем только числовую часть
        s = s.str.extract(r'(-?\d+(?:\.\d+)?)', expand=False)

        return s

    def transform_data(self, df_features: pd.DataFrame, df_weight: pd.DataFrame) -> pd.DataFrame:
        """Очистка, объединение и трансформация данных"""

        raw = self.config['raw_names']

        # проверяем наличие ключевых колонок
        if raw['tag'] not in df_features.columns or raw['tag_weight'] not in df_weight.columns:
            raise KeyError(f"Не найдены колонки для объединения таблиц!")

        logger.info('Начало трансформации данных...')

        # объединяем таблицы - Inner Join
        df_merge = pd.merge(
            df_features,
            df_weight,
            left_on=self.config['raw_names']['tag'],
            right_on=self.config['raw_names']['tag_weight'],
            how='inner'
        ).drop('Tag No', axis=1, errors='ignore')

        # собираем колонки которые должны быть чистовыми
        cols_to_clean = self.cols_to_convert + [self.config['raw_names']['weight']]

        # очистка данных через регулярные выражения
        for col in cols_to_clean:
            df_merge[col] = self.vectorized_numeric_clean(df_merge[col])
            # считаем количество NaN до конвертации
            nan_before = df_merge[col].isna().sum()
            # конвертируем
            df_merge[col] = pd.to_numeric(df_merge[col], errors='coerce')
            # считаем количество NaN после конвертации
            nan_after = df_merge[col].isna().sum()
            new_nans = nan_after - nan_before

            if new_nans > len(df_merge[col]) * 0.05:
                logger.warning(f'ВНИМАНИЕ: В колонке "{col}" не удалось распознать {new_nans} значений. '
                               f'Возможно, изменился формат чисел в источнике.')

        # удаляем мусорные строки
        before_drop = len(df_merge)
        df_merge = df_merge.dropna(subset=cols_to_clean).reset_index(drop=True)
        dropped_rows = before_drop - len(df_merge)

        if dropped_rows > 0:
            logger.info(f'Удалено строк с размерностями или пропусками: {dropped_rows}')

        if df_merge.empty:
            logger.error(f'ОШИБКА: После очистки данных датасет оказался пустым.')
            raise ValueError('Пустой датасет после трансформации. Проверьте формат исходных данных.')

        # переименовываем колонки
        df_merge = df_merge.rename(columns=self.rename_map)

        c_head = self.config['col_names']['head']
        c_flow = self.config['col_names']['flow']
        c_weight = self.config['col_names']['weight']
        c_useful = self.config['col_names']['useful_kw']
        c_weight_log = self.config['col_names']['weight_log']

        # Feature Engineering
        # вводим ПОЛЕЗНУЮ МОЩНОСТЬ (киловатт)
        df_merge[c_useful] = df_merge[c_head] * df_merge[c_flow]

        # проверяем безопасность логарифмирования полезной мощности
        invalid_power = df_merge[c_useful] < 0
        if invalid_power.sum() > 0:
            logger.warning(f"Найдено {invalid_power.sum()} строк с отрицательной мощностью. Удаляем.")
            df_merge = df_merge[~invalid_power]

        # логарифмируем useful_kw
        df_merge[c_useful] = np.log1p(df_merge[c_useful])

        # проверяем безопасность логарифмирования веса
        invalid_weight = df_merge[c_weight] <= 0
        if invalid_weight.sum() > 0:
            logger.warning(f"Найдено {invalid_weight.sum()} строк с некорректным весом (<= 0). Удаляем.")
            df_merge = df_merge[~invalid_weight]

        # логарифмируем целевую weight_kg
        df_merge[c_weight_log] = np.log(df_merge[c_weight])

        # удаляем старые колонки
        df_merge = df_merge.drop(columns=[c_head, c_flow, c_weight])

        # делаем weight_log последней колонкой
        cols = df_merge.columns.tolist()
        cols.remove(c_weight_log)
        cols.append(c_weight_log)
        df_merge = df_merge[cols]

        logger.info('Трансформация данных завершена.\n')
        return df_merge

    def split_data(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Разделение данных на обучающую и тестовую выборки"""
        logger.info('Разделение данных на Train и Test (80/20)...')

        test_size = self.config['ml_params']['test_size']
        random_state = self.config['ml_params']['random_state']

        df_train, df_test = train_test_split(df, test_size=test_size, random_state=random_state)
        return df_train.copy(), df_test.copy()

    def isolate_anomalies(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Разметка аномалий: создаем новый признак вместо удаления строк"""
        logger.info('Поиск и разметка аномалий (IsolationForest)...')

        # обучаем лес на train
        self.iso_forest.fit(df_train[self.anomaly_check_cols])

        # получаем метки
        train_labels = self.iso_forest.predict(df_train[self.anomaly_check_cols])
        test_labels = self.iso_forest.predict(df_test[self.anomaly_check_cols])

        # создаем колонку is_anomaly (1 - аномалия, 0 - нормальный)
        df_train['is_anomaly'] = np.where(train_labels == -1, 1, 0)
        df_test['is_anomaly'] = np.where(test_labels == -1, 1, 0)

        # вывод статистики
        logger.info(f'Всего насосов в Train: {len(df_train)}, из них аномальных: {df_train["is_anomaly"].sum()}')
        logger.info(f'Всего насосов в Test: {len(df_test)}, из них аномальных: {df_test["is_anomaly"].sum()}\n')

        return df_train, df_test

    def scale_features(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Масштабирование числовых признаков (StandardScaler)"""
        logger.info('Масштабирование признаков...')

        # находим нормальные насосы
        normal_train_mask = df_train['is_anomaly'] == 0

        # обучаем скейлер по нормальным насосам
        self.scaler.fit(df_train.loc[normal_train_mask, self.features_to_scale])

        # масштабируем данные в train и test
        df_train[self.features_to_scale] = self.scaler.transform(df_train[self.features_to_scale])
        df_test[self.features_to_scale] = self.scaler.transform(df_test[self.features_to_scale])

        logger.info('Масштабирование завершено.\n')
        return df_train, df_test

    def load(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> None:
        """Сохранение готовых данных и артефактов"""
        logger.info(f'Сохранение файлов в директорию: {self.output_folder}...')

        # создаем директорию если ее нет
        self.output_folder.mkdir(parents=True, exist_ok=True)

        # формируем пути и сохраняем датасеты
        df_train.to_csv(self.output_folder / 'dataset_ml_train.csv', index=False)
        df_test.to_csv(self.output_folder / 'dataset_ml_test.csv', index=False)

        # сохраняем обученные модели
        joblib.dump(self.scaler, self.output_folder / 'pump_scaler.joblib')
        joblib.dump(self.iso_forest, self.output_folder / 'pump_iso_forest.joblib')

        logger.info('Сохранение файлов завершено.')

    def load_to_db(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> None:
        """Выгрузка готовых датасетов в PostgreSQL (Neon)"""
        logger.info('Подключение к базе данных Neon...')

        table_prefix = self.config['db_params']['table_prefix']
        # находим файл .env
        env_path = Path('.env')

        # загружаем переменные из .env
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
        else:
            logger.warning(f"Файл {env_path} не найден! Убедитесь, что он существует.")

        # достаем данные из .env
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASSWORD')

        # проверяем что все сошлось
        if not all([db_host, db_port, db_name, db_user, db_pass]):
            logger.error("ОШИБКА: Не найдены данные для БД.")
            return

        # формируем строку подключения
        engine_url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?sslmode=require"

        try:
            engine = create_engine(engine_url)

            logger.info(f"Создаем таблицу {table_prefix}_train и заливаем данные...")
            df_train.to_sql(f"{table_prefix}_train", engine, if_exists='replace', index=False)

            logger.info(f"Создаем таблицу {table_prefix}_test и заливаем данные...")
            df_test.to_sql(f"{table_prefix}_test", engine, if_exists='replace', index=False)

            logger.info('Данные успешно сохранены в облачную БД Neon.')

        except Exception as e:
            logger.error(f"ОШИБКА при записи в БД: {e}")

    def run(self):
        """Главный метод, запуск конвейера по очереди"""
        logger.info('\n--- ЗАПУСК ETL ПАЙПЛАЙНА ---')

        # 1. Extract (извлечение)
        df_features, df_weight = self.extract()

        # 2. Transform (очистка и трансформация данных)
        df_transformed = self.transform_data(df_features, df_weight)

        # 3. Разделение на Train и Test
        df_train, df_test = self.split_data(df_transformed)

        # 4. Поиск аномалий (IsolationForest) - создание колонки is_anomaly
        df_train, df_test = self.isolate_anomalies(df_train, df_test)

        # 5. Масштабирование данных (StandardScaler)
        df_train, df_test = self.scale_features(df_train, df_test)

        # 6. Load (выгрузка данных)
        self.load(df_train, df_test)

        # 7. Загрузка в облачную БД
        self.load_to_db(df_train, df_test)

        logger.info('ETL пайплайн завершен.')

if __name__ == '__main__':
    pipeline = PumpETLPipeline(
        input_file_path='data/Data.xlsx',
        output_folder_path='ml_artifacts',
        config=CONFIG
    )

    pipeline.run()