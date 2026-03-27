import pandas as pd
import numpy as np
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from configs.config import CONFIG

# настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class PumpETLPipeline:
    def __init__(self, input_file_path: str | Path, output_folder_path: str | Path, config: dict,
                 weight_file_path: str | Path | None = None):
        # сохраняем пути к файлам
        self.input_file = Path(input_file_path)
        # если путь ко второму файлу передан — используем его, если нет — берем основной файл
        self.weight_file = Path(weight_file_path) if weight_file_path else self.input_file
        self.output_folder = Path(output_folder_path)
        self.config = config

        self.cols_to_convert = self.config['cols_to_convert']
        self.rename_map = self.config['rename_map']

    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Безопасное чтение данных с кэшированием в CSV для скорости."""
        logger.info(f"Чтение данных (источник: {self.input_file})...")

        # ищем директорию файла и создаем скрытую папку .cache
        input_dir = self.input_file.parent
        cache_dir = input_dir / '.cache'
        cache_dir.mkdir(parents=True, exist_ok=True)

        # читаем данные характеристик
        df_features = self._read_with_cache(
            excel_path=self.input_file,
            sheet_name=self.config['sheets']['features'],
            cache_dir=cache_dir,
            suffix="features"
        )

        # читаем данные весов
        df_weight = self._read_with_cache(
            excel_path=self.weight_file,
            sheet_name=self.config['sheets'].get('weight'),
            cache_dir=cache_dir,
            suffix="weight"
        )

        logger.info("Данные успешно загружены.\n")
        return df_features, df_weight

    def _read_with_cache(self, excel_path: Path, sheet_name: str | int | None, cache_dir: Path,
                         suffix: str) -> pd.DataFrame:
        """Универсальная функция чтения Excel с кэшированием (адаптированная версия)"""

        # формируем путь к файлу на основе суффикса (features или weight)
        cache_file = cache_dir / f'{excel_path.stem}_{suffix}.csv'

        try:
            # проверяем наличие xlsx файла
            if not excel_path.exists():
                raise FileNotFoundError()

            # флаг для обновления кэша
            need_update = True

            # если файл кэша есть, проверяем изменения
            if cache_file.exists():
                # получаем время последнего изменения
                excel_mtime = excel_path.stat().st_mtime
                cache_mtime = cache_file.stat().st_mtime

                # если кэш новее файла, то изменения не производятся
                if cache_mtime > excel_mtime:
                    need_update = False

            if need_update:
                # если второй файл эксель без листа (один лист в целом), используем индекс 0
                target_sheet = sheet_name if sheet_name else 0
                logger.info(f"Читаем Excel файл {excel_path.name} (лист: {target_sheet})...")

                # читаем лист с xlsx
                df_excel = pd.read_excel(excel_path, sheet_name=target_sheet)

                logger.info(f"Создаем/обновляем CSV-кэш: {cache_file.name}")

                # сохраняем результат в csv
                df_excel.to_csv(cache_file, index=False, encoding='utf-8-sig')
                df = df_excel
            else:
                logger.info(f"Найден свежий кэш для {suffix}, выполняется быстрое чтение...")
                # читаем данные из csv
                df = pd.read_csv(cache_file, encoding='utf-8-sig')

            return df

        # обработка ошибок
        except FileNotFoundError:
            logger.error(f"\nОШИБКА: Файл '{excel_path}' не найден!")
            logger.info("Проверьте, правильно ли указан путь и лежит ли файл в нужной папке.")
            raise

        except ValueError as e:
            logger.error(f"\nОШИБКА СТРУКТУРЫ ФАЙЛА: Не найден нужный лист в Excel.")
            logger.info(f"Убедитесь, что в '{excel_path.name}' точно есть нужная вкладка.")
            logger.info(f"Технические детали: {e}")
            raise

        except Exception as e:
            logger.error(f"\nНЕИЗВЕСТНАЯ ОШИБКА при извлечении данных из {excel_path.name}: {e}")
            raise

    def _vectorized_numeric_clean(self, series: pd.Series) -> pd.Series:
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

    def _filter_critical_data(self, df: pd.DataFrame, critical_cols: list) -> pd.DataFrame:
        """
        Удаление строк, в которых отсутствуют критически важные параметры (подача и напор).
        """
        before_drop = len(df)

        # how='any' удалит строку, если отсутствует ХОТЯ БЫ ОДИН из параметров
        df = df.dropna(subset=critical_cols, how='any').reset_index(drop=True)

        dropped = before_drop - len(df)
        if dropped > 0:
            logger.info(f"Валидация: удалено {dropped} строк из-за отсутствия подачи или напора.")

        return df

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
        cols_to_clean = self.cols_to_convert

        # очистка данных через регулярные выражения
        for col in cols_to_clean:
            df_merge[col] = self._vectorized_numeric_clean(df_merge[col])
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

        # удаляем насосы без flow_rate и fluid_head
        critical_pumps_cols = [raw['flow'], raw['head']]
        df_merge = self._filter_critical_data(df_merge, critical_pumps_cols)

        if df_merge.empty:
            logger.error(f'ОШИБКА: После очистки данных датасет оказался пустым.')
            raise ValueError('Пустой датасет после трансформации.')

        # переименовываем колонки
        df_merge = df_merge.rename(columns=self.rename_map)

        logger.info('Трансформация данных завершена.\n')
        return df_merge

    def _add_features(self, df_merge: pd.DataFrame, is_inference: bool = False) -> pd.DataFrame:
        """Генерация физических признаков"""
        c_eff = self.config['col_names']['pump_eff']
        c_head = self.config['col_names']['head']
        c_flow = self.config['col_names']['flow']
        c_weight = self.config['col_names']['weight_kg']
        c_speed = self.config['col_names']['rpm']

        # новые параметры
        c_useful = self.config['feat_eng']['useful_kw']
        c_diameter = self.config['feat_eng']['diameter_proxy']
        c_weight_log = self.config['feat_eng']['weight_log']

        # Feature Engineering

        # вспомогательные переменные для удобства
        s_head = df_merge[c_head].astype(float)
        s_speed = df_merge[c_speed].astype(float)

        logger.info("Расчет инженерных признаков...")

        # вводим ПОЛЕЗНУЮ МОЩНОСТЬ (киловатт)
        df_merge[c_useful] = df_merge[c_head] * df_merge[c_flow]

        # Геометрический фактор
        # Косвенная оценка диаметра рабочего колеса
        df_merge[c_diameter] = np.sqrt(s_head) / (s_speed + 1e-6)

        # проверяем безопасность логарифмирования полезной мощности
        invalid_power = df_merge[c_useful] < 0
        if invalid_power.sum() > 0:
            logger.warning(f"Найдено {invalid_power.sum()} строк с отрицательной мощностью. Удаляем.")
            df_merge = df_merge[~invalid_power]

        cols_to_exclude = [c_weight_log, c_diameter]
        # логарифмирование новых признаков
        new_physics_cols = [col for col in self.config['feat_eng'] if col not in cols_to_exclude]
        for col in new_physics_cols:
            df_merge[f'{col}_log'] = np.log1p(df_merge[col].replace([np.inf, -np.inf], np.nan).fillna(0))
            df_merge = df_merge.drop(columns=[col])

        # ---БЕЗОПАСНАЯ ОБРАБОТКА ЦЕЛЕВОЙ ПЕРЕМЕННОЙ---
        if c_weight in df_merge.columns:
            if not is_inference:
                # РЕЖИМ ОБУЧЕНИЯ
                invalid_weight = (df_merge[c_weight] <= 0) | df_merge[c_weight].isna()
                if invalid_weight.sum() > 0:
                    logger.warning(f"Удаляем {invalid_weight.sum()} строк без целевой переменной (веса).")
                    df_merge = df_merge[~invalid_weight]

                df_merge[c_weight_log] = np.log(df_merge[c_weight])

            else:
                # РЕЖИМ ИНФЕРЕНСА
                # если вес <= 0 (ошибка ввода пользователя), превращаем его в NaN, чтобы логарифм не упал
                invalid_input = df_merge[c_weight] <= 0
                if invalid_input.sum() > 0:
                    df_merge.loc[invalid_input, c_weight] = np.nan

                df_merge[c_weight_log] = np.log(df_merge[c_weight].astype(float))

            # удаляем исходные колонки
            df_merge = df_merge.drop(columns=[c_weight, c_eff], errors='ignore')

            # ставим weight_log в конец таблицы
            cols = df_merge.columns.tolist()
            if c_weight_log in cols:
                cols.remove(c_weight_log)
                cols.append(c_weight_log)
                df_merge = df_merge[cols]

        else:
            logger.info("Колонка веса отсутствует в датасете. Пропускаем.")

        logger.info('Рассчет инженерных признаков завершен.')
        return df_merge

    def load(self, df: pd.DataFrame) -> None:
        """Сохранение готовых данных и артефактов"""
        logger.info(f'Сохранение файлов в директорию: {self.output_folder}...')

        # создаем директорию если ее нет
        self.output_folder.mkdir(parents=True, exist_ok=True)

        # формируем пути и сохраняем датасеты
        df.to_csv(self.output_folder / 'dataset_ml.csv', index=False)

        logger.info('Сохранение файлов завершено.')

    def load_to_db(self, df: pd.DataFrame) -> None:
        """Выгрузка готовых датасетов в PostgreSQL (Neon)"""
        logger.info('Подключение к базе данных Neon...')

        table_prefix = self.config['db_params']['table_prefix']
        # находим файл .env
        base_dir = Path(__file__).resolve().parent.parent
        env_path = base_dir / '.env'

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

            logger.info(f"Создаем таблицу {table_prefix}_ml и заливаем данные...")
            df.to_sql(f"{table_prefix}_ml", engine, if_exists='replace', index=False)

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

        # 3. Feature Engineering
        df_transformed = self._add_features(df_transformed)

        # 3. Load (выгрузка данных)
        self.load(df_transformed)

        # 4. Загрузка в облачную БД
        self.load_to_db(df_transformed)

        logger.info('ETL пайплайн завершен.')


if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent.parent

    pipeline = PumpETLPipeline(
        input_file_path=BASE_DIR / 'data' / 'Data.xlsx',
        output_folder_path=BASE_DIR / 'datasets',
        config=CONFIG
    )

    pipeline.run()