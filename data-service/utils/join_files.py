import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

file1_path = BASE_DIR / 'datasets' / 'vessel_dataset_ml_1.csv'
file2_path = BASE_DIR / 'datasets' / 'vessel_dataset_ml_2.csv'
result_path = BASE_DIR / 'datasets' / 'vessel_dataset_ml.csv'

df1 = pd.read_csv(file1_path)
df2 = pd.read_csv(file2_path)

tag_column = 'tag'

# 1-3. Логика пересчета тегов (оставляем как было)
prefix = df1[tag_column].iloc[-1][0]
last_tag_value = int(df1[tag_column].iloc[-1][1:])
first_tag_df2 = int(df2[tag_column].iloc[0][1:])
offset = last_tag_value - first_tag_df2 + 1

new_numbers = df2[tag_column].str[1:].astype(int) + offset
df2[tag_column] = prefix + new_numbers.astype(str)

# 4. Склеиваем
result = pd.concat([df1, df2], ignore_index=True)

# ---------------------------------------------------------
# 4.5 ПРОВЕРКА И ОЧИСТКА ОТ ДУБЛИКАТОВ
# ---------------------------------------------------------
# Собираем список всех колонок, кроме 'tag'
columns_to_check = [col for col in result.columns if col != tag_column]

# Считаем количество дубликатов
duplicates_mask = result.duplicated(subset=columns_to_check)
duplicates_count = duplicates_mask.sum()

if duplicates_count > 0:
    print(f"⚠️ ВНИМАНИЕ: Найдено {duplicates_count} дублирующихся строк (без учета тега)!")

    # Оставляем только первое вхождение (keep='first'), остальное удаляем
    result = result.drop_duplicates(subset=columns_to_check, keep='first')

    # Сбрасываем индексы после удаления, чтобы не было дыр (0, 1, 3, 4...)
    result = result.reset_index(drop=True)
    print("✅ Дубликаты успешно удалены.")
else:
    print("✅ Дубликатов не найдено, данные чистые.")
# ---------------------------------------------------------

# 5. Сохраняем финальный результат
result.to_csv(result_path, index=False)