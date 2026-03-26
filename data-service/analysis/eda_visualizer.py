import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path


def generate_report(file_path='../datasets/dataset_inference.csv'):
    # загрузка и настройка стилей
    df = pd.read_csv(file_path)
    sns.set_theme(style="whitegrid")

    # анализ пропусков
    plt.figure(figsize=(10, 5))
    missing_pct = (df.isnull().sum() / len(df)) * 100
    missing_pct = missing_pct.sort_values(ascending=False)
    sns.barplot(x=missing_pct.values, y=missing_pct.index, palette='Reds_r')
    plt.title('Percentage of Missing Data (AFTER Enrichment)')
    plt.xlabel('% Missing')
    plt.tight_layout()
    plt.savefig('missing_analysis.png')

    # матрица корреляций
    plt.figure(figsize=(12, 10))
    numeric_df = df.select_dtypes(include=[np.number])
    corr = numeric_df.corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', center=0, fmt='.2f')
    plt.title('Feature Correlation Matrix')
    plt.tight_layout()
    plt.savefig('correlations.png')

    # напор vs расход
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='flow_rate', y='fluid_head', alpha=0.6, color='blue')
    sns.regplot(data=df, x='flow_rate', y='fluid_head', scatter=False, color='red')
    plt.title('Pump Performance Mapping (Flow vs Head)')
    plt.xlabel('Flow Rate (L/S)')
    plt.ylabel('Fluid Head (M)')
    plt.savefig('pump_performance.png')

    # распределение целевой переменной
    plt.figure(figsize=(8, 6))
    sns.histplot(df['weight_log'].dropna(), kde=True, color='purple')
    plt.title('Target Variable Distribution: log(Weight)')
    plt.savefig('target_dist.png')

    print("Графики для отчета успешно сгенерированы.")


if __name__ == "__main__":
    generate_report()