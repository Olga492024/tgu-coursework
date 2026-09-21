import pandas as pd
import matplotlib.pyplot as plt
import time
import os

os.makedirs('logs', exist_ok=True)

while True:
    try:
        csv_path = 'logs/metric_log.csv'

        if not os.path.exists(csv_path):
            print("metric_log.csv not found yet, waiting...")
            time.sleep(5)
            continue

        df = pd.read_csv(csv_path)
        print(f"CSV loaded. Columns: {df.columns.tolist()}")  # ← ОТЛАДКА
        print(f"Data shape: {df.shape}")  # ← ОТЛАДКА

        if len(df) > 0:
            # Проверь точное название столбца
            error_column = 'absolute_error'  # Проверь это название!

            if error_column not in df.columns:
                print(f"ERROR: Column '{error_column}' not found!")
                print(f"Available columns: {df.columns.tolist()}")
                time.sleep(5)
                continue

            plt.figure(figsize=(10, 6))
            plt.hist(df[error_column], bins=30, edgecolor='black', color='skyblue')
            plt.xlabel('Абсолютная ошибка')
            plt.ylabel('Частота')
            plt.title('Распределение абсолютных ошибок')
            plt.grid(axis='y', alpha=0.3)

            plt.savefig('logs/error_distribution.png', dpi=100, bbox_inches='tight')
            plt.close()

            print(f"✓ Graph updated: {len(df)} records processed at {time.strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")

    time.sleep(5)