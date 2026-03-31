import os
import pandas as pd
import numpy as np


def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


def create_lag_features(df):

    # 🔥 Sort properly (VERY IMPORTANT)
    df = df.sort_values(by=['State', 'District', 'Year', 'Month'])

    # Group by location
    group_cols = ['State', 'District']

    # -----------------------------
    # 🌧 Rainfall lags
    # -----------------------------
    df['rainfall_lag_1'] = df.groupby(group_cols)['rainfall'].shift(1)
    df['rainfall_lag_2'] = df.groupby(group_cols)['rainfall'].shift(2)
    df['rainfall_lag_3'] = df.groupby(group_cols)['rainfall'].shift(3)

    # -----------------------------
    # 💧 Groundwater lags
    # -----------------------------
    df['groundwater_lag_1'] = df.groupby(group_cols)['groundwater'].shift(1)
    df['groundwater_lag_2'] = df.groupby(group_cols)['groundwater'].shift(2)

    # -----------------------------
    # 🏞 Reservoir lag
    # -----------------------------
    df['reservoir_lag_1'] = df.groupby(group_cols)['reservoir'].shift(1)

    # -----------------------------
    # 📊 Rolling mean feature
    # -----------------------------
    df['rainfall_roll_3'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(3).mean())

    # -----------------------------
    # 📈 Trend feature
    # -----------------------------
    df['rainfall_trend'] = df['rainfall_lag_1'] - df['rainfall_lag_2']

    # -----------------------------
    # ⚠️ Climate risk feature
    # -----------------------------
    df['climate_risk'] = abs(df['deviation']) / 100

    # -----------------------------
    # 🔄 Cyclical month encoding (seasonality)
    # -----------------------------
    df['month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)

    # -----------------------------
    # 📊 Rolling std feature
    # -----------------------------
    df['rainfall_std_3'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(3).std())

    # -----------------------------
    # 📈 Rainfall to average ratio
    # -----------------------------

    # -----------------------------
    # 💧 Rainfall-groundwater interaction
    # -----------------------------

    # -----------------------------
    # 🔮 Next-month rainfall prediction target
    # -----------------------------
    df['rainfall_next'] = df.groupby(group_cols)['rainfall'].shift(-1)

    return df


def build_features():
    root = get_project_root()

    input_path = os.path.join(root, "data", "processed", "climate", "final_climate.csv")

    print("📂 Loading dataset...")
    df = pd.read_csv(input_path)

    print("⚙️ Creating lag features...")
    df = create_lag_features(df)

    print("🧹 Removing rows with NaN due to lag...")

    # 🔥 Drop rows where lag is missing
    df = df.dropna(subset=[
        'rainfall_lag_1',
        'rainfall_lag_2',
        'rainfall_lag_3',
        'groundwater_lag_1',
        'rainfall_roll_3',
        'rainfall_trend',
        'rainfall_next'
    ])

    # Save new dataset
    output_path = os.path.join(root, "data", "processed", "climate", "climate_features.csv")
    df.to_csv(output_path, index=False)

    print(f"✅ Features dataset saved → {output_path}")

    # Debug info
    print("\n📊 Dataset shape:", df.shape)
    print("\n📊 Sample:")
    print(df.head())


if __name__ == "__main__":
    build_features()