import os
import pandas as pd
import re


def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


# -------------------------------
# 🔧 CLEAN HELPERS
# -------------------------------
def extract_year_clean(val):
    match = re.search(r'(19|20)\d{2}', str(val))
    return int(match.group()) if match else None


def extract_month_clean(val):
    months = {
        'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
        'july':7,'august':8,'september':9,'october':10,'november':11,'december':12
    }

    val = str(val).lower()

    for m in months:
        if m in val:
            return months[m]

    return None


# -------------------------------
# 🌧 CLEAN RAINFALL
# -------------------------------
def clean_rainfall(path):
    df = pd.read_csv(path)

    df = df.rename(columns={
        'Actual Rainfall (UOM:mm(Millimeter)), Scaling Factor:1': 'rainfall',
        'Deviation In Rainfall (UOM:mm(Millimeter)), Scaling Factor:1': 'deviation'
    })

    # 🔥 CLEAN YEAR + MONTH
    df['Year'] = df['Year'].apply(extract_year_clean)
    df['Month'] = df['Month'].apply(extract_month_clean)

    df = df[['State', 'District', 'Year', 'Month', 'rainfall', 'deviation']]

    return df


# -------------------------------
# 💧 CLEAN GROUNDWATER
# -------------------------------
def clean_groundwater(path):
    df = pd.read_csv(path)

    df = df.rename(columns={
        'Ground Water Level (UOM:M(Meter)), Scaling Factor:1': 'groundwater'
    })

    df['Year'] = df['Year'].apply(extract_year_clean)
    df['Month'] = df['Month'].apply(extract_month_clean)

    df = df[['State', 'District', 'Year', 'Month', 'groundwater']]

    return df


# -------------------------------
# 🏞 CLEAN RESERVOIR
# -------------------------------
def clean_reservoir(path):
    df = pd.read_csv(path)

    df = df.rename(columns={
        'Reservoir Water Storage (UOM:BCM(BillionCubicMeter)), Scaling Factor:1': 'reservoir'
    })

    df['Year'] = df['Year'].apply(extract_year_clean)
    df['Month'] = df['Month'].apply(extract_month_clean)

    df = df[['State', 'District', 'Year', 'Month', 'reservoir']]

    return df


# -------------------------------
# 🔥 MAIN BUILDER
# -------------------------------
def build_climate_dataset():
    root = get_project_root()
    raw_path = os.path.join(root, "data", "raw", "climate")

    rainfall_path = os.path.join(raw_path, "Rainfall.csv")
    groundwater_path = os.path.join(raw_path, "prewalai23_17706290804484446.csv")
    reservoir_path = os.path.join(raw_path, "prewalai23_17706291098669744.csv")

    print("📂 Loading datasets...")

    rainfall = clean_rainfall(rainfall_path)
    groundwater = clean_groundwater(groundwater_path)
    reservoir = clean_reservoir(reservoir_path)

    print("🔗 Merging datasets...")

    df = rainfall.merge(groundwater, on=['State', 'District', 'Year', 'Month'], how='left')
    df = df.merge(reservoir, on=['State', 'District', 'Year', 'Month'], how='left')

    print("🧹 Cleaning missing values...")

    # Remove rows with missing Year or Month
    df = df.dropna(subset=['Year', 'Month'])

    # Remove rows with missing rainfall or deviation (critical features)
    df = df.dropna(subset=['rainfall', 'deviation'])

    # Ensure proper sorting before filling
    df = df.sort_values(by=['State', 'District', 'Year', 'Month'])

    # Fill groundwater and reservoir missing values per location
    df[['groundwater', 'reservoir']] = df.groupby(
        ['State', 'District']
    )[['groundwater', 'reservoir']].ffill()

    # Ensure Year and Month are integers
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)

    # Final checks
    print(f"Dataset shape → {df.shape}")
    print(f"Null values → \n{df.isnull().sum()}")
    print(f"Year range → {df['Year'].min()} to {df['Year'].max()}")

    # Save final dataset
    output_dir = os.path.join(root, "data", "processed", "climate")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "final_climate.csv")
    df.to_csv(output_path, index=False)

    print(f"✅ Final dataset saved → {output_path}")
    print(df.head())


if __name__ == "__main__":
    build_climate_dataset()