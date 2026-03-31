import os
import pandas as pd
import re

def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

def extract_year(value):
    if pd.isna(value):
        return None

    match = re.search(r'\b(19|20)\d{2}\b', str(value))
    if match:
        return int(match.group())
    return None


def read_file(filepath):
    try:
        if filepath.endswith(".csv"):
            return pd.read_csv(filepath)

        elif filepath.endswith(".xlsx"):
            return pd.read_excel(filepath)

        elif filepath.endswith(".xls"):
            return pd.read_excel(filepath, engine="xlrd")

    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")
        return None


def analyze_file(filepath):
    df = read_file(filepath)

    if df is None:
        return [], [], None, None

    columns = list(df.columns)

    # 🔥 Detect time columns
    time_columns = [
        col for col in df.columns
        if any(k in col.lower() for k in ["year", "month", "date"])
    ]

    years = []

    # 🔥 Step 1: scan time columns first
    for col in time_columns:
        try:
            values = df[col].dropna().astype(str)

            for val in values:
                year = extract_year(val)
                if year and 1990 <= year <= 2035:
                    years.append(year)
        except:
            continue

    # 🔥 Step 2: fallback (if time columns failed)
    if not years:
        for col in df.columns:
            try:
                sample = df[col].dropna().astype(str).head(1000)

                for val in sample:
                    year = extract_year(val)
                    if year and 1990 <= year <= 2035:
                        years.append(year)
            except:
                continue

    min_year = min(years) if years else None
    max_year = max(years) if years else None

    return columns, time_columns, min_year, max_year

def inspect_climate():
    root = get_project_root()
    climate_path = os.path.join(root, "data", "raw", "climate")

    summary = []

    print(f"\n🔍 Inspecting Climate Sector\n")

    for file in os.listdir(climate_path):
        if file.endswith((".csv", ".xlsx", ".xls")):

            filepath = os.path.join(climate_path, file)

            print(f"🔍 {file}")

            cols, time_cols, min_y, max_y = analyze_file(filepath)

            print(f"   📅 {min_y} → {max_y}")

            summary.append({
                "file": file,
                "columns": ", ".join(cols),
                "time_columns": ", ".join(time_cols),
                "min_year": min_y,
                "max_year": max_y
            })

    df = pd.DataFrame(summary)

    # 🔥 NEW CLEAN PATH
    output_dir = os.path.join(root, "data", "processed", "climate")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "climate_summary.csv")
    df.to_csv(output_path, index=False)

    print(f"\n✅ Saved summary → {output_path}")


if __name__ == "__main__":
    inspect_climate()