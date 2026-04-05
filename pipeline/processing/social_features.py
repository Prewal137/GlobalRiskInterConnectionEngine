"""
🔧 Social Sector Feature Engineering Script

Converts cleaned social data into ML-ready features for predicting social risk.

Input: data/processed/social/cleaned/final_social.csv
Output: data/processed/social/features/social_features.csv

Features Created:
- Normalized signals (log + MinMax scaling)
- Time encoding (sin/cos)
- Lag features (1, 2, 3 periods)
- Rolling features (3, 6 period means)
- Volatility features (rolling std)
- Trend/change features
- Target variable (social_risk)
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import os

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 2 levels from pipeline/processing/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "social", "cleaned", "final_social.csv")
OUTPUT_FOLDER = os.path.join(BASE_PATH, "data", "processed", "social", "features")
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "social_features_forecast.csv")

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("="*70)
print("🔧 SOCIAL SECTOR FEATURE ENGINEERING")
print("="*70)
print(f"\n📂 Input file: {INPUT_FILE}")
print(f"📁 Output folder: {OUTPUT_FOLDER}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def normalize_signals(df):
    """
    Normalize protest_count and violence_count using log transform + MinMax scaling.
    
    Steps:
    1. Apply log1p transform to handle skewness
    2. Apply MinMax scaling to bring to [0, 1] range
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with normalized columns added
    """
    print("\n📊 STEP 2: Normalizing signals...")
    
    df = df.copy()
    
    # Columns to normalize
    cols_to_normalize = ['protest_count', 'violence_count']
    
    for col in cols_to_normalize:
        if col not in df.columns:
            print(f"   ⚠️  Warning: {col} not found, skipping")
            continue
        
        # Step 1: Log transform (log1p handles zeros safely)
        log_col = f"{col}_log"
        df[log_col] = np.log1p(df[col])
        
        # Step 2: MinMax scaling
        scaled_col = f"{col}_scaled"
        scaler = MinMaxScaler()
        df[scaled_col] = scaler.fit_transform(df[[log_col]])
        
        print(f"   ✅ {col}: log range [{df[log_col].min():.2f}, {df[log_col].max():.2f}], "
              f"scaled range [{df[scaled_col].min():.4f}, {df[scaled_col].max():.4f}]")
    
    return df


def create_time_encoding(df):
    """
    Create cyclical time encoding for Month column.
    
    Uses sin/cos transformation to capture cyclicality:
    - month_sin = sin(2π * Month / 12)
    - month_cos = cos(2π * Month / 12)
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with time encoding columns added
    """
    print("\n⏰ STEP 3: Creating time encoding...")
    
    df = df.copy()
    
    # Cyclical encoding for Month
    df['month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)
    
    print(f"   ✅ month_sin range: [{df['month_sin'].min():.4f}, {df['month_sin'].max():.4f}]")
    print(f"   ✅ month_cos range: [{df['month_cos'].min():.4f}, {df['month_cos'].max():.4f}]")
    
    return df


def create_lag_features(df):
    """
    Create lag features for each state.
    
    Creates:
    - protest_lag_1, protest_lag_2, protest_lag_3
    - violence_lag_1
    
    Uses groupby("State").shift() to avoid data leakage across states.
    
    Args:
        df (pd.DataFrame): Input DataFrame (must be sorted by State, Year, Month)
        
    Returns:
        pd.DataFrame: DataFrame with lag features added
    """
    print("\n⏮️  STEP 4: Creating lag features...")
    
    df = df.copy()
    
    # Ensure sorted by State, Year, Month
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    
    # Protest lags
    for lag in [1, 2, 3]:
        col_name = f'protest_lag_{lag}'
        df[col_name] = df.groupby('State')['protest_count_scaled'].shift(lag)
        print(f"   ✅ Created {col_name}")
    
    # Violence lag
    df['violence_lag_1'] = df.groupby('State')['violence_count_scaled'].shift(1)
    print(f"   ✅ Created violence_lag_1")
    
    return df


def create_rolling_features(df):
    """
    Create rolling mean features for each state.
    
    Creates:
    - protest_roll_3 (3-period rolling mean)
    - protest_roll_6 (6-period rolling mean)
    - violence_roll_3 (3-period rolling mean)
    
    Args:
        df (pd.DataFrame): Input DataFrame (must be sorted by State, Year, Month)
        
    Returns:
        pd.DataFrame: DataFrame with rolling features added
    """
    print("\n📈 STEP 5: Creating rolling features...")
    
    df = df.copy()
    
    # Ensure sorted
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    
    # Protest rolling means
    for window in [3, 6]:
        col_name = f'protest_roll_{window}'
        df[col_name] = df.groupby('State')['protest_count_scaled'].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean()
        )
        print(f"   ✅ Created {col_name}")
    
    # Violence rolling mean
    df['violence_roll_3'] = df.groupby('State')['violence_count_scaled'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    print(f"   ✅ Created violence_roll_3")
    
    return df


def create_volatility_features(df):
    """
    Create volatility (rolling standard deviation) features.
    
    Creates:
    - protest_std_3 (3-period rolling std)
    - violence_std_3 (3-period rolling std)
    
    Args:
        df (pd.DataFrame): Input DataFrame (must be sorted by State, Year, Month)
        
    Returns:
        pd.DataFrame: DataFrame with volatility features added
    """
    print("\n📉 STEP 6: Creating volatility features...")
    
    df = df.copy()
    
    # Ensure sorted
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    
    # Protest volatility
    df['protest_std_3'] = df.groupby('State')['protest_count_scaled'].transform(
        lambda x: x.rolling(window=3, min_periods=1).std()
    )
    print(f"   ✅ Created protest_std_3")
    
    # Violence volatility
    df['violence_std_3'] = df.groupby('State')['violence_count_scaled'].transform(
        lambda x: x.rolling(window=3, min_periods=1).std()
    )
    print(f"   ✅ Created violence_std_3")
    
    return df


def create_trend_features(df):
    """
    Create trend/change features.
    
    Creates:
    - protest_change = protest_count_scaled - protest_lag_1
    - violence_change = violence_count_scaled - violence_lag_1
    
    Args:
        df (pd.DataFrame): Input DataFrame with lag features
        
    Returns:
        pd.DataFrame: DataFrame with trend features added
    """
    print("\n📊 STEP 7: Creating trend features...")
    
    df = df.copy()
    
    # Protest change
    df['protest_change'] = df['protest_count_scaled'] - df['protest_lag_1']
    print(f"   ✅ Created protest_change")
    
    # Violence change
    df['violence_change'] = df['violence_count_scaled'] - df['violence_lag_1']
    print(f"   ✅ Created violence_change")
    
    return df


def create_target_variable(df):
    """
    Create optimized target variable: social_risk
    
    Formula:
    social_risk = 0.6 * protest_roll_3 + 0.4 * violence_roll_3
    
    Then smooth with 3-period rolling mean.
    
    Args:
        df (pd.DataFrame): Input DataFrame with rolling features
        
    Returns:
        pd.DataFrame: DataFrame with target variable added
    """
    print("\n🎯 STEP 8: Creating target variable (social_risk)...")
    
    df = df.copy()
    
    # Weighted combination
    df['social_risk'] = 0.6 * df['protest_roll_3'] + 0.4 * df['violence_roll_3']
    
    # Smooth with rolling mean
    df['social_risk'] = df.groupby('State')['social_risk'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    print(f"   ✅ social_risk range: [{df['social_risk'].min():.4f}, {df['social_risk'].max():.4f}]")
    print(f"   ✅ social_risk mean: {df['social_risk'].mean():.4f}")
    
    return df


def create_future_target(df):
    """
    Create future prediction target to avoid data leakage.
    
    Shifts social_risk by -1 period within each state:
    social_risk_t_plus_1 = social_risk at time t+1
    
    This enables true predictive modeling where:
    - Features are at time t
    - Target is at time t+1
    
    Args:
        df (pd.DataFrame): Input DataFrame with social_risk column
        
    Returns:
        pd.DataFrame: DataFrame with future target added
    """
    print("\n🔮 Creating future prediction target...")
    
    df = df.copy()
    
    # Ensure sorted by State, Year, Month for proper shifting
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    
    # Create future target by shifting -1 (next period's value)
    df['social_risk_t_plus_1'] = df.groupby('State')['social_risk'].shift(-1)
    
    print(f"   ✅ Future target created: social_risk_t_plus_1")
    print(f"   ✅ social_risk_t_plus_1 range: [{df['social_risk_t_plus_1'].min():.4f}, {df['social_risk_t_plus_1'].max():.4f}]")
    print(f"   ✅ social_risk_t_plus_1 mean: {df['social_risk_t_plus_1'].mean():.4f}")
    print(f"   ⚠️  Note: Last period for each state will have NaN (no future data)")
    
    return df


def select_final_columns(df):
    """
    Select final columns for the feature dataset.
    
    Keeps:
    - State, Year, Month (identifiers)
    - All engineered features (EXCEPT social_risk to avoid leakage)
    - social_risk_t_plus_1 (future prediction target)
    
    Args:
        df (pd.DataFrame): Full DataFrame with all features
        
    Returns:
        pd.DataFrame: Final feature DataFrame
    """
    print("\n📋 STEP 10: Selecting final columns...")
    
    # Define column groups
    identifier_cols = ['State', 'Year', 'Month']
    
    # Original signals (keep for reference)
    original_signals = ['protest_count', 'violence_count', 'conflict_events', 'fatalities']
    
    # Normalized signals
    normalized_signals = ['protest_count_log', 'protest_count_scaled', 
                         'violence_count_log', 'violence_count_scaled']
    
    # Time encoding
    time_features = ['month_sin', 'month_cos']
    
    # Lag features
    lag_features = ['protest_lag_1', 'protest_lag_2', 'protest_lag_3', 'violence_lag_1']
    
    # Rolling features
    rolling_features = ['protest_roll_3', 'protest_roll_6', 'violence_roll_3']
    
    # Volatility features
    volatility_features = ['protest_std_3', 'violence_std_3']
    
    # Trend features
    trend_features = ['protest_change', 'violence_change']
    
    # Target variable (FUTURE prediction - NOT current social_risk)
    target = ['social_risk_t_plus_1']
    
    # IMPORTANT: Do NOT include 'social_risk' in features to avoid data leakage!
    # social_risk is only used to create the future target
    
    # Combine all columns
    final_columns = (identifier_cols + 
                    original_signals + 
                    normalized_signals + 
                    time_features + 
                    lag_features + 
                    rolling_features + 
                    volatility_features + 
                    trend_features + 
                    target)
    
    # Filter to only existing columns
    existing_columns = [col for col in final_columns if col in df.columns]
    
    print(f"   Total columns selected: {len(existing_columns)}")
    print(f"   Features (inputs): {len(existing_columns) - 4}")  # Exclude identifiers and target
    print(f"   Target: social_risk_t_plus_1")
    print(f"   ⚠️  Excluded 'social_risk' from features (prevents data leakage)")
    print(f"   Columns: {existing_columns}")
    
    return df[existing_columns]


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ ERROR: Input file not found: {INPUT_FILE}")
        return
    
    # ================================================================
    # STEP 1: Load data
    # ================================================================
    
    print("\n📄 STEP 1: Loading data...")
    df = pd.read_csv(INPUT_FILE)
    
    print(f"   ✅ Loaded: {len(df):,} rows, {len(df.columns)} columns")
    print(f"   States: {df['State'].nunique()}")
    print(f"   Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    # Sort by State, Year, Month (CRITICAL for time-series features)
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    print(f"   ✅ Sorted by State, Year, Month")
    
    # ================================================================
    # STEP 2: Normalize signals
    # ================================================================
    
    df = normalize_signals(df)
    
    # ================================================================
    # STEP 3: Time encoding
    # ================================================================
    
    df = create_time_encoding(df)
    
    # ================================================================
    # STEP 4: Lag features
    # ================================================================
    
    df = create_lag_features(df)
    
    # ================================================================
    # STEP 5: Rolling features
    # ================================================================
    
    df = create_rolling_features(df)
    
    # ================================================================
    # STEP 6: Volatility features
    # ================================================================
    
    df = create_volatility_features(df)
    
    # ================================================================
    # STEP 7: Trend features
    # ================================================================
    
    df = create_trend_features(df)
    
    # ================================================================
    # STEP 8: Target variable
    # ================================================================
    
    df = create_target_variable(df)
    
    # ================================================================
    # STEP 8.5: Future prediction target
    # ================================================================
    
    df = create_future_target(df)
    
    # ================================================================
    # STEP 9: Handle missing values
    # ================================================================
    
    print("\n🧹 STEP 9: Handling missing values...")
    
    before_rows = len(df)
    
    # Drop rows with NaN from lag/rolling operations AND future target
    df = df.dropna(subset=['social_risk_t_plus_1'])
    
    after_rows = len(df)
    dropped_rows = before_rows - after_rows
    
    print(f"   Dropped {dropped_rows} rows with NaN values")
    print(f"   Remaining rows: {after_rows:,}")
    print(f"   ✅ Includes removal of last period per state (no future data)")
    
    # DO NOT forward fill - this would cause data leakage!
    print(f"   ✅ No forward fill applied (avoiding leakage)")
    
    # ================================================================
    # STEP 10: Select final columns
    # ================================================================
    
    df_final = select_final_columns(df)
    
    # ================================================================
    # STEP 11: Save output
    # ================================================================
    
    print("\n" + "="*70)
    print("💾 STEP 11: Saving output...")
    
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Output saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    
    # ================================================================
    # STEP 12: Logging & Summary
    # ================================================================
    
    print("\n" + "="*70)
    print("📊 FINAL DATASET SUMMARY")
    print("="*70)
    print(f"   Total rows: {len(df_final):,}")
    print(f"   Total features: {len(df_final.columns) - 4}")  # Exclude State, Year, Month, target
    print(f"   Target variable: social_risk_t_plus_1 (FUTURE prediction)")
    print(f"   States: {df_final['State'].nunique()}")
    print(f"   Year range: {df_final['Year'].min()} - {df_final['Year'].max()}")
    print(f"   ✅ Future prediction target created")
    
    # Count feature types
    feature_summary = {
        'Original signals': 4,
        'Normalized signals': 4,
        'Time encoding': 2,
        'Lag features': 4,
        'Rolling features': 3,
        'Volatility features': 2,
        'Trend features': 2,
    }
    
    print(f"\n📈 Feature Breakdown:")
    for feature_type, count in feature_summary.items():
        print(f"   • {feature_type:<25s}: {count}")
    
    total_features = sum(feature_summary.values())
    print(f"   {'─'*40}")
    print(f"   {'Total features':<25s}: {total_features}")
    
    # Show sample
    print(f"\n📄 Sample data (first 5 rows):")
    print(df_final.head().to_string(index=False))
    
    # Show target statistics
    print(f"\n📊 Target Variable Statistics (social_risk_t_plus_1):")
    print(f"   Mean: {df_final['social_risk_t_plus_1'].mean():.4f}")
    print(f"   Std:  {df_final['social_risk_t_plus_1'].std():.4f}")
    print(f"   Min:  {df_final['social_risk_t_plus_1'].min():.4f}")
    print(f"   Max:  {df_final['social_risk_t_plus_1'].max():.4f}")
    
    print("\n" + "="*70)
    print("✅ Social feature engineering completed")
    print("="*70)


if __name__ == "__main__":
    main()
