"""
🔧 Economy Sector Feature Engineering (Phase 4)

This script creates ML-ready features from the cleaned monthly economy data.

Input: data/processed/economy/final_economy_monthly.csv
Output: data/processed/economy/economy_features.csv

Features Created:
1. Lag Features (t-1, t-2)
2. Rolling Statistics (3-month)
3. Derived Features (changes, returns)
4. Economic Stress Indices
5. Target Variable (Economic Risk Score)
"""

import pandas as pd
import os
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "final_economy_monthly.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "economy_features.csv")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 FEATURE ENGINEERING FUNCTIONS
# ================================================================

def load_data():
    """
    Load cleaned monthly economy data.
    
    Returns:
        pd.DataFrame: Input data
    """
    print("\n📥 Loading input data...")
    
    df = pd.read_csv(INPUT_FILE)
    
    # Sort by time
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df)} rows ({df['Year'].min()}-{df['Year'].max()})")
    print(f"   Columns: {list(df.columns)}")
    
    return df


def create_lag_features(df):
    """
    Create lag features (t-1, t-2).
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with lag features
    """
    print("\n⏱️  Creating Lag Features...")
    
    df = df.copy()
    
    # Inflation lags
    df['inflation_lag_1'] = df.groupby('Country')['Inflation'].shift(1)
    df['inflation_lag_2'] = df.groupby('Country')['Inflation'].shift(2)
    
    # Interest rate lag
    df['interest_lag_1'] = df.groupby('Country')['InterestRate'].shift(1)
    
    # Exchange rate lag
    df['exchange_lag_1'] = df.groupby('Country')['ExchangeRate'].shift(1)
    
    # NIFTY lag
    df['nifty_lag_1'] = df.groupby('Country')['NIFTY50'].shift(1)
    
    # VIX lag
    df['vix_lag_1'] = df.groupby('Country')['VIX'].shift(1)
    
    print(f"   ✅ Created 6 lag features")
    print(f"      - inflation_lag_1, inflation_lag_2")
    print(f"      - interest_lag_1")
    print(f"      - exchange_lag_1")
    print(f"      - nifty_lag_1")
    print(f"      - vix_lag_1")
    
    return df


def create_rolling_features(df):
    """
    Create rolling window statistics (3-month).
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with rolling features
    """
    print("\n📊 Creating Rolling Features...")
    
    df = df.copy()
    
    # Inflation rolling mean (3 months)
    df['inflation_roll_3'] = df.groupby('Country')['Inflation'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # NIFTY rolling mean (3 months)
    df['nifty_roll_3'] = df.groupby('Country')['NIFTY50'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # VIX rolling mean (3 months)
    df['vix_roll_3'] = df.groupby('Country')['VIX'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # Inflation volatility (std dev over 3 months)
    df['inflation_std_3'] = df.groupby('Country')['Inflation'].transform(
        lambda x: x.rolling(window=3, min_periods=1).std()
    )
    
    print(f"   ✅ Created 4 rolling features")
    print(f"      - inflation_roll_3, nifty_roll_3, vix_roll_3")
    print(f"      - inflation_std_3")
    
    return df


def create_derived_features(df):
    """
    Create derived features (changes, returns).
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with derived features
    """
    print("\n🔄 Creating Derived Features...")
    
    df = df.copy()
    
    # Inflation change (month-over-month)
    df['inflation_change'] = df.groupby('Country')['Inflation'].diff(1)
    
    # NIFTY returns (percentage change)
    df['nifty_return'] = df.groupby('Country')['NIFTY50'].pct_change(1)
    
    # VIX change (month-over-month)
    df['vix_change'] = df.groupby('Country')['VIX'].diff(1)
    
    print(f"   ✅ Created 3 derived features")
    print(f"      - inflation_change")
    print(f"      - nifty_return")
    print(f"      - vix_change")
    
    return df


def create_stress_features(df):
    """
    Create economic stress indices.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with stress features
    """
    print("\n⚠️  Creating Economic Stress Features...")
    
    df = df.copy()
    
    # Stagflation Index = Inflation × Interest Rate
    # High values indicate stagflationary pressure (high inflation + high rates)
    df['stagflation_index'] = df['Inflation'] * df['InterestRate']
    
    # Market Stress = VIX / NIFTY
    # High values indicate market fear/stress relative to market level
    df['market_stress'] = df['VIX'] / df['NIFTY50']
    
    print(f"   ✅ Created 2 stress features")
    print(f"      - stagflation_index (Inflation × InterestRate)")
    print(f"      - market_stress (VIX / NIFTY50)")
    
    return df


def create_target_variable(df, train_end_year=2019, predict_future=True):
    """
    Create target variable: Economic Risk Score.
    
    Formula:
    economic_risk = normalized(inflation + vix + interest - nifty)
    Then smoothed with 3-month rolling mean
    
    CRITICAL FIX: Normalization done WITHOUT data leakage
    - Fit scaler on training period ONLY (<= train_end_year)
    - Transform entire dataset using training scaler
    
    ADVANCED FEATURE: Predict FUTURE risk instead of current
    - If predict_future=True: Target = economic_risk(t+1)
    - If predict_future=False: Target = economic_risk(t)
    
    Args:
        df (pd.DataFrame): Input DataFrame
        train_end_year (int): End year for training period
        predict_future (bool): Whether to predict next month's risk
        
    Returns:
        pd.DataFrame: DataFrame with target variable and scaler
    """
    print("\n🎯 Creating Target Variable (Economic Risk Score)...")
    print(f"   ⚠️  NORMALIZATION LEAKAGE FIX: Fitting scaler on data ≤ {train_end_year} ONLY")
    
    df = df.copy()
    
    # Step 1: Normalize components using MinMaxScaler - FIT ON TRAINING DATA ONLY
    scaler = MinMaxScaler(feature_range=(0, 1))
    
    # Select columns to normalize
    norm_cols = ['Inflation', 'VIX', 'InterestRate', 'NIFTY50']
    
    # CRITICAL: Split by time to prevent leakage
    train_mask = df['Year'] <= train_end_year
    test_mask = df['Year'] > train_end_year
    
    # Fit scaler ONLY on training data
    train_data = df.loc[train_mask, norm_cols].values
    scaler.fit(train_data)
    
    print(f"   ✅ Scaler fitted on {train_mask.sum()} rows (Year ≤ {train_end_year})")
    print(f"   ✅ Scaler will transform {test_mask.sum()} test rows (Year > {train_end_year})")
    
    # Transform entire dataset using training scaler
    normalized_values = scaler.transform(df[norm_cols].values)
    
    # Add back to dataframe
    df['norm_inflation'] = normalized_values[:, 0]
    df['norm_vix'] = normalized_values[:, 1]
    df['norm_interest'] = normalized_values[:, 2]
    df['norm_nifty'] = normalized_values[:, 3]
    
    # Step 2: Calculate raw risk score
    # Higher inflation, VIX, interest = higher risk
    # Higher NIFTY = lower risk (good economic condition)
    df['economic_risk_raw'] = (
        df['norm_inflation'] + 
        df['norm_vix'] + 
        df['norm_interest'] - 
        df['norm_nifty']
    )
    
    # Step 3: Smooth using 3-month rolling mean
    df['economic_risk'] = df.groupby('Country')['economic_risk_raw'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # Drop intermediate columns
    df = df.drop(columns=['norm_inflation', 'norm_vix', 'norm_interest', 
                          'norm_nifty', 'economic_risk_raw'])
    
    # 🔥 ADVANCED: Create FUTURE RISK target (predict next month)
    if predict_future:
        print(f"\n🔥 PREDICTING FUTURE RISK: Model will predict economic_risk(t+1)")
        print(f"   This enables TRUE forecasting of next month's economic conditions")
        
        # Shift target forward by 1 month
        df['economic_risk_next'] = df.groupby('Country')['economic_risk'].shift(-1)
        
        # Report impact
        print(f"   ✅ Created forward-looking target: economic_risk_next")
        print(f"   📊 Rows with valid future target: {df['economic_risk_next'].notna().sum()}")
        print(f"   📊 Rows lost (last month per country): {df['economic_risk_next'].isna().sum()}")
    
    print(f"   ✅ Created target variable: economic_risk")
    print(f"      Formula: (norm_inflation + norm_vix + norm_interest - norm_nifty)")
    print(f"      Smoothing: 3-month rolling mean")
    print(f"   📦 Scaler saved for later use in model training")
    
    return df, scaler


def handle_missing_values(df):
    """
    Handle missing values created during feature engineering.
    
    Args:
        df (pd.DataFrame): DataFrame with features
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\n🧹 Handling Missing Values...")
    
    # Report missing before
    missing_before = df.isna().sum().sum()
    print(f"   Missing values before: {missing_before}")
    
    # Drop rows with any NaN values
    # (We use interpolation only if needed, but for now drop NA)
    df_clean = df.dropna().reset_index(drop=True)
    
    # Report missing after
    missing_after = df_clean.isna().sum().sum()
    print(f"   Missing values after: {missing_after}")
    print(f"   Rows dropped: {len(df) - len(df_clean)}")
    
    return df_clean


def finalize_features(df):
    """
    Finalize feature set and prepare output.
    
    Args:
        df (pd.DataFrame): DataFrame with all features
        
    Returns:
        pd.DataFrame: Final feature matrix
    """
    print("\n📦 Finalizing Feature Set...")
    
    # Define final column order
    feature_cols = [
        # Base features
        'Inflation', 'InterestRate', 'ExchangeRate', 'NIFTY50', 'VIX',
        
        # Lag features
        'inflation_lag_1', 'inflation_lag_2',
        'interest_lag_1', 'exchange_lag_1',
        'nifty_lag_1', 'vix_lag_1',
        
        # Rolling features
        'inflation_roll_3', 'nifty_roll_3', 'vix_roll_3', 'inflation_std_3',
        
        # Derived features
        'inflation_change', 'nifty_return', 'vix_change',
        
        # Stress features
        'stagflation_index', 'market_stress',
        
        # Current target (for reference)
        'economic_risk',
        
        # Future target (PRIMARY TARGET FOR PREDICTION)
        'economic_risk_next'
    ]
    
    # Keep only available columns
    available_cols = ['Country', 'Year', 'Month'] + [col for col in feature_cols if col in df.columns]
    
    df_final = df[available_cols].copy()
    
    # Ensure correct data types
    df_final['Year'] = df_final['Year'].astype(int)
    df_final['Month'] = df_final['Month'].astype(int)
    
    numeric_cols = df_final.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col not in ['Year', 'Month']:
            df_final[col] = df_final[col].astype(float)
    
    print(f"   ✅ Final columns: {len(df_final.columns)}")
    print(f"   ✅ Final rows: {len(df_final)}")
    print(f"   🎯 PRIMARY TARGET: economic_risk_next (predicts NEXT month's risk)")
    
    return df_final


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🔧 ECONOMY SECTOR FEATURE ENGINEERING (PHASE 4)")
    print("="*70)
    
    # ============================================================
    # STEP 1: LOAD DATA
    # ============================================================
    
    print("\n📥 STEP 1: Loading input data...")
    print("-" * 60)
    
    df = load_data()
    
    # ============================================================
    # STEP 2: CREATE LAG FEATURES
    # ============================================================
    
    print("\n\n📥 STEP 2: Creating lag features...")
    print("-" * 60)
    
    df = create_lag_features(df)
    
    # ============================================================
    # STEP 3: CREATE ROLLING FEATURES
    # ============================================================
    
    print("\n\n📥 STEP 3: Creating rolling features...")
    print("-" * 60)
    
    df = create_rolling_features(df)
    
    # ============================================================
    # STEP 4: CREATE DERIVED FEATURES
    # ============================================================
    
    print("\n\n📥 STEP 4: Creating derived features...")
    print("-" * 60)
    
    df = create_derived_features(df)
    
    # ============================================================
    # STEP 5: CREATE STRESS FEATURES
    # ============================================================
    
    print("\n\n📥 STEP 5: Creating stress features...")
    print("-" * 60)
    
    df = create_stress_features(df)
    
    # ============================================================
    # STEP 6: CREATE TARGET VARIABLE
    # ============================================================
    
    print("\n\n📥 STEP 6: Creating target variable...")
    print("-" * 60)
    
    df, scaler = create_target_variable(df, train_end_year=2019)
    
    # Save scaler for later use in model training
    import joblib
    scaler_output = os.path.join(BASE_PATH, "models", "trained", "economy_scaler.pkl")
    os.makedirs(os.path.dirname(scaler_output), exist_ok=True)
    joblib.dump(scaler, scaler_output)
    print(f"\n✅ Scaler saved → {scaler_output}")
    
    # ============================================================
    # STEP 7: HANDLE MISSING VALUES
    # ============================================================
    
    print("\n\n📥 STEP 7: Handling missing values...")
    print("-" * 60)
    
    df_clean = handle_missing_values(df)
    
    # ============================================================
    # STEP 8: FINALIZE FEATURES
    # ============================================================
    
    print("\n\n📥 STEP 8: Finalizing feature set...")
    print("-" * 60)
    
    df_final = finalize_features(df_clean)
    
    # ============================================================
    # STEP 9: SAVE OUTPUT
    # ============================================================
    
    print("\n\n📥 STEP 9: Saving output...")
    print("-" * 60)
    
    # Save to CSV
    df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    print(f"\n✅ Features saved → {OUTPUT_FILE}")
    
    # Display summary
    print("\n" + "="*70)
    print("📊 FEATURE ENGINEERING SUMMARY")
    print("="*70)
    print(f"\n   Total features created: {len(df_final.columns) - 3} (excluding Country, Year, Month)")
    print(f"   Final dataset shape: {df_final.shape}")
    print(f"   Time range: {df_final['Year'].min()}/{df_final['Month'].min()} - {df_final['Year'].max()}/{df_final['Month'].max()}")
    
    # Feature breakdown
    print(f"\n   Feature Categories:")
    print(f"      Base features: 5")
    print(f"      Lag features: 6")
    print(f"      Rolling features: 4")
    print(f"      Derived features: 3")
    print(f"      Stress features: 2")
    print(f"      Target variable: 1")
    
    # Target variable stats
    print(f"\n   🎯 Target Variable (economic_risk):")
    print(f"      Mean: {df_final['economic_risk'].mean():.4f}")
    print(f"      Std: {df_final['economic_risk'].std():.4f}")
    print(f"      Min: {df_final['economic_risk'].min():.4f}")
    print(f"      Max: {df_final['economic_risk'].max():.4f}")
    
    print("\n" + "="*70)
    print("✅ ECONOMY FEATURE ENGINEERING COMPLETED")
    print("="*70)


if __name__ == "__main__":
    main()
