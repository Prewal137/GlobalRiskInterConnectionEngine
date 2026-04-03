"""
🧠 GEOPOLITICS FEATURE ENGINEERING (Phase 4)

Transform raw conflict/uncertainty data into ML-ready intelligence features.

Input: 
  - data/processed/geopolitics/final_geopolitics.csv

Output:
  - data/processed/geopolitics/geopolitics_features.csv

Features Created:
  1. Time Features (seasonality)
  2. Lag Features (temporal patterns)
  3. Rolling Features (trends & volatility)
  4. Change Features (momentum)
  5. Normalized Features (0-1 scaling)
  6. Instability Score (composite signal)
  7. Target Variable (geopolitical risk)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "final_geopolitics.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_features.csv")


# ================================================================
# 🎯 FEATURE ENGINEERING FUNCTIONS
# ================================================================

def load_data(file_path):
    """Load and prepare base dataset."""
    print("\n📥 Loading data...")
    df = pd.read_csv(file_path)
    
    # Ensure datetime sorting
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df)} rows")
    print(f"   🌍 Countries: {df['Country'].nunique()}")
    print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    return df


def apply_log_transforms(df):
    """
    Apply log transformation to reduce impact of extreme outliers.
    
    Features transformed:
    - conflict_count
    - fatalities_sum
    - deaths_total
    
    Why: Compresses extreme values while preserving patterns
    """
    print("\n🔢 Applying log transformations (reducing outlier dominance)...")
    
    df = df.copy()
    
    # Log transform skewed features
    for col in ['conflict_count', 'fatalities_sum', 'deaths_total']:
        if col in df.columns:
            df[col] = np.log1p(df[col])  # log(1 + x) handles zeros
            print(f"   ✅ Applied log1p to {col}")
    
    return df


def create_time_features(df):
    """
    Create time-based features for seasonality capture.
    
    Features:
    - month_sin: Sine transformation of month
    - month_cos: Cosine transformation of month
    """
    print("\n⏰ Creating time features...")
    
    df = df.copy()
    
    # Cyclical encoding for month (captures seasonality)
    df['month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)
    
    print("   ✅ Created month_sin, month_cos")
    
    return df


def create_lag_features(df):
    """
    Create lag features to capture temporal dependencies.
    
    Lag Features (by Country):
    - conflict_count_lag_1, lag_3, lag_6
    - fatalities_sum_lag_1
    - policy_uncertainty_lag_1
    - global_uncertainty_lag_1
    
    Note: Lag 1 = previous month, Lag 3 = 3 months ago, etc.
    """
    print("\n⏳ Creating lag features...")
    
    df = df.copy()
    
    # Group by country for proper lagging
    df = df.sort_values(['Country', 'Year', 'Month'])
    
    # Conflict lags
    for lag in [1, 3, 6]:
        df[f'conflict_lag_{lag}'] = df.groupby('Country')['conflict_count'].shift(lag)
    
    # Fatalities lag
    df['fatalities_lag_1'] = df.groupby('Country')['fatalities_sum'].shift(1)
    
    # Uncertainty lags
    df['policy_lag_1'] = df.groupby('Country')['policy_uncertainty'].shift(1)
    df['uncertainty_lag_1'] = df.groupby('Country')['global_uncertainty'].shift(1)
    
    print("   ✅ Created: conflict_lag_1/3/6, fatalities_lag_1, policy_lag_1, uncertainty_lag_1")
    
    return df


def create_rolling_features(df):
    """
    Create rolling window features for trend detection.
    
    Rolling Features (by Country):
    - conflict_roll_3, conflict_roll_6 (3-month, 6-month moving averages)
    - fatalities_roll_3
    - global_uncertainty_roll_6
    - conflict_std_3 (volatility measure)
    """
    print("\n📊 Creating rolling features...")
    
    df = df.copy()
    df = df.sort_values(['Country', 'Year', 'Month'])
    
    # Rolling averages (conflict)
    df['conflict_roll_3'] = df.groupby('Country')['conflict_count'].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df['conflict_roll_6'] = df.groupby('Country')['conflict_count'].transform(
        lambda x: x.shift(1).rolling(window=6, min_periods=1).mean()
    )
    
    # Rolling average (fatalities)
    df['fatalities_roll_3'] = df.groupby('Country')['fatalities_sum'].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    
    # Rolling average (uncertainty)
    df['uncertainty_roll_6'] = df.groupby('Country')['global_uncertainty'].transform(
        lambda x: x.shift(1).rolling(window=6, min_periods=1).mean()
    )
    
    # Rolling std (conflict volatility)
    df['conflict_std_3'] = df.groupby('Country')['conflict_count'].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).std()
    )
    
    print("   ✅ Created: conflict_roll_3/6, fatalities_roll_3, uncertainty_roll_6, conflict_std_3")
    
    return df


def create_change_features(df):
    """
    Create change/momentum features to detect sudden spikes.
    
    Change Features (by Country):
    - conflict_change (month-over-month change)
    - fatalities_change
    - uncertainty_change
    - conflict_present (event flag - NEW!)
    - shock (sudden escalation - NEW!)
    """
    print("\n📈 Creating change features...")
    
    df = df.copy()
    df = df.sort_values(['Country', 'Year', 'Month'])
    
    # Month-over-month changes (using log-transformed values)
    df['conflict_change'] = df.groupby('Country')['conflict_count'].diff(1)
    df['fatalities_change'] = df.groupby('Country')['fatalities_sum'].diff(1)
    df['uncertainty_change'] = df.groupby('Country')['global_uncertainty'].diff(1)
    
    # NEW: Event flag - helps model distinguish "something happened" vs "nothing"
    df['conflict_present'] = (df['conflict_count'] > 0).astype(int)
    
    # NEW: Shock feature - detects sudden escalation events
    # Threshold: significant increase in conflicts (top 25% of changes)
    conflict_change_threshold = df['conflict_change'].quantile(0.75)
    df['shock'] = (df['conflict_change'] > conflict_change_threshold).astype(int)
    
    print("   ✅ Created: conflict_change, fatalities_change, uncertainty_change")
    print("   ✅ Added: conflict_present (event flag)")
    print(f"   ✅ Added: shock (sudden escalation, threshold={conflict_change_threshold:.2f})")
    
    return df


def normalize_features(df):
    """
    Normalize key features to 0-1 scale for composite score calculation.
    
    COUNTRY-WISE NORMALIZATION (not global!)
    
    Uses Min-Max normalization per country:
    normalized = (value - min) / (max - min)
    
    Why country-wise:
    - USA vs small countries become comparable
    - Model learns relative risk per country
    - Better generalization across regions
    """
    print("\n⚖️ Normalizing features (country-wise, not global)...")
    
    df = df.copy()
    
    # Features to normalize (including log-transformed ones)
    features_to_normalize = [
        'conflict_count',
        'fatalities_sum',
        'deaths_total',
        'conflict_intensity',
        'policy_uncertainty',
        'global_uncertainty'
    ]
    
    # Normalize each feature PER COUNTRY
    for feature in features_to_normalize:
        if feature in df.columns:
            # Min-Max normalization per country (NOT global!)
            df[f'{feature}_norm'] = df.groupby('Country')[feature].transform(
                lambda x: (x - x.min()) / (x.max() - x.min() + 1e-10)
            )
    
    normalized_cols = [f'{f}_norm' for f in features_to_normalize]
    print(f"   ✅ Normalized: {', '.join(normalized_cols)}")
    print(f"      Method: Country-wise Min-Max (better generalization)")
    
    return df


def create_instability_score(df):
    """
    Create composite instability score - the core innovation!
    
    WEIGHTED VERSION - Gives higher importance to real violence:
    
    instability_score = 
        0.25 * conflict_count_norm +
        0.25 * fatalities_sum_norm +
        0.20 * deaths_total_norm +
        0.15 * conflict_intensity_norm +
        0.10 * policy_uncertainty_norm +
        0.05 * global_uncertainty_norm
    
    Higher score = higher geopolitical instability
    """
    print("\n🔥 Creating weighted instability score...")
    
    df = df.copy()
    
    # List of normalized components with weights
    # Weights reflect real-world impact (deaths/conflicts > uncertainty)
    weighted_components = {
        'conflict_count_norm': 0.25,           # Direct conflict activity
        'fatalities_sum_norm': 0.25,           # Human cost - very important
        'deaths_total_norm': 0.20,             # Overall death toll
        'conflict_intensity_norm': 0.15,       # Severity of conflicts
        'policy_uncertainty_norm': 0.10,       # Policy dimension - less direct
        'global_uncertainty_norm': 0.05        # Global context - least weight
    }
    
    # Calculate weighted sum
    weighted_sum = np.zeros(len(df))
    
    for component, weight in weighted_components.items():
        if component in df.columns:
            weighted_sum += weight * df[component].values
    
    df['instability_score'] = weighted_sum
    
    # Normalize weighted instability score to 0-1
    df['instability_score'] = df.groupby('Country')['instability_score'].transform(
        lambda x: (x - x.min()) / (x.max() - x.min() + 1e-10)
    )
    
    print(f"   ✅ Created weighted instability_score")
    print(f"      Weights: conflict(0.25), fatalities(0.25), deaths(0.20), intensity(0.15), policy(0.10), global(0.05)")
    
    return df


def create_target_variable(df):
    """
    Create target variable for prediction.
    
    Target Variable:
    - geopolitical_risk: 3-month rolling mean of instability_score (smooth trend)
    - geopolitical_risk_raw: Raw instability_score (for spike detection)
    
    IMPORTANT: Shifted by +1 to prevent data leakage!
    """
    print("\n🎯 Creating target variables...")
    
    df = df.copy()
    
    # Ensure sorted by Country and time
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    # Primary target: Smooth trend (3-month rolling mean, shifted forward by 1)
    df['geopolitical_risk'] = df.groupby('Country')['instability_score'].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    
    # Secondary target: Raw signal (shifted forward by 1 to prevent leakage)
    df['geopolitical_risk_raw'] = df.groupby('Country')['instability_score'].shift(1)
    
    # ================================================================
    # 🔮 UPGRADE 1: ADD TIME-FORECAST TARGETS
    # ================================================================
    print("   🚀 Adding forecasting targets (+3 months, +6 months ahead)...")
    
    # Future risk at +3 months (what will risk be 3 months from now?)
    df['future_risk_3m'] = df.groupby('Country')['geopolitical_risk'].shift(-3)
    
    # Future risk at +6 months (what will risk be 6 months from now?)
    df['future_risk_6m'] = df.groupby('Country')['geopolitical_risk'].shift(-6)
    
    print("   ✅ Created geopolitical_risk (primary target)")
    print("   ✅ Created geopolitical_risk_raw (secondary target)")
    print("   ✅ Created future_risk_3m (forecast +3 months)")
    print("   ✅ Created future_risk_6m (forecast +6 months)")
    
    return df


def create_interaction_features(df):
    """
    Create interaction features to capture compound risk effects.
    
    Interaction Features:
    - conflict_uncertainty: High conflict + high uncertainty = MUCH worse
    - fatality_intensity: Deaths × severity = true impact measure
    
    Why interactions matter:
    - Tree models (XGBoost) LOVE interaction features
    - Captures non-linear compound effects
    - Reality: risks multiply, not just add
    """
    print("\n🔗 Creating interaction features (compound risk effects)...")
    
    df = df.copy()
    
    # Interaction 1: Conflict × Uncertainty (amplification effect)
    df['conflict_uncertainty'] = df['conflict_count_norm'] * df['global_uncertainty_norm']
    
    # Interaction 2: Fatalities × Intensity (true human cost)
    df['fatality_intensity'] = df['fatalities_sum_norm'] * df['conflict_intensity_norm']
    
    print("   ✅ Created conflict_uncertainty (conflict × global uncertainty)")
    print("   ✅ Created fatality_intensity (fatalities × conflict intensity)")
    print(f"      Purpose: Capture compound/multiplicative risk effects")
    
    return df


def handle_missing_values(df):
    """
    Handle NaN values created by lag/rolling operations.
    
    Strategy:
    - Fill lag/rolling NaN with 0 (no prior data)
    - Fill change NaN with 0 (no change)
    """
    print("\n🧹 Handling missing values...")
    
    df = df.copy()
    
    # Columns that typically have NaN from lag/rolling
    lag_roll_cols = [col for col in df.columns if any(pattern in col for pattern in 
                     ['lag_', 'roll_', 'std_', 'change'])]
    
    # Fill with 0 (represents no activity/change)
    for col in lag_roll_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    
    # Count remaining NaN
    nan_count = df.isna().sum().sum()
    print(f"   ✅ Filled lag/roll NaN with 0")
    print(f"   ⚠️  Remaining NaN: {nan_count}")
    
    return df


# ================================================================
# 📤 OUTPUT FUNCTIONS
# ================================================================

def save_features(df, output_path):
    """Save final feature dataset."""
    print("\n📤 Saving features...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"   ✅ Saved to {output_path}")
    print(f"   📊 Shape: {df.shape}")


def print_final_summary(df):
    """Print comprehensive feature summary."""
    print("\n" + "="*70)
    print("🎉 PHASE 4 COMPLETE - FEATURE ENGINEERING")
    print("="*70)
    
    print(f"\n📊 DATASET SHAPE: {df.shape}")
    print(f"   Rows: {len(df):,}")
    print(f"   Features: {len(df.columns)}")
    
    # Count feature types
    time_features = [col for col in df.columns if 'sin' in col or 'cos' in col]
    lag_features = [col for col in df.columns if 'lag' in col]
    rolling_features = [col for col in df.columns if 'roll' in col or 'std' in col]
    change_features = [col for col in df.columns if 'change' in col]
    normalized_features = [col for col in df.columns if '_norm' in col]
    score_features = [col for col in df.columns if 'score' in col or 'risk' in col]
    
    print(f"\n📋 FEATURE BREAKDOWN:")
    print(f"   ⏰ Time features: {len(time_features)}")
    print(f"   ⏳ Lag features: {len(lag_features)}")
    print(f"   📊 Rolling features: {len(rolling_features)}")
    print(f"   📈 Change features: {len(change_features)}")
    print(f"   ⚖️ Normalized features: {len(normalized_features)}")
    print(f"   🎯 Score/Risk features: {len(score_features)}")
    
    print(f"\n📈 ALL FEATURES ({len(df.columns)} total):")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:3d}. {col}")
    
    print(f"\n📊 SAMPLE DATA (first 10 rows):")
    print(df.head(10).to_string(index=False))
    
    print("\n" + "="*70)


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function for Phase 4."""
    print("\n🚀 Starting Geopolitics Feature Engineering (Phase 4)...")
    print("="*70)
    
    # Step 1: Load data
    df = load_data(INPUT_FILE)
    
    # Step 1.5: Apply log transforms to reduce outlier impact (NEW!)
    df = apply_log_transforms(df)
    
    # Step 2: Create time features
    df = create_time_features(df)
    
    # Step 3: Create lag features
    df = create_lag_features(df)
    
    # Step 4: Create rolling features
    df = create_rolling_features(df)
    
    # Step 5: Create change features
    df = create_change_features(df)
    
    # Step 6: Normalize features (country-wise, not global)
    df = normalize_features(df)
    
    # Step 7: Create weighted instability score
    df = create_instability_score(df)
    
    # Step 8: Create target variables
    df = create_target_variable(df)
    
    # Step 9: Create interaction features (NEW!)
    df = create_interaction_features(df)
    
    # Step 10: Handle missing values
    df = handle_missing_values(df)
    
    # Step 11: Save final features
    save_features(df, OUTPUT_FILE)
    
    # Step 12: Print summary
    print_final_summary(df)
    
    print("\n✅ Phase 4 completed successfully!")
    print(f"📄 Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error during Phase 4: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
