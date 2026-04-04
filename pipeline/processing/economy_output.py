"""
📊 Economy Risk Output Postprocessing

This script enhances raw model predictions with interpretable risk levels,
direction indicators, and smoothing for production deployment.

Input:  data/processed/economy/economic_risk_output_v2.csv
Output: data/processed/economy/economic_risk_final.csv

Enhancements:
    1. Risk Levels (LOW/MEDIUM/HIGH)
    2. Risk Direction (UP/DOWN/STABLE)
    3. Smoothed predictions (3-month rolling mean)
    4. Clean structured output for API consumption
"""

import pandas as pd
import numpy as np
import os

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "economic_risk_output_v2.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "economic_risk_final.csv")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 STEP 1: LOAD DATA
# ================================================================

def load_data():
    """
    Load and prepare economy risk predictions.
    
    Returns:
        pd.DataFrame: Loaded and sorted dataset
    """
    print("\n" + "="*70)
    print("📥 STEP 1: LOADING DATA")
    print("="*70)
    
    df = pd.read_csv(INPUT_FILE)
    
    # Sort by time (critical for time-series operations)
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df):,} rows")
    print(f"   📅 Time range: {df['Year'].min()}/{df['Month'].min()} - {df['Year'].max()}/{df['Month'].max()}")
    print(f"   🌍 Countries: {df['Country'].nunique()}")
    print(f"   📊 Columns: {list(df.columns)}")
    
    # Handle different prediction column names
    # Use ensemble if available, otherwise RF, otherwise first prediction column
    if 'predicted_risk_ensemble' in df.columns:
        df['predicted_risk'] = df['predicted_risk_ensemble']
        print(f"   ✅ Using ensemble predictions")
    elif 'predicted_risk_rf' in df.columns:
        df['predicted_risk'] = df['predicted_risk_rf']
        print(f"   ✅ Using RandomForest predictions")
    elif 'predicted_risk_xgb' in df.columns:
        df['predicted_risk'] = df['predicted_risk_xgb']
        print(f"   ✅ Using XGBoost predictions")
    elif 'predicted_risk' in df.columns:
        print(f"   ✅ Using predicted_risk column")
    else:
        raise ValueError("No prediction column found in input file!")
    
    return df

# ================================================================
# 🔧 STEP 2: CREATE RISK LEVEL
# ================================================================

def create_risk_level(df):
    """
    Categorize predicted risk into interpretable levels using DYNAMIC QUANTILE-BASED THRESHOLDS.
    
    This approach ensures balanced distribution across risk categories by using:
        - q1 (33rd percentile): LOW/MEDIUM boundary
        - q2 (66th percentile): MEDIUM/HIGH boundary
    
    Benefits:
        - Balanced categories (~33% each)
        - Adapts to data distribution
        - More realistic than fixed thresholds
    
    Args:
        df (pd.DataFrame): Dataset with predicted_risk column
        
    Returns:
        pd.DataFrame: Dataset with risk_level column added
    """
    print("\n" + "="*70)
    print("🎯 STEP 2: CREATING RISK LEVELS (DYNAMIC THRESHOLDS)")
    print("="*70)
    
    df = df.copy()
    
    # Calculate dynamic quantile-based thresholds
    q1 = df['predicted_risk'].quantile(0.33)
    q2 = df['predicted_risk'].quantile(0.66)
    
    print(f"   📊 Dynamic Thresholds Calculated:")
    print(f"      q1 (33rd percentile): {q1:.4f}")
    print(f"      q2 (66th percentile): {q2:.4f}")
    print(f"      Min risk: {df['predicted_risk'].min():.4f}")
    print(f"      Max risk: {df['predicted_risk'].max():.4f}")
    
    def classify_risk_dynamic(risk_score, threshold_q1, threshold_q2):
        """Classify risk score using dynamic thresholds."""
        if pd.isna(risk_score):
            return "UNKNOWN"
        elif risk_score < threshold_q1:
            return "LOW"
        elif risk_score < threshold_q2:
            return "MEDIUM"
        else:
            return "HIGH"
    
    # Apply classification with dynamic thresholds
    df['risk_level'] = df['predicted_risk'].apply(
        lambda x: classify_risk_dynamic(x, q1, q2)
    )
    
    # Report distribution
    level_counts = df['risk_level'].value_counts()
    print(f"\n   ✅ Risk levels assigned (BALANCED DISTRIBUTION):")
    for level in ['LOW', 'MEDIUM', 'HIGH', 'UNKNOWN']:
        if level in level_counts.index:
            count = level_counts[level]
            pct = count / len(df) * 100
            bar = '█' * int(pct / 2)
            print(f"      {level:<10s}: {count:3d} ({pct:5.1f}%) {bar}")
    
    print(f"\n   💡 BENEFIT: Categories are now balanced and adaptive to data distribution")
    
    return df

# ================================================================
# 🔧 STEP 3: CREATE RISK DIRECTION
# ================================================================

def create_risk_direction(df):
    """
    Determine risk direction by comparing with previous month.
    
    Logic:
        UP:     predicted_risk(t) > predicted_risk(t-1)
        DOWN:   predicted_risk(t) < predicted_risk(t-1)
        STABLE: First row or no change
    
    Args:
        df (pd.DataFrame): Dataset with predicted_risk column
        
    Returns:
        pd.DataFrame: Dataset with risk_direction column added
    """
    print("\n" + "="*70)
    print("📈 STEP 3: CREATING RISK DIRECTION")
    print("="*70)
    
    df = df.copy()
    
    # Calculate previous month's risk (per country)
    df['prev_risk'] = df.groupby('Country')['predicted_risk'].shift(1)
    
    # Determine direction
    conditions = [
        df['predicted_risk'] > df['prev_risk'],
        df['predicted_risk'] < df['prev_risk'],
        df['predicted_risk'] == df['prev_risk'],
        df['prev_risk'].isna()  # First month for each country
    ]
    
    choices = ['UP', 'DOWN', 'STABLE', 'STABLE']
    
    df['risk_direction'] = np.select(conditions, choices, default='UNKNOWN')
    
    # Drop temporary column
    df = df.drop(columns=['prev_risk'])
    
    # Report distribution
    direction_counts = df['risk_direction'].value_counts()
    print(f"   ✅ Risk directions assigned:")
    for direction, count in direction_counts.items():
        pct = count / len(df) * 100
        bar = '█' * int(pct / 2)
        print(f"      {direction:<10s}: {count:3d} ({pct:5.1f}%) {bar}")
    
    return df

# ================================================================
# 🔧 STEP 4: SMOOTHING (RECOMMENDED)
# ================================================================

def apply_smoothing(df, window=3):
    """
    Apply rolling mean smoothing to reduce noise.
    
    Args:
        df (pd.DataFrame): Dataset with predicted_risk column
        window (int): Rolling window size (default: 3 months)
        
    Returns:
        pd.DataFrame: Dataset with smoothed_risk column added
    """
    print("\n" + "="*70)
    print("🔧 STEP 4: APPLYING SMOOTHING")
    print("="*70)
    
    df = df.copy()
    
    # Apply 3-month rolling mean per country
    df['smoothed_risk'] = df.groupby('Country')['predicted_risk'].transform(
        lambda x: x.rolling(window=window, min_periods=1).mean()
    )
    
    print(f"   ✅ Smoothing applied (window={window} months)")
    print(f"   📊 Smoothing stats:")
    print(f"      Original std: {df['predicted_risk'].std():.4f}")
    print(f"      Smoothed std: {df['smoothed_risk'].std():.4f}")
    print(f"      Noise reduction: {(1 - df['smoothed_risk'].std()/df['predicted_risk'].std())*100:.1f}%")
    
    return df

# ================================================================
# 🔧 STEP 5: FINAL STRUCTURE
# ================================================================

def finalize_output(df):
    """
    Create final structured output with clean column names.
    
    Final columns:
        Country, Year, Month, actual_risk, predicted_risk,
        smoothed_risk, risk_level, risk_direction
    
    Args:
        df (pd.DataFrame): Enhanced dataset
        
    Returns:
        pd.DataFrame: Final structured output
    """
    print("\n" + "="*70)
    print("📋 STEP 5: FINALIZING OUTPUT STRUCTURE")
    print("="*70)
    
    # Define final column order
    final_columns = [
        'Country',
        'Year',
        'Month',
        'actual_risk',
        'predicted_risk',
        'smoothed_risk',
        'risk_level',
        'risk_direction'
    ]
    
    # Select only required columns
    df_final = df[final_columns].copy()
    
    # Ensure correct data types
    df_final['Year'] = df_final['Year'].astype(int)
    df_final['Month'] = df_final['Month'].astype(int)
    df_final['actual_risk'] = df_final['actual_risk'].astype(float)
    df_final['predicted_risk'] = df_final['predicted_risk'].astype(float)
    df_final['smoothed_risk'] = df_final['smoothed_risk'].astype(float)
    df_final['risk_level'] = df_final['risk_level'].astype(str)
    df_final['risk_direction'] = df_final['risk_direction'].astype(str)
    
    print(f"   ✅ Final structure created")
    print(f"   📊 Shape: {df_final.shape}")
    print(f"   📋 Columns: {list(df_final.columns)}")
    
    # Display sample
    print(f"\n   📄 SAMPLE OUTPUT (first 10 rows):")
    print("-" * 100)
    print(df_final.head(10).to_string(index=False))
    print("-" * 100)
    
    return df_final

# ================================================================
# 🔧 STEP 6: SAVE OUTPUT
# ================================================================

def save_output(df, output_path=OUTPUT_FILE):
    """
    Save final enhanced predictions to CSV.
    
    Args:
        df (pd.DataFrame): Final dataset
        output_path (str): Output file path
    """
    print("\n" + "="*70)
    print("💾 STEP 6: SAVING OUTPUT")
    print("="*70)
    
    # Save to CSV
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(output_path) / 1024
    print(f"   ✅ Output saved to: {output_path}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Rows: {len(df):,}")
    print(f"   📋 Columns: {len(df.columns)}")

# ================================================================
# 🔧 VALIDATION & QUALITY CHECKS
# ================================================================

def validate_output(df):
    """
    Perform quality checks on final output.
    
    Args:
        df (pd.DataFrame): Final dataset
    """
    print("\n" + "="*70)
    print("✅ VALIDATION & QUALITY CHECKS")
    print("="*70)
    
    issues = []
    
    # Check 1: No missing values in critical columns
    critical_cols = ['Country', 'Year', 'Month', 'predicted_risk', 'risk_level', 'risk_direction']
    missing = df[critical_cols].isna().sum()
    
    if missing.sum() > 0:
        issues.append(f"⚠️  Missing values found:\n{missing[missing > 0]}")
    else:
        print(f"   ✅ No missing values in critical columns")
    
    # Check 2: Valid risk levels
    valid_levels = {'LOW', 'MEDIUM', 'HIGH', 'UNKNOWN'}
    actual_levels = set(df['risk_level'].unique())
    
    if not actual_levels.issubset(valid_levels):
        invalid = actual_levels - valid_levels
        issues.append(f"⚠️  Invalid risk levels found: {invalid}")
    else:
        print(f"   ✅ All risk levels are valid")
    
    # Check 3: Valid risk directions
    valid_directions = {'UP', 'DOWN', 'STABLE', 'UNKNOWN'}
    actual_directions = set(df['risk_direction'].unique())
    
    if not actual_directions.issubset(valid_directions):
        invalid = actual_directions - valid_directions
        issues.append(f"⚠️  Invalid risk directions found: {invalid}")
    else:
        print(f"   ✅ All risk directions are valid")
    
    # Check 4: Time ordering preserved
    is_sorted = df.equals(df.sort_values(['Country', 'Year', 'Month']))
    
    if not is_sorted:
        issues.append("⚠️  Data is not properly sorted by time")
    else:
        print(f"   ✅ Time ordering preserved")
    
    # Check 5: Reasonable prediction range
    pred_min = df['predicted_risk'].min()
    pred_max = df['predicted_risk'].max()
    
    print(f"   📊 Prediction range: [{pred_min:.4f}, {pred_max:.4f}]")
    
    if abs(pred_max) > 10 or abs(pred_min) > 10:
        issues.append(f"⚠️  Extreme predictions detected (range: {pred_min:.2f} to {pred_max:.2f})")
    else:
        print(f"   ✅ Predictions within reasonable range")
    
    # Summary
    print("\n" + "-"*70)
    if len(issues) == 0:
        print("   🎉 ALL VALIDATION CHECKS PASSED!")
    else:
        print(f"   ⚠️  {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"      {issue}")
    print("-"*70)

# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("📊 ECONOMY RISK OUTPUT POSTPROCESSING")
    print("="*70)
    
    try:
        # ========================================
        # STEP 1: LOAD DATA
        # ========================================
        df = load_data()
        
        # ========================================
        # STEP 2: CREATE RISK LEVEL
        # ========================================
        df = create_risk_level(df)
        
        # ========================================
        # STEP 3: CREATE RISK DIRECTION
        # ========================================
        df = create_risk_direction(df)
        
        # ========================================
        # STEP 4: APPLY SMOOTHING
        # ========================================
        df = apply_smoothing(df, window=3)
        
        # ========================================
        # STEP 5: FINALIZE OUTPUT
        # ========================================
        df_final = finalize_output(df)
        
        # ========================================
        # STEP 6: SAVE OUTPUT
        # ========================================
        save_output(df_final)
        
        # ========================================
        # VALIDATION
        # ========================================
        validate_output(df_final)
        
        # ========================================
        # FINAL SUMMARY
        # ========================================
        print("\n" + "="*70)
        print("✅ POSTPROCESSING COMPLETE")
        print("="*70)
        
        print(f"\n   📊 FINAL OUTPUT SUMMARY:")
        print(f"      Total records: {len(df_final):,}")
        print(f"      Time range: {df_final['Year'].min()}/{df_final['Month'].min()} - {df_final['Year'].max()}/{df_final['Month'].max()}")
        print(f"      Countries: {df_final['Country'].nunique()}")
        
        print(f"\n   📋 ENHANCEMENTS ADDED:")
        print(f"      ✅ Risk Levels (LOW/MEDIUM/HIGH)")
        print(f"      ✅ Risk Direction (UP/DOWN/STABLE)")
        print(f"      ✅ Smoothed Predictions (3-month rolling mean)")
        print(f"      ✅ Production-ready structure")
        
        print(f"\n   💾 OUTPUT FILE:")
        print(f"      {OUTPUT_FILE}")
        
        print("\n" + "="*70)
        print("🎯 OUTPUT READY FOR API DEPLOYMENT")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
