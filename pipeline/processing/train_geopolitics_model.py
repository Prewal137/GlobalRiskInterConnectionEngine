"""
🤖 GEOPOLITICS MODEL TRAINING (Phase 5)

Train XGBoost model to predict geopolitical risk.

Input: 
  - data/processed/geopolitics/geopolitics_features.csv

Output:
  - models/trained/geopolitics_model.pkl

Target Variable:
  - geopolitical_risk (primary)

Strategy:
  - Time-based split (train ≤ 2019, test > 2019)
  - No random split (time-series data!)
"""

import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import os
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_features.csv")
OUTPUT_MODEL = os.path.join(BASE_PATH, "models", "trained", "geopolitics_model.pkl")


# ================================================================
# 📥 LOAD AND PREPARE DATA
# ================================================================

def load_data(file_path):
    """Load and prepare dataset."""
    print("\n📥 Loading data...")
    df = pd.read_csv(file_path)
    
    # Sort by Country, Year, Month (critical for time-series)
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df):,} rows")
    print(f"   🌍 Countries: {df['Country'].nunique()}")
    print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    return df


def prepare_features(df):
    """
    Prepare features and target variable.
    
    CRITICAL FIX - Remove data leakage sources:
    
    Drop columns:
    - Country (keep for grouping but not model input)
    - Year, Month (already encoded in features)
    - geopolitical_risk (target)
    - geopolitical_risk_raw (alternative target, not used now)
    - instability_score ❌ LEAKAGE! (directly used to create target)
    """
    print("\n🔧 Preparing features...")
    print("   ⚠️  CRITICAL: Removing data leakage sources")
    
    # Columns to drop (including leakage!)
    drop_cols = [
        'Country', 
        'Year', 
        'Month', 
        'geopolitical_risk', 
        'geopolitical_risk_raw',
        'instability_score'  # ❌ REMOVED - Direct leakage!
    ]
    
    print(f"   🚫 Dropping features: {drop_cols}")
    print(f"      Reason: Target leakage (instability_score directly creates target)")
    
    # Separate target
    y = df['geopolitical_risk']
    
    # Drop unnecessary columns + leakage
    X = df.drop(columns=drop_cols, errors='ignore')
    
    print(f"   ✅ Features shape AFTER leakage removal: {X.shape}")
    print(f"   ✅ Target shape: {y.shape}")
    print(f"   📋 Remaining feature columns: {list(X.columns)}")
    
    return X, y


def time_based_split(X, y, df, train_end_year=2019):
    """
    Time-based train-test split.
    
    Train: Year <= 2019
    Test: Year > 2019
    
    Why: Simulates real-world prediction, avoids future leakage
    """
    print(f"\n⏰ Performing time-based split (train ≤ {train_end_year}, test > {train_end_year})...")
    
    train_mask = df['Year'] <= train_end_year
    test_mask = df['Year'] > train_end_year
    
    X_train = X[train_mask]
    X_test = X[test_mask]
    y_train = y[train_mask]
    y_test = y[test_mask]
    
    print(f"   ✅ Train size: {len(X_train):,} rows ({len(X_train)/len(X)*100:.1f}%)")
    print(f"   ✅ Test size: {len(X_test):,} rows ({len(X_test)/len(X)*100:.1f}%)")
    
    # Check for NaN in target
    nan_train = y_train.isna().sum()
    nan_test = y_test.isna().sum()
    
    if nan_train > 0 or nan_test > 0:
        print(f"   ⚠️  NaN in train target: {nan_train}")
        print(f"   ⚠️  NaN in test target: {nan_test}")
        print(f"      Filling with 0...")
        y_train = y_train.fillna(0)
        y_test = y_test.fillna(0)
    
    return X_train, X_test, y_train, y_test


# ================================================================
# 🤖 MODEL TRAINING
# ================================================================

def train_xgboost_model(X_train, y_train):
    """
    Train XGBoost Regressor.
    
    Parameters optimized for geopolitical risk prediction:
    - n_estimators = 300 (enough trees for complex patterns)
    - max_depth = 6 (moderate depth to avoid overfitting)
    - learning_rate = 0.05 (slow learning for better generalization)
    - subsample = 0.8 (80% data per tree - regularization)
    - colsample_bytree = 0.8 (80% features per tree - regularization)
    """
    print("\n🤖 Training XGBoost model...")
    
    model = XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        verbosity=1
    )
    
    print("   📦 Model parameters:")
    print(f"      n_estimators: 300")
    print(f"      max_depth: 6")
    print(f"      learning_rate: 0.05")
    print(f"      subsample: 0.8")
    print(f"      colsample_bytree: 0.8")
    
    # Fit model
    print("   🏋️  Fitting model...")
    model.fit(X_train, y_train)
    
    print("   ✅ Model trained successfully!")
    
    return model


# ================================================================
# 📊 EVALUATION
# ================================================================

def evaluate_model(model, X_test, y_test):
    """
    Evaluate model on test set.
    
    Metrics:
    - MAE (Mean Absolute Error)
    - RMSE (Root Mean Squared Error)
    - R² Score
    """
    print("\n📊 Evaluating model...")
    
    # Predict
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    print(f"   ✅ MAE:  {mae:.6f}")
    print(f"   ✅ RMSE: {rmse:.6f}")
    print(f"   ✅ R²:    {r2:.6f}")
    
    # Interpretation
    print(f"\n   🧠 Performance interpretation:")
    if r2 >= 0.40:
        print(f"      🔥 EXCELLENT! R²={r2:.3f} - Strong predictive power")
    elif r2 >= 0.30:
        print(f"      ✅ VERY GOOD! R²={r2:.3f} - Solid model for noisy geopolitics")
    elif r2 >= 0.20:
        print(f"      👍 GOOD! R²={r2:.3f} - Acceptable for complex geopolitical data")
    else:
        print(f"      ⚠️  MODERATE R²={r2:.3f} - Geopolitics is very noisy")
    
    return {'MAE': mae, 'RMSE': rmse, 'R2': r2}, y_pred


def print_feature_importance(model, X, top_n=10):
    """
    Print and analyze feature importance.
    """
    print("\n📈 Top Feature Importances:")
    print("="*70)
    
    # Get feature names and importances
    feature_names = X.columns
    importances = model.feature_importances_
    
    # Create DataFrame and sort
    importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': importances
    }).sort_values('Importance', ascending=False)
    
    # Print top N
    print(f"\n🏆 TOP {top_n} MOST IMPORTANT FEATURES:")
    for i, row in importance_df.head(top_n).iterrows():
        bar = '█' * int(row['Importance'] * 100)
        print(f"   {i+1:2d}. {row['Feature']:30s} {bar} {row['Importance']:.4f}")
    
    print("="*70)
    
    return importance_df


# ================================================================
# 💾 SAVE MODEL
# ================================================================

def save_model(model, output_path):
    """Save trained model to disk."""
    print("\n💾 Saving model...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save with joblib
    joblib.dump(model, output_path)
    
    print(f"   ✅ Model saved to {output_path}")


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function for Phase 5."""
    print("\n🚀 Starting Geopolitics Model Training (Phase 5)...")
    print("="*70)
    
    # Step 1: Load data
    df = load_data(INPUT_FILE)
    
    # Step 2: Prepare features
    X, y = prepare_features(df)
    
    # Step 3: Time-based split
    X_train, X_test, y_train, y_test = time_based_split(X, y, df, train_end_year=2019)
    
    # Step 4: Train model
    model = train_xgboost_model(X_train, y_train)
    
    # Step 5: Evaluate model
    metrics, y_pred = evaluate_model(model, X_test, y_test)
    
    # Step 6: Feature importance
    importance_df = print_feature_importance(model, X)
    
    # Step 7: Save model
    save_model(model, OUTPUT_MODEL)
    
    # Final summary
    print("\n" + "="*70)
    print("🎉 PHASE 5 COMPLETE - MODEL TRAINING")
    print("="*70)
    
    print(f"\n📊 FINAL METRICS:")
    print(f"   MAE:  {metrics['MAE']:.6f}")
    print(f"   RMSE: {metrics['RMSE']:.6f}")
    print(f"   R²:   {metrics['R2']:.6f}")
    
    print(f"\n💾 MODEL SAVED:")
    print(f"   {OUTPUT_MODEL}")
    
    print(f"\n📈 FEATURE STATISTICS:")
    print(f"   Total features: {len(importance_df)}")
    print(f"   Top feature: {importance_df.iloc[0]['Feature']} ({importance_df.iloc[0]['Importance']:.4f})")
    
    print("\n✅ Phase 5 completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error during Phase 5: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
