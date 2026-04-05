"""
🤖 Infrastructure Sector Model Training (Phase 5)

Trains ML models to predict infrastructure risk.

Input:  data/processed/infrastructure/infrastructure_features.csv
Output: models/trained/infrastructure_model.pkl
        models/trained/infrastructure_scaler.pkl
        data/processed/infrastructure/infrastructure_risk_output.csv

Models Tested:
    - Random Forest Regressor
    - XGBoost Regressor

Selection: Best model based on R² score
"""

import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.preprocessing import StandardScaler
import joblib
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 3 levels from pipeline/processing/infrastructure/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_features.csv")
MODEL_OUTPUT = os.path.join(BASE_PATH, "models", "trained", "infrastructure_model.pkl")
SCALER_OUTPUT = os.path.join(BASE_PATH, "models", "trained", "infrastructure_scaler.pkl")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_risk_output.csv")

# Ensure output directories exist
os.makedirs(os.path.dirname(MODEL_OUTPUT), exist_ok=True)
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

print("="*70)
print("🤖 INFRASTRUCTURE SECTOR MODEL TRAINING")
print("="*70)
print(f"\n📂 Loading features from: {INPUT_FILE}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def load_data():
    """Load infrastructure features dataset."""
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    df = pd.read_csv(INPUT_FILE)
    print(f"   ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"   📅 Year range: {df['year'].min()} - {df['year'].max()}")
    print(f"   🗺️  States: {df['state'].nunique()}")
    
    return df


def select_features(df):
    """
    Select feature matrix X and target vector y.
    
    Drops:
    - country (constant)
    - state (identifier)
    - year (used for split, not feature)
    
    Target:
    - infrastructure_risk
    """
    print("\n" + "="*70)
    print("🔍 STEP 1: SELECTING FEATURES")
    print("="*70)
    
    # Columns to drop
    drop_cols = ['country', 'state', 'year']
    
    # Define features (all except dropped columns and target)
    feature_cols = [col for col in df.columns if col not in drop_cols and col != 'infrastructure_risk']
    
    X = df[feature_cols].copy()
    y = df['infrastructure_risk'].copy()
    
    print(f"   Features: {len(feature_cols)} columns")
    print(f"   Target: infrastructure_risk")
    print(f"   Feature columns:")
    for i, col in enumerate(feature_cols, 1):
        print(f"      {i:2d}. {col}")
    
    return X, y, feature_cols


def time_based_split(df, train_end_year=2019):
    """
    Split data by time period.
    
    Train: years <= train_end_year
    Test: years > train_end_year
    
    This prevents data leakage and simulates real-world prediction.
    """
    print("\n" + "="*70)
    print("✂️  STEP 2: TIME-BASED TRAIN-TEST SPLIT")
    print("="*70)
    
    train_df = df[df['year'] <= train_end_year].copy()
    test_df = df[df['year'] > train_end_year].copy()
    
    print(f"   Training period: {df['year'].min()} - {train_end_year}")
    print(f"   Testing period: {train_end_year + 1} - {df['year'].max()}")
    print(f"   Training samples: {len(train_df)}")
    print(f"   Testing samples: {len(test_df)}")
    print(f"   Split ratio: {len(train_df)/len(df)*100:.1f}% / {len(test_df)/len(df)*100:.1f}%")
    
    return train_df, test_df


def scale_features(X_train, X_test):
    """
    Scale features using StandardScaler.
    
    Fit on training data only to prevent data leakage.
    """
    print("\n" + "="*70)
    print("📏 STEP 3: SCALING FEATURES")
    print("="*70)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print(f"   ✅ Scaler fitted on {X_train.shape[1]} features")
    print(f"   Training shape: {X_train_scaled.shape}")
    print(f"   Testing shape: {X_test_scaled.shape}")
    
    return X_train_scaled, X_test_scaled, scaler


def create_models():
    """Create Random Forest and XGBoost models."""
    print("\n" + "="*70)
    print("🏗️  STEP 4: CREATING MODELS")
    print("="*70)
    
    # Random Forest
    rf = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    
    # XGBoost
    xgb = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    
    print(f"   ✅ Random Forest: {rf.n_estimators} trees, max_depth={rf.max_depth}")
    print(f"   ✅ XGBoost: {xgb.n_estimators} estimators, lr={xgb.learning_rate}, max_depth={xgb.max_depth}")
    
    return rf, xgb


def train_and_evaluate(model, X_train, y_train, X_test, y_test, model_name):
    """Train a model and evaluate performance."""
    print(f"\n   🔄 Training {model_name}...")
    
    # Train
    model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # Evaluate
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    print(f"   ✅ {model_name} trained successfully")
    print(f"      MAE:  {mae:.4f}")
    print(f"      RMSE: {rmse:.4f}")
    print(f"      R²:   {r2:.4f}")
    
    return model, y_pred, {'MAE': mae, 'RMSE': rmse, 'R2': r2}


def plot_feature_importance(model, feature_names, model_name):
    """Plot feature importance if available."""
    if not hasattr(model, 'feature_importances_'):
        print(f"   ⚠️  {model_name} does not support feature importance")
        return
    
    print(f"\n   📊 {model_name} Feature Importance:")
    print(f"   {'-'*70}")
    
    # Get importance
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    
    # Print top 10
    print(f"   Top 10 Important Features:")
    for i, idx in enumerate(indices[:10], 1):
        print(f"      {i:2d}. {feature_names[idx]:<40s}: {importances[idx]:.4f}")
    
    # Plot if matplotlib is available
    try:
        plt.figure(figsize=(12, 6))
        top_n = min(15, len(feature_names))
        top_indices = indices[:top_n]
        
        plt.barh(range(top_n), importances[top_indices][::-1])
        plt.yticks(range(top_n), [feature_names[i] for i in top_indices[::-1]])
        plt.xlabel('Importance')
        plt.title(f'{model_name} - Top {top_n} Feature Importances')
        plt.tight_layout()
        
        plot_path = os.path.join(BASE_PATH, "models", "evaluation", "infrastructure_feature_importance.png")
        os.makedirs(os.path.dirname(plot_path), exist_ok=True)
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"   📈 Plot saved to: {plot_path}")
    except Exception as e:
        print(f"   ⚠️  Could not save plot: {e}")


def generate_predictions(df, X, model, scaler):
    """Generate predictions for entire dataset."""
    print("\n" + "="*70)
    print("🔮 STEP 6: GENERATING PREDICTIONS")
    print("="*70)
    
    # Scale features
    X_scaled = scaler.transform(X)
    
    # Predict
    predictions = model.predict(X_scaled)
    
    # Normalize predictions to [0, 1]
    pred_min = predictions.min()
    pred_max = predictions.max()
    
    if pred_max > pred_min:
        normalized_preds = (predictions - pred_min) / (pred_max - pred_min)
    else:
        normalized_preds = predictions
    
    print(f"   ✅ Generated {len(predictions)} predictions")
    print(f"   Raw range: [{pred_min:.4f}, {pred_max:.4f}]")
    print(f"   Normalized range: [{normalized_preds.min():.4f}, {normalized_preds.max():.4f}]")
    
    return normalized_preds


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main function to train infrastructure risk model."""
    
    # Step 1: Load data
    print("\n📥 Loading infrastructure features...")
    df = load_data()
    
    # Step 2: Select features
    X, y, feature_names = select_features(df)
    
    # Step 3: Time-based split
    train_df, test_df = time_based_split(df, train_end_year=2019)
    
    # Get train/test sets
    X_train = train_df[feature_names]
    y_train = train_df['infrastructure_risk']
    X_test = test_df[feature_names]
    y_test = test_df['infrastructure_risk']
    
    print(f"\n   After removing NaN:")
    print(f"      Train: {len(X_train)} samples")
    print(f"      Test: {len(X_test)} samples")
    
    # Check for NaN in features
    if X_train.isna().any().any() or X_test.isna().any().any():
        print(f"\n   ⚠️  Warning: NaN values detected in features")
        print(f"      Filling with median...")
        X_train = X_train.fillna(X_train.median())
        X_test = X_test.fillna(X_train.median())
    
    # Step 4: Scale features
    X_train_scaled, X_test_scaled, scaler = scale_features(X_train, X_test)
    
    # Step 5: Create models
    rf, xgb = create_models()
    
    # Step 6: Train and evaluate both models
    print("\n" + "="*70)
    print("🎓 STEP 5: TRAINING AND EVALUATION")
    print("="*70)
    
    rf_model, rf_preds, rf_metrics = train_and_evaluate(
        rf, X_train_scaled, y_train, X_test_scaled, y_test, "Random Forest"
    )
    
    xgb_model, xgb_preds, xgb_metrics = train_and_evaluate(
        xgb, X_train_scaled, y_train, X_test_scaled, y_test, "XGBoost"
    )
    
    # Step 7: Compare models
    print("\n" + "="*70)
    print("📊 MODEL COMPARISON")
    print("="*70)
    print(f"   {'Metric':<15s} {'Random Forest':<15s} {'XGBoost':<15s}")
    print(f"   {'-'*50}")
    print(f"   {'MAE':<15s} {rf_metrics['MAE']:<15.4f} {xgb_metrics['MAE']:<15.4f}")
    print(f"   {'RMSE':<15s} {rf_metrics['RMSE']:<15.4f} {xgb_metrics['RMSE']:<15.4f}")
    print(f"   {'R² Score':<15s} {rf_metrics['R2']:<15.4f} {xgb_metrics['R2']:<15.4f}")
    
    # Select best model
    if xgb_metrics['R2'] >= rf_metrics['R2']:
        best_model = xgb_model
        best_name = "XGBoost"
        best_metrics = xgb_metrics
        print(f"\n   🏆 Winner: XGBoost (higher R²)")
    else:
        best_model = rf_model
        best_name = "Random Forest"
        best_metrics = rf_metrics
        print(f"\n   🏆 Winner: Random Forest (higher R²)")
    
    # Step 8: Feature importance
    print("\n" + "="*70)
    print("📈 FEATURE IMPORTANCE ANALYSIS")
    print("="*70)
    plot_feature_importance(best_model, feature_names, best_name)
    
    # Step 9: Save model and scaler
    print("\n" + "="*70)
    print("💾 SAVING MODEL AND SCALER")
    print("="*70)
    
    joblib.dump(best_model, MODEL_OUTPUT)
    joblib.dump(scaler, SCALER_OUTPUT)
    
    model_size = os.path.getsize(MODEL_OUTPUT) / 1024
    scaler_size = os.path.getsize(SCALER_OUTPUT) / 1024
    
    print(f"   ✅ Model saved: {MODEL_OUTPUT} ({model_size:.2f} KB)")
    print(f"   ✅ Scaler saved: {SCALER_OUTPUT} ({scaler_size:.2f} KB)")
    
    # Step 10: Generate predictions for entire dataset
    predictions = generate_predictions(df, X, best_model, scaler)
    
    # Add predictions to dataframe
    df['predicted_risk'] = predictions
    
    # Step 11: Save output
    print("\n" + "="*70)
    print("💾 SAVING PREDICTIONS")
    print("="*70)
    
    # Select output columns
    output_cols = ['country', 'state', 'year', 'infrastructure_risk', 'predicted_risk']
    output_df = df[output_cols].copy()
    
    output_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Rows: {len(output_df)}")
    
    # Step 12: Final summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY")
    print("="*70)
    print(f"   Best model: {best_name}")
    print(f"   MAE:  {best_metrics['MAE']:.4f}")
    print(f"   RMSE: {best_metrics['RMSE']:.4f}")
    print(f"   R²:   {best_metrics['R2']:.4f}")
    print(f"   Training samples: {len(X_train)}")
    print(f"   Testing samples: {len(X_test)}")
    
    # Sample predictions
    print(f"\n   📋 SAMPLE PREDICTIONS (first 10 rows):")
    print(f"   {'-'*100}")
    print(output_df.head(10).to_string(index=False))
    print(f"   {'-'*100}")
    
    # Prediction statistics
    print(f"\n   📈 PREDICTION STATISTICS:")
    print(f"      Mean predicted risk: {output_df['predicted_risk'].mean():.4f}")
    print(f"      Std: {output_df['predicted_risk'].std():.4f}")
    print(f"      Min: {output_df['predicted_risk'].min():.4f}")
    print(f"      Max: {output_df['predicted_risk'].max():.4f}")
    
    # Actual vs Predicted correlation
    correlation = output_df['infrastructure_risk'].corr(output_df['predicted_risk'])
    print(f"      Correlation (actual vs predicted): {correlation:.4f}")
    
    print(f"\n" + "="*70)
    print(f"✅ INFRASTRUCTURE MODEL TRAINING COMPLETE")
    print("="*70)
    print(f"\n📁 Next steps:")
    print(f"   1. Review predictions: {OUTPUT_FILE}")
    print(f"   2. Proceed to Phase 6: API Creation")
    print(f"   3. Create backend/app/routes/infrastructure.py")
    print(f"\n" + "="*70)


# ================================================================
# 🚀 RUN
# ================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
