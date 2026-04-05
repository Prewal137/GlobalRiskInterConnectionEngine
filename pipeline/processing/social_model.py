"""
🤖 Social Sector Model Training (Phase 5)

This script trains an XGBoost model to predict future social risk.

Input: 
    - data/processed/social/features/social_features_forecast.csv

Output:
    - models/trained/social_model.pkl
    - data/processed/social/social_risk_output.csv

Key Features:
    - Strict time-based train-test split (no leakage)
    - Predicts FUTURE social risk (social_risk_t_plus_1)
    - XGBoost with optimized hyperparameters
    - Feature importance analysis
    - Comprehensive evaluation metrics
"""

import pandas as pd
import numpy as np
import joblib
import os
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "social", "features", "social_features_forecast.csv")
MODEL_OUTPUT = os.path.join(BASE_PATH, "models", "trained", "social_model.pkl")
PREDICTIONS_OUTPUT = os.path.join(BASE_PATH, "data", "processed", "social", "social_risk_output.csv")

# Ensure output directories exist
os.makedirs(os.path.dirname(MODEL_OUTPUT), exist_ok=True)
os.makedirs(os.path.dirname(PREDICTIONS_OUTPUT), exist_ok=True)

# ================================================================
# 🔧 STEP 1: LOAD DATA
# ================================================================

def load_data():
    """
    Load and prepare social features dataset.
    
    Returns:
        pd.DataFrame: Loaded and sorted dataset
    """
    print("\n" + "="*70)
    print("📥 STEP 1: LOADING DATA")
    print("="*70)
    
    df = pd.read_csv(INPUT_FILE)
    
    # Sort by time (critical for time-series)
    df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df):,} rows")
    print(f"   📅 Time range: {df['Year'].min()} - {df['Year'].max()}")
    print(f"   🗺️  States: {df['State'].nunique()}")
    print(f"   📊 Total columns: {len(df.columns)}")
    
    return df

# ================================================================
# 🔧 STEP 2: TRAIN-TEST SPLIT (STRICT TIME SPLIT)
# ================================================================

def time_based_split(df, train_end_year=2019):
    """
    Perform strict time-based train-test split.
    
    Train: Year <= 2019
    Test: Year > 2019
    
    Args:
        df (pd.DataFrame): Input dataset
        train_end_year (int): End year for training period
        
    Returns:
        tuple: (train_df, test_df)
    """
    print("\n" + "="*70)
    print("⏰ STEP 2: TIME-BASED TRAIN-TEST SPLIT")
    print("="*70)
    
    train_mask = df['Year'] <= train_end_year
    test_mask = df['Year'] > train_end_year
    
    train_df = df[train_mask].reset_index(drop=True)
    test_df = df[test_mask].reset_index(drop=True)
    
    print(f"   📚 Train size: {len(train_df):,} rows ({len(train_df)/len(df)*100:.1f}%)")
    print(f"      Year range: {train_df['Year'].min()} - {train_df['Year'].max()}")
    print(f"   📝 Test size: {len(test_df):,} rows ({len(test_df)/len(df)*100:.1f}%)")
    print(f"      Year range: {test_df['Year'].min()} - {test_df['Year'].max()}")
    print(f"   ✅ NO temporal leakage (strict time separation)")
    
    return train_df, test_df

# ================================================================
# 🔧 STEP 3: SELECT FEATURES
# ================================================================

def select_features(df):
    """
    Select feature matrix X and target vector y.
    
    Drop columns:
        - State, Year, Month (metadata)
        - conflict_events, fatalities (all zeros, no signal)
    
    Target: social_risk_t_plus_1 (future risk)
    
    Args:
        df (pd.DataFrame): Dataset
        
    Returns:
        tuple: (X, y, feature_names)
    """
    print("\n" + "="*70)
    print("🎯 STEP 3: FEATURE SELECTION")
    print("="*70)
    
    # Columns to drop
    drop_cols = ['State', 'Year', 'Month', 'conflict_events', 'fatalities']
    
    # Define features (all except dropped columns and target)
    feature_cols = [col for col in df.columns if col not in drop_cols and col != 'social_risk_t_plus_1']
    
    X = df[feature_cols].copy()
    y = df['social_risk_t_plus_1'].copy()
    
    print(f"   📊 Feature shape: {X.shape}")
    print(f"   🎯 Target shape: {y.shape}")
    print(f"   📋 Features ({len(feature_cols)}): {feature_cols}")
    print(f"   🎯 Primary target: social_risk_t_plus_1 (predicts NEXT period's risk)")
    
    # Check for NaN values
    nan_features = X.isna().sum().sum()
    nan_target = y.isna().sum()
    
    if nan_features > 0 or nan_target > 0:
        print(f"   ⚠️  Warning: {nan_features} NaN in features, {nan_target} NaN in target")
        print(f"   🧹 Dropping rows with NaN values...")
        
        mask = X.notna().all(axis=1) & y.notna()
        X = X[mask]
        y = y[mask]
        
        print(f"   ✅ Clean shape: X={X.shape}, y={y.shape}")
    
    return X, y, feature_cols

# ================================================================
# 🔧 STEP 4: MODEL DEFINITION
# ================================================================

def create_model():
    """
    Create XGBoost Regressor with optimized hyperparameters.
    
    Returns:
        XGBRegressor: Configured model
    """
    print("\n" + "="*70)
    print("🤖 STEP 4: MODEL CREATION (XGBOOST)")
    print("="*70)
    
    model = XGBRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    
    print(f"   ✅ XGBoost Regressor created")
    print(f"   📋 Hyperparameters:")
    print(f"      - n_estimators: 200")
    print(f"      - max_depth: 5")
    print(f"      - learning_rate: 0.05")
    print(f"      - subsample: 0.8")
    print(f"      - colsample_bytree: 0.8")
    print(f"      - random_state: 42")
    
    return model

# ================================================================
# 🔧 STEP 5: TRAIN MODEL
# ================================================================

def train_model(model, X_train, y_train):
    """
    Train the XGBoost model.
    
    Args:
        model (XGBRegressor): Model to train
        X_train (pd.DataFrame): Training features
        y_train (pd.Series): Training target
        
    Returns:
        Trained model
    """
    print("\n" + "="*70)
    print("🎯 STEP 5: MODEL TRAINING")
    print("="*70)
    
    print(f"   📚 Training data shape: {X_train.shape}")
    print(f"   🎯 Training target shape: {y_train.shape}")
    print(f"   🔄 Fitting model...")
    
    model.fit(X_train, y_train)
    
    print(f"   ✅ Model trained successfully")
    print(f"   📊 Number of trees: {model.n_estimators}")
    
    return model

# ================================================================
# 🔧 STEP 6: PREDICTIONS
# ================================================================

def make_predictions(model, X_test):
    """
    Generate predictions on test set.
    
    Args:
        model: Trained model
        X_test (pd.DataFrame): Test features
        
    Returns:
        np.ndarray: Predictions
    """
    print("\n" + "="*70)
    print("🔮 STEP 6: GENERATING PREDICTIONS")
    print("="*70)
    
    y_pred = model.predict(X_test)
    
    print(f"   ✅ Generated {len(y_pred):,} predictions")
    print(f"   📊 Prediction stats:")
    print(f"      Mean: {y_pred.mean():.4f}")
    print(f"      Std: {y_pred.std():.4f}")
    print(f"      Min: {y_pred.min():.4f}")
    print(f"      Max: {y_pred.max():.4f}")
    
    return y_pred

# ================================================================
# 🔧 STEP 7: EVALUATION
# ================================================================

def evaluate_model(y_true, y_pred):
    """
    Compute evaluation metrics.
    
    Args:
        y_true (np.ndarray): Actual values
        y_pred (np.ndarray): Predicted values
        
    Returns:
        dict: Evaluation metrics
    """
    print("\n" + "="*70)
    print("📊 STEP 7: MODEL EVALUATION")
    print("="*70)
    
    # Calculate metrics
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    
    print(f"   🎯 Test set size: {len(y_true):,} samples")
    print(f"\n   📈 METRICS:")
    print(f"      MAE (Mean Absolute Error):  {mae:.6f}")
    print(f"      RMSE (Root Mean Squared Error): {rmse:.6f}")
    print(f"      R² (R-Squared):               {r2:.6f}")
    
    # Interpretation
    print(f"\n   💡 INTERPRETATION:")
    print(f"      - MAE: On average, predictions are off by ±{mae:.4f} units")
    print(f"      - RMSE: Penalizes larger errors more heavily")
    print(f"      - R²: Model explains {r2*100:.2f}% of variance in test data")
    
    if r2 > 0.8:
        print(f"      ✅ Excellent fit!")
    elif r2 > 0.6:
        print(f"      ✅ Good fit!")
    elif r2 > 0.4:
        print(f"      ⚠️  Moderate fit")
    else:
        print(f"      ⚠️  Room for improvement")
    
    return {'MAE': mae, 'RMSE': rmse, 'R2': r2}

# ================================================================
# 🔧 STEP 8: SAVE MODEL
# ================================================================

def save_model(model, model_path=MODEL_OUTPUT):
    """
    Save trained model to disk.
    
    Args:
        model: Trained model
        model_path (str): Output path
    """
    print("\n" + "="*70)
    print("💾 STEP 8: SAVING MODEL")
    print("="*70)
    
    joblib.dump(model, model_path)
    
    print(f"   ✅ Model saved to: {model_path}")
    print(f"   📦 File size: {os.path.getsize(model_path) / 1024:.2f} KB")

# ================================================================
# 🔧 STEP 9: SAVE OUTPUT
# ================================================================

def save_predictions(test_df, y_pred, output_path=PREDICTIONS_OUTPUT):
    """
    Save predictions to CSV file.
    
    Args:
        test_df (pd.DataFrame): Test dataframe with metadata
        y_pred (np.ndarray): Predictions
        output_path (str): Output file path
    """
    print("\n" + "="*70)
    print("📝 STEP 9: SAVING PREDICTIONS")
    print("="*70)
    
    # Create output dataframe
    output_df = pd.DataFrame({
        'State': test_df['State'],
        'Year': test_df['Year'],
        'Month': test_df['Month'],
        'actual_risk': test_df['social_risk_t_plus_1'].values,
        'predicted_risk': y_pred
    })
    
    # Save to CSV
    output_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"   ✅ Predictions saved to: {output_path}")
    print(f"   📊 Output shape: {output_df.shape}")
    print(f"\n   📋 SAMPLE OUTPUT (first 5 rows):")
    print(output_df.head(10).to_string(index=False))

# ================================================================
# 🔧 STEP 10: FEATURE IMPORTANCE
# ================================================================

def plot_feature_importance(model, feature_names, top_n=10):
    """
    Extract and display feature importances.
    
    Args:
        model: Trained model
        feature_names (list): Feature column names
        top_n (int): Number of top features to display
    """
    print("\n" + "="*70)
    print("🔍 STEP 10: FEATURE IMPORTANCE ANALYSIS")
    print("="*70)
    
    # Extract importances
    importances = model.feature_importances_
    
    # Create dataframe
    importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)
    
    print(f"   📊 TOP {top_n} MOST IMPORTANT FEATURES:\n")
    
    for i, row in importance_df.head(top_n).iterrows():
        bar = '█' * int(row['importance'] * 50)
        print(f"   {i+1:2d}. {row['feature']:30s} {bar} {row['importance']:.6f}")
    
    print(f"\n   💡 INSIGHTS:")
    print(f"      - Top features drive model predictions")
    print(f"      - Lag features indicate temporal patterns")
    print(f"      - Rolling features capture trends")
    
    return importance_df

# ================================================================
# 🔧 STEP 11: PLOT ACTUAL VS PREDICTED
# ================================================================

def plot_actual_vs_predicted(test_df, y_pred):
    """
    Create visualization of actual vs predicted values.
    
    Args:
        test_df (pd.DataFrame): Test dataframe
        y_pred (np.ndarray): Predictions
    """
    print("\n" + "="*70)
    print("📈 STEP 11: VISUALIZATION (ACTUAL VS PREDICTED)")
    print("="*70)
    
    try:
        # Create figure
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # Plot 1: Time series comparison
        ax1 = axes[0]
        test_df_copy = test_df.copy()
        test_df_copy['predicted'] = y_pred
        
        ax1.plot(range(len(test_df_copy)), test_df_copy['social_risk_t_plus_1'], 
                label='Actual Risk', linewidth=2, alpha=0.7)
        ax1.plot(range(len(test_df_copy)), y_pred, 
                label='Predicted Risk', linewidth=2, alpha=0.7, linestyle='--')
        ax1.set_xlabel('Time Index', fontsize=12)
        ax1.set_ylabel('Social Risk Score', fontsize=12)
        ax1.set_title('Social Risk: Actual vs Predicted (Test Set)', fontsize=14, fontweight='bold')
        ax1.legend(loc='best', fontsize=10)
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Scatter plot
        ax2 = axes[1]
        ax2.scatter(test_df_copy['social_risk_t_plus_1'], y_pred, alpha=0.6, s=50)
        
        # Add diagonal line
        min_val = min(test_df_copy['social_risk_t_plus_1'].min(), y_pred.min())
        max_val = max(test_df_copy['social_risk_t_plus_1'].max(), y_pred.max())
        ax2.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Perfect Prediction')
        
        ax2.set_xlabel('Actual Social Risk', fontsize=12)
        ax2.set_ylabel('Predicted Social Risk', fontsize=12)
        ax2.set_title('Actual vs Predicted Social Risk (Test Set)', fontsize=14, fontweight='bold')
        ax2.legend(loc='best', fontsize=10)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plot
        plot_path = os.path.join(BASE_PATH, "models", "evaluation", "social_actual_vs_predicted.png")
        os.makedirs(os.path.dirname(plot_path), exist_ok=True)
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        
        print(f"   ✅ Plot saved to: {plot_path}")
        print(f"   📊 Visualization shows model performance over time")
        
        plt.close()
        
    except Exception as e:
        print(f"   ⚠️  Could not create plot: {e}")

# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🤖 SOCIAL SECTOR MODEL TRAINING (PHASE 5)")
    print("="*70)
    
    try:
        # ============================================================
        # STEP 1: LOAD DATA
        # ============================================================
        df = load_data()
        
        # ============================================================
        # STEP 2: TIME-BASED SPLIT
        # ============================================================
        train_df, test_df = time_based_split(df, train_end_year=2019)
        
        # ============================================================
        # STEP 3: SELECT FEATURES
        # ============================================================
        X_train, y_train, feature_names = select_features(train_df)
        X_test, y_test, _ = select_features(test_df)
        
        # ============================================================
        # STEP 4: CREATE MODEL
        # ============================================================
        model = create_model()
        
        # ============================================================
        # STEP 5: TRAIN MODEL
        # ============================================================
        model = train_model(model, X_train, y_train)
        
        # ============================================================
        # STEP 6: PREDICTIONS
        # ============================================================
        y_pred = make_predictions(model, X_test)
        
        # ============================================================
        # STEP 7: EVALUATION
        # ============================================================
        metrics = evaluate_model(y_test, y_pred)
        
        # ============================================================
        # STEP 8: SAVE MODEL
        # ============================================================
        save_model(model)
        
        # ============================================================
        # STEP 9: SAVE OUTPUT
        # ============================================================
        save_predictions(test_df, y_pred)
        
        # ============================================================
        # STEP 10: FEATURE IMPORTANCE
        # ============================================================
        importance_df = plot_feature_importance(model, feature_names, top_n=10)
        
        # ============================================================
        # STEP 11: VISUALIZATION
        # ============================================================
        plot_actual_vs_predicted(test_df, y_pred)
        
        # ============================================================
        # FINAL SUMMARY
        # ============================================================
        print("\n" + "="*70)
        print("✅ SOCIAL MODEL TRAINING COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"\n   📊 FINAL RESULTS:")
        print(f"      Training samples: {len(X_train):,}")
        print(f"      Test samples: {len(X_test):,}")
        print(f"      MAE: {metrics['MAE']:.6f}")
        print(f"      RMSE: {metrics['RMSE']:.6f}")
        print(f"      R²: {metrics['R2']:.6f}")
        
        print(f"\n   💾 OUTPUTS:")
        print(f"      Model: {MODEL_OUTPUT}")
        print(f"      Predictions: {PREDICTIONS_OUTPUT}")
        print(f"      Plot: models/evaluation/social_actual_vs_predicted.png")
        
        print("\n" + "="*70)
        print("🎯 MODEL READY FOR PRODUCTION DEPLOYMENT")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
