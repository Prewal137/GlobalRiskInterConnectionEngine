"""
🚀 Economy Model Improvement - Enhanced Pipeline

This script implements advanced optimizations to improve model performance:
1. Enhanced feature engineering (more lags, trends, interactions)
2. Improved XGBoost with regularization
3. RandomForest ensemble
4. TimeSeriesSplit validation
5. Comprehensive comparison

Input: data/processed/economy/economy_features.csv
Output: models/trained/economy_model_v2.pkl
        data/processed/economy/economic_risk_output_v2.csv
"""

import pandas as pd
import numpy as np
import joblib
import os
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "economy_features.csv")
SCALER_PATH = os.path.join(BASE_PATH, "models", "trained", "economy_scaler.pkl")
MODEL_OUTPUT = os.path.join(BASE_PATH, "models", "trained", "economy_model_v2.pkl")
PREDICTIONS_OUTPUT = os.path.join(BASE_PATH, "data", "processed", "economy", "economic_risk_output_v2.csv")
COMPARISON_OUTPUT = os.path.join(BASE_PATH, "data", "processed", "economy", "model_comparison.csv")

os.makedirs(os.path.dirname(MODEL_OUTPUT), exist_ok=True)
os.makedirs(os.path.dirname(PREDICTIONS_OUTPUT), exist_ok=True)

# ================================================================
# 🔧 ENHANCED FEATURE ENGINEERING
# ================================================================

def create_enhanced_features(df):
    """
    Create enhanced features for better prediction:
    1. Additional lag features (lag_3, lag_6, lag_12)
    2. Rolling trend features (6-month)
    3. Interaction features
    4. Shock detection features
    """
    print("\n" + "="*70)
    print("🔧 ENHANCED FEATURE ENGINEERING")
    print("="*70)
    
    df = df.copy()
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    # ========================================
    # 1. ADDITIONAL LAG FEATURES (VERY IMPORTANT)
    # ========================================
    print("\n   📊 Adding extended lag features...")
    
    key_vars = ['Inflation', 'InterestRate', 'NIFTY50', 'VIX', 'ExchangeRate']
    
    for var in key_vars:
        # Lag 3 (quarterly)
        df[f'{var.lower()}_lag_3'] = df.groupby('Country')[var].shift(3)
        
        # Lag 6 (semi-annual)
        df[f'{var.lower()}_lag_6'] = df.groupby('Country')[var].shift(6)
        
        # Lag 12 (annual - captures seasonality)
        df[f'{var.lower()}_lag_12'] = df.groupby('Country')[var].shift(12)
    
    print(f"      ✅ Added lag_3, lag_6, lag_12 for {len(key_vars)} variables")
    print(f"      Total new lag features: {len(key_vars) * 3}")
    
    # ========================================
    # 2. ROLLING TREND FEATURES
    # ========================================
    print("\n   📈 Adding rolling trend features...")
    
    # 6-month trends (medium-term)
    df['nifty_trend_6'] = df.groupby('Country')['NIFTY50'].transform(
        lambda x: x.rolling(window=6, min_periods=1).mean()
    )
    df['inflation_trend_6'] = df.groupby('Country')['Inflation'].transform(
        lambda x: x.rolling(window=6, min_periods=1).mean()
    )
    df['vix_trend_6'] = df.groupby('Country')['VIX'].transform(
        lambda x: x.rolling(window=6, min_periods=1).mean()
    )
    
    # 12-month trends (long-term)
    df['nifty_trend_12'] = df.groupby('Country')['NIFTY50'].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean()
    )
    df['inflation_trend_12'] = df.groupby('Country')['Inflation'].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean()
    )
    
    print(f"      ✅ Added 6-month and 12-month trends")
    
    # ========================================
    # 3. INTERACTION FEATURES (DOMAIN KNOWLEDGE)
    # ========================================
    print("\n   🔗 Adding interaction features...")
    
    # Inflation × Interest Rate (stagflation pressure)
    df['inflation_x_interest'] = df['Inflation'] * df['InterestRate']
    
    # NIFTY × VIX (market stress interaction)
    df['nifty_x_vix'] = df['NIFTY50'] * df['VIX']
    
    # Exchange Rate × Inflation (imported inflation)
    df['exchange_x_inflation'] = df['ExchangeRate'] * df['Inflation']
    
    # VIX / NIFTY (normalized market fear)
    df['vix_normalized'] = df['VIX'] / df['NIFTY50']
    
    print(f"      ✅ Added 4 interaction features")
    
    # ========================================
    # 4. SHOCK DETECTION FEATURES
    # ========================================
    print("\n   ⚡ Adding shock detection features...")
    
    # Market shock indicator (VIX > 12-month rolling mean)
    vix_rolling_mean = df.groupby('Country')['VIX'].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean()
    )
    df['market_shock'] = (df['VIX'] > vix_rolling_mean).astype(int)
    
    # Volatility spike (VIX change > 2 std dev)
    vix_std = df.groupby('Country')['VIX'].transform(
        lambda x: x.rolling(window=12, min_periods=1).std()
    )
    df['volatility_spike'] = ((df['VIX'].diff() > 2 * vix_std)).astype(int)
    
    # Inflation shock
    inflation_rolling_mean = df.groupby('Country')['Inflation'].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean()
    )
    df['inflation_shock'] = (df['Inflation'] > inflation_rolling_mean * 1.2).astype(int)
    
    print(f"      ✅ Added 3 shock indicators")
    
    # ========================================
    # 5. MOMENTUM FEATURES
    # ========================================
    print("\n   🚀 Adding momentum features...")
    
    # NIFTY momentum (3-month return)
    df['nifty_momentum_3'] = df.groupby('Country')['NIFTY50'].pct_change(periods=3)
    
    # VIX momentum
    df['vix_momentum_3'] = df.groupby('Country')['VIX'].pct_change(periods=3)
    
    print(f"      ✅ Added momentum features")
    
    # ========================================
    # SUMMARY
    # ========================================
    original_features = 21
    new_features = len(df.columns) - 3 - 1  # Exclude Country, Year, Month, target
    
    print(f"\n   📊 FEATURE EXPANSION:")
    print(f"      Original features: {original_features}")
    print(f"      New features added: {new_features - original_features}")
    print(f"      Total features: {new_features}")
    
    return df

# ================================================================
# 🔧 IMPROVED MODEL TRAINING
# ================================================================

def train_improved_models(X_train, y_train, X_test, y_test):
    """
    Train improved models with:
    1. Regularized XGBoost
    2. RandomForest
    3. Ensemble (0.6 XGB + 0.4 RF)
    """
    print("\n" + "="*70)
    print("🤖 TRAINING IMPROVED MODELS")
    print("="*70)
    
    results = {}
    
    # ========================================
    # MODEL 1: IMPROVED XGBOOST WITH REGULARIZATION
    # ========================================
    print("\n   🎯 Training XGBoost with regularization...")
    
    xgb_model = XGBRegressor(
        n_estimators=300,
        max_depth=3,              # Reduced complexity
        learning_rate=0.03,       # Slower learning
        subsample=0.7,            # More randomness
        colsample_bytree=0.7,     # Feature sampling
        reg_alpha=1.0,            # L1 regularization 🔥
        reg_lambda=1.5,           # L2 regularization 🔥
        random_state=42,
        n_jobs=-1
    )
    
    xgb_model.fit(X_train, y_train)
    xgb_pred = xgb_model.predict(X_test)
    
    xgb_mae = mean_absolute_error(y_test, xgb_pred)
    xgb_rmse = np.sqrt(mean_squared_error(y_test, xgb_pred))
    xgb_r2 = r2_score(y_test, xgb_pred)
    
    print(f"      ✅ XGBoost trained")
    print(f"         MAE: {xgb_mae:.6f}")
    print(f"         RMSE: {xgb_rmse:.6f}")
    print(f"         R²: {xgb_r2:.6f}")
    
    results['XGBoost'] = {
        'model': xgb_model,
        'predictions': xgb_pred,
        'MAE': xgb_mae,
        'RMSE': xgb_rmse,
        'R2': xgb_r2
    }
    
    # ========================================
    # MODEL 2: RANDOM FOREST (GOOD FOR SMALL DATA)
    # ========================================
    print("\n   🌲 Training RandomForest...")
    
    rf_model = RandomForestRegressor(
        n_estimators=300,
        max_depth=8,
        min_samples_split=5,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1
    )
    
    rf_model.fit(X_train, y_train)
    rf_pred = rf_model.predict(X_test)
    
    rf_mae = mean_absolute_error(y_test, rf_pred)
    rf_rmse = np.sqrt(mean_squared_error(y_test, rf_pred))
    rf_r2 = r2_score(y_test, rf_pred)
    
    print(f"      ✅ RandomForest trained")
    print(f"         MAE: {rf_mae:.6f}")
    print(f"         RMSE: {rf_rmse:.6f}")
    print(f"         R²: {rf_r2:.6f}")
    
    results['RandomForest'] = {
        'model': rf_model,
        'predictions': rf_pred,
        'MAE': rf_mae,
        'RMSE': rf_rmse,
        'R2': rf_r2
    }
    
    # ========================================
    # MODEL 3: ENSEMBLE (0.6 XGB + 0.4 RF)
    # ========================================
    print("\n   🔥 Creating Ensemble (0.6 XGB + 0.4 RF)...")
    
    ensemble_pred = 0.6 * xgb_pred + 0.4 * rf_pred
    
    ens_mae = mean_absolute_error(y_test, ensemble_pred)
    ens_rmse = np.sqrt(mean_squared_error(y_test, ensemble_pred))
    ens_r2 = r2_score(y_test, ensemble_pred)
    
    print(f"      ✅ Ensemble created")
    print(f"         MAE: {ens_mae:.6f}")
    print(f"         RMSE: {ens_rmse:.6f}")
    print(f"         R²: {ens_r2:.6f}")
    
    results['Ensemble'] = {
        'model': {'xgb': xgb_model, 'rf': rf_model},
        'predictions': ensemble_pred,
        'MAE': ens_mae,
        'RMSE': ens_rmse,
        'R2': ens_r2
    }
    
    # ========================================
    # COMPARISON TABLE
    # ========================================
    print("\n" + "-"*70)
    print("📊 MODEL COMPARISON:")
    print("-"*70)
    print(f"{'Model':<20} {'MAE':<12} {'RMSE':<12} {'R²':<12}")
    print("-"*70)
    
    for name, metrics in results.items():
        print(f"{name:<20} {metrics['MAE']:<12.6f} {metrics['RMSE']:<12.6f} {metrics['R2']:<12.6f}")
    
    print("-"*70)
    
    # Find best model
    best_model = max(results.keys(), key=lambda k: results[k]['R2'])
    print(f"\n   🏆 BEST MODEL: {best_model} (R²={results[best_model]['R2']:.4f})")
    
    return results

# ================================================================
# 🔧 TIME SERIES CROSS-VALIDATION
# ================================================================

def time_series_cv(X, y, n_splits=5):
    """
    Perform TimeSeriesSplit cross-validation for robust evaluation.
    """
    print("\n" + "="*70)
    print("⏰ TIME SERIES CROSS-VALIDATION")
    print("="*70)
    
    tscv = TimeSeriesSplit(n_splits=n_splits)
    
    cv_results = []
    
    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):
        X_train_fold, X_test_fold = X.iloc[train_idx], X.iloc[test_idx]
        y_train_fold, y_test_fold = y.iloc[train_idx], y.iloc[test_idx]
        
        # Quick XGBoost for CV
        model = XGBRegressor(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.05,
            reg_alpha=1.0,
            reg_lambda=1.5,
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X_train_fold, y_train_fold)
        y_pred_fold = model.predict(X_test_fold)
        
        mae = mean_absolute_error(y_test_fold, y_pred_fold)
        rmse = np.sqrt(mean_squared_error(y_test_fold, y_pred_fold))
        r2 = r2_score(y_test_fold, y_pred_fold)
        
        cv_results.append({'fold': fold, 'MAE': mae, 'RMSE': rmse, 'R2': r2})
        
        print(f"   Fold {fold}: MAE={mae:.4f}, RMSE={rmse:.4f}, R²={r2:.4f}")
    
    # Average metrics
    avg_mae = np.mean([r['MAE'] for r in cv_results])
    avg_rmse = np.mean([r['RMSE'] for r in cv_results])
    avg_r2 = np.mean([r['R2'] for r in cv_results])
    
    print(f"\n   📊 AVERAGE CV METRICS:")
    print(f"      MAE: {avg_mae:.6f}")
    print(f"      RMSE: {avg_rmse:.6f}")
    print(f"      R²: {avg_r2:.6f}")
    
    return cv_results, {'MAE': avg_mae, 'RMSE': avg_rmse, 'R2': avg_r2}

# ================================================================
# 🔧 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🚀 ECONOMY MODEL IMPROVEMENT PIPELINE")
    print("="*70)
    
    try:
        # ========================================
        # STEP 1: LOAD DATA
        # ========================================
        print("\n📥 Loading data...")
        df = pd.read_csv(INPUT_FILE)
        df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
        print(f"   ✅ Loaded {len(df)} rows")
        
        # ========================================
        # STEP 2: ENHANCED FEATURE ENGINEERING
        # ========================================
        df_enhanced = create_enhanced_features(df)
        
        # ========================================
        # STEP 3: TIME-BASED SPLIT
        # ========================================
        print("\n⏰ Performing time-based split...")
        train_df = df_enhanced[df_enhanced['Year'] <= 2019].copy()
        test_df = df_enhanced[df_enhanced['Year'] > 2019].copy()
        
        print(f"   Train: {len(train_df)} rows ({train_df['Year'].min()}-{train_df['Year'].max()})")
        print(f"   Test: {len(test_df)} rows ({test_df['Year'].min()}-{test_df['Year'].max()})")
        
        # ========================================
        # STEP 4: SELECT FEATURES & TARGET
        # ========================================
        print("\n🎯 Selecting features...")
        
        drop_cols = ['Country', 'Year', 'Month', 'economic_risk']
        feature_cols = [col for col in df_enhanced.columns if col not in drop_cols]
        
        X_train = train_df[feature_cols].copy()
        y_train = train_df['economic_risk_next'].copy()
        X_test = test_df[feature_cols].copy()
        y_test = test_df['economic_risk_next'].copy()
        
        # Handle NaN
        mask_train = X_train.notna().all(axis=1) & y_train.notna()
        mask_test = X_test.notna().all(axis=1) & y_test.notna()
        
        X_train = X_train[mask_train]
        y_train = y_train[mask_train]
        X_test = X_test[mask_test]
        y_test = y_test[mask_test]
        
        print(f"   ✅ Features: {len(feature_cols)}")
        print(f"   ✅ Train shape: {X_train.shape}")
        print(f"   ✅ Test shape: {X_test.shape}")
        
        # ========================================
        # STEP 5: APPLY SCALER
        # ========================================
        print("\n🔧 Applying scaler...")
        scaler = joblib.load(SCALER_PATH)
        
        base_features = ['Inflation', 'VIX', 'InterestRate', 'NIFTY50']
        X_train[base_features] = scaler.transform(X_train[base_features])
        X_test[base_features] = scaler.transform(X_test[base_features])
        
        print(f"   ✅ Scaler applied to base features")
        
        # ========================================
        # STEP 6: TIME SERIES CROSS-VALIDATION
        # ========================================
        X_full = pd.concat([X_train, X_test], ignore_index=True)
        y_full = pd.concat([y_train, y_test], ignore_index=True)
        
        cv_results, cv_metrics = time_series_cv(X_full, y_full, n_splits=5)
        
        # ========================================
        # STEP 7: TRAIN IMPROVED MODELS
        # ========================================
        results = train_improved_models(X_train, y_train, X_test, y_test)
        
        # ========================================
        # STEP 8: SAVE BEST MODEL
        # ========================================
        print("\n💾 Saving best model...")
        
        best_model_name = max(results.keys(), key=lambda k: results[k]['R2'])
        best_model_data = results[best_model_name]
        
        if best_model_name == 'Ensemble':
            # Save both models
            joblib.dump(best_model_data['model'], MODEL_OUTPUT)
            print(f"   ✅ Ensemble saved (XGB + RF)")
        else:
            joblib.dump(best_model_data['model'], MODEL_OUTPUT)
            print(f"   ✅ {best_model_name} saved")
        
        # ========================================
        # STEP 9: SAVE PREDICTIONS
        # ========================================
        print("\n📝 Saving predictions...")
        
        output_df = pd.DataFrame({
            'Country': test_df.loc[y_test.index, 'Country'],
            'Year': test_df.loc[y_test.index, 'Year'],
            'Month': test_df.loc[y_test.index, 'Month'],
            'actual_risk': y_test.values,
            'predicted_risk_xgb': results['XGBoost']['predictions'],
            'predicted_risk_rf': results['RandomForest']['predictions'],
            'predicted_risk_ensemble': results['Ensemble']['predictions']
        })
        
        output_df.to_csv(PREDICTIONS_OUTPUT, index=False)
        print(f"   ✅ Predictions saved to: {PREDICTIONS_OUTPUT}")
        
        # ========================================
        # STEP 10: COMPARE OLD vs NEW
        # ========================================
        print("\n" + "="*70)
        print("📊 PERFORMANCE COMPARISON: OLD vs NEW")
        print("="*70)
        
        # Old model metrics (from previous run)
        old_metrics = {
            'MAE': 0.793147,
            'RMSE': 1.011882,
            'R2': -0.486099
        }
        
        new_metrics = results['Ensemble']
        
        print(f"\n{'Metric':<15} {'OLD':<15} {'NEW (Ensemble)':<15} {'Improvement':<15}")
        print("-"*70)
        
        for metric in ['MAE', 'RMSE', 'R2']:
            old_val = old_metrics[metric]
            new_val = new_metrics[metric]
            
            if metric == 'R2':
                improvement = new_val - old_val
                sign = "+" if improvement > 0 else ""
            else:
                improvement = old_val - new_val  # Lower is better for MAE/RMSE
                sign = "+" if improvement > 0 else ""
            
            print(f"{metric:<15} {old_val:<15.6f} {new_val:<15.6f} {sign}{improvement:.6f}")
        
        print("-"*70)
        
        # Overall assessment
        r2_improvement = new_metrics['R2'] - old_metrics['R2']
        
        if r2_improvement > 0.5:
            print(f"\n   🎉 EXCELLENT IMPROVEMENT! R² increased by {r2_improvement:.4f}")
        elif r2_improvement > 0.2:
            print(f"\n   ✅ GOOD IMPROVEMENT! R² increased by {r2_improvement:.4f}")
        elif r2_improvement > 0:
            print(f"\n   ⚠️  MODEST IMPROVEMENT. R² increased by {r2_improvement:.4f}")
        else:
            print(f"\n   ❌ NO IMPROVEMENT. R² decreased by {abs(r2_improvement):.4f}")
        
        # ========================================
        # FINAL SUMMARY
        # ========================================
        print("\n" + "="*70)
        print("✅ MODEL IMPROVEMENT COMPLETE")
        print("="*70)
        
        print(f"\n   🏆 BEST MODEL: {best_model_name}")
        print(f"   📊 Final Metrics:")
        print(f"      MAE: {new_metrics['MAE']:.6f}")
        print(f"      RMSE: {new_metrics['RMSE']:.6f}")
        print(f"      R²: {new_metrics['R2']:.6f}")
        
        print(f"\n   💾 Outputs:")
        print(f"      Model: {MODEL_OUTPUT}")
        print(f"      Predictions: {PREDICTIONS_OUTPUT}")
        
        print("\n" + "="*70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
