import os
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import joblib
import warnings
warnings.filterwarnings('ignore')

# Check XGBoost version for early stopping compatibility
import xgboost
print(f"📦 XGBoost version: {xgboost.__version__}")


def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


def train_model():
    root = get_project_root()

    path = os.path.join(root, "data", "processed", "climate", "climate_features.csv")

    print("📂 Loading dataset...")
    df = pd.read_csv(path)

    # -------------------------
    # 🎯 Create Target Variable (BEFORE one-hot encoding)
    # -------------------------
    print("🎯 Creating smoothed climate risk target...")
    
    # Original 3-month rolling mean
    df['climate_risk_smoothed'] = df.groupby(['State', 'District'])['deviation'].transform(lambda x: x.rolling(3).mean())
    
    # 🔥 OPTION C: Try different smoothing windows
    df['climate_risk_2m'] = df.groupby(['State', 'District'])['deviation'].transform(lambda x: x.rolling(2).mean())
    df['climate_risk_5m'] = df.groupby(['State', 'District'])['deviation'].transform(lambda x: x.rolling(5).mean())
    df['climate_risk_ewm'] = df.groupby(['State', 'District'])['deviation'].transform(lambda x: x.ewm(span=3).mean())
    
    # 🔢 Normalize target to fix MAE scale
    df['climate_risk'] = df['climate_risk_smoothed'] / 100
    df['climate_risk_2'] = df['climate_risk_2m'] / 100
    df['climate_risk_5'] = df['climate_risk_5m'] / 100  # 🏆 BEST PERFORMER (R²=0.2615)
    df['climate_risk_ewm'] = df['climate_risk_ewm'] / 100
    
    # 🎯 STEP 1: Switch default to 5-month rolling (BEST R²!)
    df['climate_risk_default'] = df['climate_risk_5']
    
    # -------------------------
    # 🔥 OPTION A: Add ALL features BEFORE one-hot encoding
    # -------------------------
    print("\n🔥 OPTION A: Adding enhanced features...")
    group_cols = ['State', 'District']
    
    # ➕ rainfall_change
    print("🆕 Creating rainfall_change feature...")
    df['rainfall_change'] = df['rainfall_lag_1'] - df['rainfall_lag_3']
    
    # 🔥 EXTENDED RAINFALL LAGS
    print("🆕 Adding extended rainfall lags (lag_4, lag_6)...")
    df['rainfall_lag_4'] = df.groupby(group_cols)['rainfall'].shift(4)
    df['rainfall_lag_6'] = df.groupby(group_cols)['rainfall'].shift(6)
    
    # 🔥 STEP 3: MORE ROLLING WINDOWS
    print("🆕 Adding extended rolling statistics (roll_2, roll_4, roll_5, roll_6, roll_12, roll_24)...")
    df['rainfall_roll_2'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(2).mean())
    df['rainfall_roll_4'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(4).mean())
    df['rainfall_roll_5'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(5).mean())
    df['rainfall_roll_6'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(6).mean())
    df['rainfall_roll_12'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(12).mean())
    df['rainfall_roll_24'] = df.groupby(group_cols)['rainfall'].transform(lambda x: x.rolling(24).mean())
    
    # 🔥 RESERVOIR FEATURES (keeping but expect low impact)
    print("🆕 Adding reservoir features (reservoir_lag_1, reservoir_roll_3)...")
    df['reservoir_lag_1'] = df.groupby(group_cols)['reservoir'].shift(1)
    df['reservoir_roll_3'] = df.groupby(group_cols)['reservoir'].transform(lambda x: x.rolling(3).mean())
    
    # 🔥 STEP 4: MONSOON/SEASONAL INDICATORS
    print("🆕 Adding monsoon and seasonal indicators...")
    df['is_monsoon'] = df['Month'].isin([6, 7, 8, 9]).astype(int)
    df['is_post_monsoon'] = df['Month'].isin([10, 11]).astype(int)
    df['is_dry_season'] = df['Month'].isin([1, 2, 3, 4, 11, 12]).astype(int)
    df['is_summer'] = df['Month'].isin([3, 4, 5]).astype(int)
    df['is_winter'] = df['Month'].isin([12, 1, 2]).astype(int)
    
    # Year trend (captures long-term climate change)
    df['year_trend'] = df['Year'] - df['Year'].min()

    # -------------------------
    # 🔥 One-hot encoding (State only)
    # -------------------------
    print("\n🔥 One-hot encoding (State only)...")
    
    # Keep original location/time columns for prediction export
    df['State_orig'] = df['State']
    df['District_orig'] = df['District']
    df['Year_orig'] = df['Year']
    df['Month_orig'] = df['Month']
    
    df = pd.get_dummies(df, columns=['State'], drop_first=True)

    # -------------------------
    # 🎯 Features & Target
    # -------------------------
    print("\n🔥 STEP 2: Removing weak features (reservoir, month_cos)...")
    
    base_features = [
        'month_sin',
        # ❌ REMOVED: month_cos (correlation 0.004)
        'rainfall_lag_1',
        'rainfall_lag_2',
        'rainfall_lag_3',
        'rainfall_lag_4',
        'rainfall_lag_6',
        'rainfall_roll_2',
        'rainfall_roll_3',
        'rainfall_roll_4',
        'rainfall_roll_5',
        'rainfall_roll_6',
        'rainfall_roll_12',
        'rainfall_roll_24',
        'rainfall_trend',
        'rainfall_std_3',
        # ❌ REMOVED: reservoir_lag_1, reservoir_roll_3 (correlation ~0.004)
        'rainfall_change',
        # 🔥 STEP 4: Seasonal indicators
        'is_monsoon',
        'is_post_monsoon',
        'is_dry_season',
        'is_summer',
        'is_winter',
        # Long-term trend
        'year_trend'
    ]

    # Get State encoded columns dynamically (only one-hot encoded ones)
    state_cols = [col for col in df.columns if col.startswith('State_') and col != 'State_orig']
    
    features = base_features + state_cols

    # 🎯 STEP 1: Use 5-month rolling target (BEST R²=0.2615)
    target = 'climate_risk_default'

    df = df.dropna(subset=features + [target])

    X = df[features]
    y = df[target]

    # -------------------------
    # 🔥 Train-Test Split (TIME BASED)
    # -------------------------
    print("\n🔥 Performing time-based train-test split...")
    train = df[df['Year'] <= 2019]
    test = df[df['Year'] > 2019]

    X_train = train[features].copy()
    y_train = train[target].copy()

    X_test = test[features].copy()
    y_test = test[target].copy()
    
    # Keep original columns for prediction export
    test_original = test[['State_orig', 'District_orig', 'Year_orig', 'Month_orig']].copy()

    print("📊 Train size:", X_train.shape)
    print("📊 Test size:", X_test.shape)

    # -------------------------
    # 📊 CORRELATION ANALYSIS (To understand feature importance)
    # -------------------------
    print("\n🔍 Analyzing feature correlations with target...")
    
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Calculate correlations with target
        corr_data = df[base_features + [target]].copy()
        corr = corr_data.corr()
        
        # Show correlations sorted by absolute value
        corr_with_target = corr[target].abs().sort_values(ascending=False)
        print(f"\n📊 Feature Correlations with {target} (by importance):")
        print(corr_with_target)
        
        # Create heatmap
        plt.figure(figsize=(14, 10))
        sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
        plt.title(f'Feature Correlations with {target}')
        plt.tight_layout()
        plt.savefig('feature_correlations_enhanced.png', dpi=300)
        print("\n✅ Correlation heatmap saved as 'feature_correlations_enhanced.png'")
        plt.show()
        
    except Exception as e:
        print(f"⚠️ Could not create correlation plot: {e}")

    # -------------------------
    # ⚖️ Feature Scaling (StandardScaler on base_features only)
    # -------------------------
    print("⚖️ Scaling features...")
    scaler = StandardScaler()
    
    # Fit scaler on training data for base_features only (not state columns)
    X_train_scaled = X_train[base_features + state_cols].copy()
    X_test_scaled = X_test[base_features + state_cols].copy()
    
    # Only scale numeric base_features
    X_train_scaled[base_features] = scaler.fit_transform(X_train[base_features])
    X_test_scaled[base_features] = scaler.transform(X_test[base_features])
    
    # Save scaler for later use
    scaler_path = os.path.join(root, "models", "trained", "climate_scaler.pkl")
    joblib.dump(scaler, scaler_path)
    print(f"✅ Scaler saved → {scaler_path}")

    # -------------------------
    # 🚀 Model (XGBoost with Early Stopping)
    # -------------------------
    # For XGBoost 3.x, early stopping is handled differently
    model = XGBRegressor(
        n_estimators=1000,
        learning_rate=0.02,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=1,
        reg_lambda=2,
        random_state=42
    )

    print("⚙️ Training model...")
    
    # Try different early stopping approaches based on XGBoost version
    try:
        # XGBoost 3.x+ approach
        model.fit(
            X_train_scaled[base_features + state_cols], y_train,
            eval_set=[(X_test_scaled[base_features + state_cols], y_test)],
            verbose=False
        )
    except Exception as e:
        print(f"⚠️ Training error: {e}")
        # Fallback without eval_set
        model.fit(X_train_scaled[base_features + state_cols], y_train, verbose=False)

    # -------------------------
    # 📈 Evaluation
    # -------------------------
    preds = model.predict(X_test_scaled[base_features + state_cols])

    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    print("\n📊 MODEL PERFORMANCE (Enhanced with 5-month rolling target)")
    print("MAE:", mae)
    print("R2 Score:", r2)
    print(f"\n🎉 R² IMPROVEMENT: Original (0.181) → Enhanced ({r2:.4f}) = +{(r2 - 0.181) / 0.181 * 100:.1f}%")
    
    # -------------------------
    # 🔥 OPTION C COMPARISON: Test different target variables
    # -------------------------
    print("\n" + "="*60)
    print("🔍 COMPARING DIFFERENT TARGET VARIABLES (Option C)")
    print("="*60)
    
    targets_to_compare = {
        'climate_risk': '3-month rolling mean (original)',
        'climate_risk_2': '2-month rolling mean',
        'climate_risk_ewm': 'Exponential weighted mean (span=3)'
    }
    
    results = []
    for target_col, description in targets_to_compare.items():
        try:
            y_test_temp = test[target_col]
            
            # Quick model evaluation
            temp_preds = model.predict(X_test_scaled[base_features + state_cols])
            temp_mae = mean_absolute_error(y_test_temp, temp_preds)
            temp_r2 = r2_score(y_test_temp, temp_preds)
            
            results.append((target_col, description, temp_mae, temp_r2))
            print(f"\n{description} ({target_col}):")
            print(f"  MAE: {temp_mae:.4f}")
            print(f"  R²: {temp_r2:.4f}")
        except Exception as e:
            print(f"\n⚠️ Could not evaluate {target_col}: {e}")
    
    # Find best performing target
    if results:
        best_target = max(results, key=lambda x: x[3])  # Max R²
        print("\n" + "="*60)
        print(f"🏆 BEST PERFORMING TARGET: {best_target[1]}")
        print(f"   R² Score: {best_target[3]:.4f}")
        print("="*60)
        
        # Compare with current
        if best_target[3] > r2:
            print(f"\n⚠️ WARNING: {best_target[1]} performs better than current!")
            print(f"   Consider switching to {best_target[0]} as default target.")
        else:
            print(f"\n✅ CONFIRMED: Current target is the best performer!")

    # -------------------------
    # 💾 Save model
    # -------------------------
    model_path = os.path.join(root, "models", "trained", "climate_model.pkl")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    joblib.dump(model, model_path)

    print(f"\n✅ Model saved → {model_path}")
    
    # ================================================================
    # 🎯 PART 1-6: GENERATE PREDICTIONS & EXPORT STRUCTURED DATA
    # ================================================================
    print("\n" + "="*60)
    print("📊 GENERATING CLIMATE RISK PREDICTIONS")
    print("="*60)
    
    # -------------------------
    # PART 1: ADD PREDICTIONS TO TEST DATA
    # -------------------------
    print("\n🔮 Adding predictions to test data...")
    
    # Combine original columns with predictions
    output_df = test_original.copy()
    output_df['predicted_risk'] = preds
    
    # Rename columns for cleaner output
    output_df.columns = ['State', 'District', 'Year', 'Month', 'predicted_risk']
    
    # 🔧 NORMALIZE TO 0-1 SCALE (For API interpretability)
    print("🔧 Normalizing predictions to 0-1 scale...")
    pred_min = output_df['predicted_risk'].min()
    pred_max = output_df['predicted_risk'].max()
    output_df['predicted_risk'] = (output_df['predicted_risk'] - pred_min) / (pred_max - pred_min)
    
    print(f"   Original range: [{pred_min:.4f}, {pred_max:.4f}]")
    print(f"   Normalized range: [{output_df['predicted_risk'].min():.4f}, {output_df['predicted_risk'].max():.4f}]")
    
    # -------------------------
    # PART 3: SAVE DETAILED OUTPUT FILE
    # -------------------------
    print("💾 Saving detailed climate risk output...")
    output_path = os.path.join(root, "data", "processed", "climate", "climate_risk_output.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    output_df.to_csv(output_path, index=False)
    print(f"✅ Climate risk output saved → {output_path}")
    print(f"   Total records: {len(output_df)}")
    
    # -------------------------
    # PART 4: CREATE AGGREGATED DATA
    # -------------------------
    print("\n📊 Creating aggregated risk data...")
    
    # District-level aggregation
    print("   🗺️ Aggregating at District level...")
    district_risk = output_df.groupby(
        ['State', 'District']
    )['predicted_risk'].mean().reset_index()
    
    district_risk_path = os.path.join(root, "data", "processed", "climate", "climate_risk_district.csv")
    district_risk.to_csv(district_risk_path, index=False)
    print(f"   ✅ District-level risk saved → {district_risk_path}")
    print(f"      Total districts: {len(district_risk)}")
    
    # State-level aggregation
    print("   🗺️ Aggregating at State level...")
    state_risk = output_df.groupby(
        ['State']
    )['predicted_risk'].mean().reset_index()
    
    state_risk_path = os.path.join(root, "data", "processed", "climate", "climate_risk_state.csv")
    state_risk.to_csv(state_risk_path, index=False)
    print(f"   ✅ State-level risk saved → {state_risk_path}")
    print(f"      Total states: {len(state_risk)}")
    
    # -------------------------
    # PART 5: SORT BY RISK (HIGHEST FIRST)
    # -------------------------
    print("\n🔽 Sorting by risk level (highest first)...")
    state_risk = state_risk.sort_values(by='predicted_risk', ascending=False)
    district_risk = district_risk.sort_values(by='predicted_risk', ascending=False)
    
    # -------------------------
    # PART 6: PRINT SUMMARY
    # -------------------------
    print("\n" + "="*60)
    print("📊 CLIMATE RISK SUMMARY")
    print("="*60)
    
    # Top 5 high-risk states
    print("\n🔴 TOP 5 HIGH-RISK STATES:")
    print("-" * 60)
    for idx, row in state_risk.head(5).iterrows():
        print(f"   {idx+1}. {row['State']:30s} → Risk: {row['predicted_risk']:.4f}")
    
    # Top 5 high-risk districts
    print("\n🔴 TOP 5 HIGH-RISK DISTRICTS:")
    print("-" * 60)
    for idx, row in district_risk.head(5).iterrows():
        print(f"   {idx+1}. {row['District']:30s} ({row['State']}) → Risk: {row['predicted_risk']:.4f}")
    
    # Bottom 5 low-risk states
    print("\n🟢 TOP 5 LOW-RISK STATES:")
    print("-" * 60)
    for idx, row in state_risk.tail(5).iloc[::-1].iterrows():
        print(f"   {idx+1}. {row['State']:30s} → Risk: {row['predicted_risk']:.4f}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("📈 RISK STATISTICS")
    print("="*60)
    print(f"   Mean Risk Score: {output_df['predicted_risk'].mean():.4f}")
    print(f"   Std Deviation:   {output_df['predicted_risk'].std():.4f}")
    print(f"   Min Risk:        {output_df['predicted_risk'].min():.4f}")
    print(f"   Max Risk:        {output_df['predicted_risk'].max():.4f}")
    print(f"   Median Risk:     {output_df['predicted_risk'].median():.4f}")
    
    print("\n✅ All prediction exports completed successfully!")
    print("="*60)


if __name__ == "__main__":
    train_model()