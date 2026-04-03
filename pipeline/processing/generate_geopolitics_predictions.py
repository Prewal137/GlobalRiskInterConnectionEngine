"""
🔮 GEOPOLITICS RISK PREDICTION (Phase 6)

Generate risk scores using trained XGBoost model.

Input: 
  - data/processed/geopolitics/geopolitics_features.csv
  - models/trained/geopolitics_model.pkl

Output:
  - data/processed/geopolitics/geopolitics_risk_output.csv
"""

import pandas as pd
import numpy as np
import joblib
import os

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
INPUT_FEATURES = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_features.csv")
MODEL_PATH = os.path.join(BASE_PATH, "models", "trained", "geopolitics_model.pkl")
OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output.csv")


# ================================================================
# 🔮 PREDICTION PIPELINE
# ================================================================

def load_model(model_path):
    """Load trained XGBoost model."""
    print("\n💾 Loading model...")
    model = joblib.load(model_path)
    print(f"   ✅ Model loaded from {model_path}")
    return model


def load_features(file_path):
    """Load feature dataset."""
    print("\n📥 Loading features...")
    df = pd.read_csv(file_path)
    
    # Sort by Country, Year, Month
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    
    print(f"   ✅ Loaded {len(df):,} rows")
    print(f"   🌍 Countries: {df['Country'].nunique()}")
    print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    return df


def prepare_for_prediction(df, model):
    """
    Prepare features for prediction (match training structure).
    
    Drop columns that were not used in training:
    - Country (kept for output but not prediction)
    - Year, Month (temporal identifiers)
    - Target variables
    - Leakage features
    """
    print("\n🔧 Preparing features for prediction...")
    
    # Keep metadata for output
    metadata_cols = ['Country', 'Year', 'Month']
    metadata = df[metadata_cols].copy()
    
    # Columns to drop (same as training)
    drop_cols = [
        'Country', 'Year', 'Month',
        'geopolitical_risk', 'geopolitical_risk_raw',
        'instability_score'
    ]
    
    # Get feature columns
    feature_cols = [col for col in df.columns if col not in drop_cols]
    
    X = df[feature_cols].copy()
    
    print(f"   ✅ Features shape: {X.shape}")
    print(f"   📋 Using {len(feature_cols)} features")
    
    return metadata, X, feature_cols


def generate_predictions(model, X, metadata):
    """
    Generate risk predictions.
    
    Returns:
    - Raw predictions
    - Normalized risk scores (0-1)
    - Risk classification labels (UPGRADE 2)
    """
    print("\n🔮 Generating predictions...")
    
    # Raw predictions
    risk_pred = model.predict(X)
    
    # Normalize to 0-1 scale
    risk_min = risk_pred.min()
    risk_max = risk_pred.max()
    
    risk_score = (risk_pred - risk_min) / (risk_max - risk_min + 1e-10)
    
    print(f"   ✅ Predictions generated")
    print(f"   📊 Prediction range: [{risk_pred.min():.4f}, {risk_pred.max():.4f}]")
    print(f"   📊 Normalized range: [{risk_score.min():.4f}, {risk_score.max():.4f}]")
    
    # Create output DataFrame
    output_df = metadata.copy()
    output_df['risk_score'] = risk_score
    output_df['risk_raw'] = risk_pred
    
    # ================================================================
    # 🔮 UPGRADE 2: ADD RISK CLASSIFICATION LABELS
    # ================================================================
    print("\n🏷️  Adding risk classification labels...")
    
    def risk_label(x):
        """Convert numeric risk score to human-readable category."""
        if x > 0.8:
            return "Very High"
        elif x > 0.6:
            return "High"
        elif x > 0.4:
            return "Medium"
        elif x > 0.2:
            return "Low"
        else:
            return "Very Low"
    
    output_df['risk_category'] = output_df['risk_score'].apply(risk_label)
    
    print(f"   ✅ Added risk_category (Very High, High, Medium, Low, Very Low)")
    
    return output_df


def save_predictions(output_df, output_path):
    """Save predictions to CSV."""
    print("\n📤 Saving predictions...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    output_df.to_csv(output_path, index=False)
    
    print(f"   ✅ Saved to {output_path}")
    print(f"   📊 Shape: {output_df.shape}")


def create_country_summary(output_df):
    """
    Create country-level summary statistics.
    
    Output:
    - Average risk per country
    - Latest risk score
    - Risk trend
    """
    print("\n📊 Creating country summaries...")
    
    # Group by country
    country_summary = output_df.groupby('Country').agg(
        avg_risk=('risk_score', 'mean'),
        latest_risk=('risk_score', 'last'),
        min_risk=('risk_score', 'min'),
        max_risk=('risk_score', 'max'),
        std_risk=('risk_score', 'std'),
        total_months=('risk_score', 'count')
    ).reset_index()
    
    # Calculate trend (latest - average)
    country_summary['risk_trend'] = country_summary['latest_risk'] - country_summary['avg_risk']
    
    # Sort by latest risk
    country_summary = country_summary.sort_values('latest_risk', ascending=False)
    
    print(f"   ✅ Created summary for {len(country_summary)} countries")
    
    return country_summary


def save_country_summary(country_summary, base_path):
    """Save country summary to CSV."""
    summary_path = base_path.replace('.csv', '_country.csv')
    
    print(f"\n📤 Saving country summary...")
    country_summary.to_csv(summary_path, index=False)
    
    print(f"   ✅ Saved to {summary_path}")
    
    return summary_path


def print_top_countries(country_summary, top_n=10):
    """Print top high-risk countries."""
    print(f"\n🚨 TOP {top_n} HIGHEST RISK COUNTRIES:")
    print("="*70)
    
    for i, row in country_summary.head(top_n).iterrows():
        trend_arrow = "↑" if row['risk_trend'] > 0 else "↓" if row['risk_trend'] < 0 else "→"
        print(f"   {i+1:2d}. {row['Country']:30s} Risk: {row['latest_risk']:.4f}  Avg: {row['avg_risk']:.4f}  Trend: {trend_arrow}")
    
    print("="*70)


def print_global_summary(output_df):
    """Print global risk summary statistics."""
    print("\n🌍 GLOBAL RISK SUMMARY:")
    print("="*70)
    
    print(f"   Total observations: {len(output_df):,}")
    print(f"   Countries analyzed: {output_df['Country'].nunique()}")
    print(f"   Time period: {output_df['Year'].min()}-{output_df['Year'].max()}")
    print(f"\n   Average risk score: {output_df['risk_score'].mean():.4f}")
    print(f"   Std deviation:      {output_df['risk_score'].std():.4f}")
    print(f"   Min risk:           {output_df['risk_score'].min():.4f}")
    print(f"   Max risk:           {output_df['risk_score'].max():.4f}")
    
    # Risk distribution
    very_high = (output_df['risk_score'] > 0.8).sum()
    high = ((output_df['risk_score'] > 0.6) & (output_df['risk_score'] <= 0.8)).sum()
    medium = ((output_df['risk_score'] > 0.4) & (output_df['risk_score'] <= 0.6)).sum()
    low = (output_df['risk_score'] <= 0.4).sum()
    
    total = len(output_df)
    print(f"\n   Risk Distribution:")
    print(f"      Very High (>0.8):  {very_high:,} ({very_high/total*100:.1f}%)")
    print(f"      High (0.6-0.8):    {high:,} ({high/total*100:.1f}%)")
    print(f"      Medium (0.4-0.6):  {medium:,} ({medium/total*100:.1f}%)")
    print(f"      Low (≤0.4):        {low:,} ({low/total*100:.1f}%)")
    
    print("="*70)


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function for prediction pipeline."""
    print("\n🔮 Starting Geopolitics Risk Prediction (Phase 6)...")
    print("="*70)
    
    # Step 1: Load model
    model = load_model(MODEL_PATH)
    
    # Step 2: Load features
    df = load_features(INPUT_FEATURES)
    
    # Step 3: Prepare for prediction
    metadata, X, feature_cols = prepare_for_prediction(df, model)
    
    # Step 4: Generate predictions
    output_df = generate_predictions(model, X, metadata)
    
    # Step 5: Save predictions
    save_predictions(output_df, OUTPUT_FILE)
    
    # Step 6: Create country summary
    country_summary = create_country_summary(output_df)
    
    # Step 7: Save country summary
    summary_path = save_country_summary(country_summary, OUTPUT_FILE)
    
    # Step 8: Print summaries
    print_top_countries(country_summary, top_n=15)
    print_global_summary(output_df)
    
    # Final summary
    print("\n" + "="*70)
    print("🎉 PHASE 6 COMPLETE - RISK PREDICTIONS GENERATED")
    print("="*70)
    
    print(f"\n📁 OUTPUT FILES:")
    print(f"   Main output: {OUTPUT_FILE}")
    print(f"   Country summary: {summary_path}")
    
    print(f"\n✅ Predictions ready for API and interconnection!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error during prediction: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
