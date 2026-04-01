"""
Trade Risk Output Generator
Generates trade risk predictions and creates aggregated output files.
"""

import pandas as pd
import numpy as np
import joblib
import os


def load_data(filepath: str) -> pd.DataFrame:
    """Load trade feature data from CSV file."""
    print(f"Loading data from {filepath}...")
    df = pd.read_csv(filepath)
    print(f"✓ Loaded {len(df)} rows with {len(df.columns)} columns")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the dataset by handling missing values."""
    print("\nCleaning data...")
    
    # Drop rows with NaN values
    initial_rows = len(df)
    df = df.dropna()
    dropped_rows = initial_rows - len(df)
    if dropped_rows > 0:
        print(f"  - Dropped {dropped_rows} rows with NaN values")
    
    print(f"✓ Cleaned dataset: {len(df)} rows")
    return df


def load_model(model_path: str):
    """Load trained model using joblib."""
    print(f"\nLoading model from {model_path}...")
    model = joblib.load(model_path)
    print("✓ Model loaded successfully")
    return model


def prepare_features(df: pd.DataFrame):
    """Prepare features for prediction (same as training)."""
    print("\nPreparing features for prediction...")
    
    # Columns to drop (must match training)
    drop_columns = ['Country', 'Shock', 'Growth', 'Export_Growth', 'Import_Growth']
    
    # Get feature columns
    feature_columns = [col for col in df.columns if col not in drop_columns]
    
    print(f"  - Using {len(feature_columns)} features")
    print(f"  - Features: {feature_columns}")
    
    X = df[feature_columns]
    return X, feature_columns


def predict_risk(model, X: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Generate trade risk predictions."""
    print("\nGenerating predictions...")
    
    # Get probability of class 1 (shock)
    probabilities = model.predict_proba(X)[:, 1]
    
    # Create Trade_Risk column
    df = df.copy()
    df['Trade_Risk'] = probabilities
    
    print(f"  - Predictions generated: {len(probabilities)}")
    print(f"  - Risk range: [{probabilities.min():.4f}, {probabilities.max():.4f}]")
    print(f"  - Mean risk: {probabilities.mean():.4f}")
    
    return df


def save_full_output(df: pd.DataFrame, output_path: str):
    """Save full prediction output with Country, Year, and Trade_Risk."""
    print(f"\nSaving full output to {output_path}...")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Select only required columns
    output_df = df[['Country', 'Year', 'Trade_Risk']].copy()
    
    # Save to CSV
    output_df.to_csv(output_path, index=False)
    
    print(f"✓ Saved {len(output_df)} predictions")


def aggregate_by_country(df: pd.DataFrame, output_path: str):
    """Aggregate trade risk by country and save."""
    print(f"\nAggregating by country and saving to {output_path}...")
    
    # Group by country and calculate mean risk
    country_risk = df.groupby('Country')['Trade_Risk'].mean().reset_index()
    country_risk.columns = ['Country', 'Trade_Risk']
    
    # Sort by risk descending
    country_risk = country_risk.sort_values('Trade_Risk', ascending=False)
    
    # Save to CSV
    country_risk.to_csv(output_path, index=False)
    
    print(f"✓ Aggregated {len(country_risk)} countries")


def get_top_countries(df: pd.DataFrame, output_path: str, top_n: int = 10):
    """Get top N risky countries and save."""
    print(f"\nIdentifying top {top_n} risky countries and saving to {output_path}...")
    
    # Aggregate by country
    country_risk = df.groupby('Country')['Trade_Risk'].mean().reset_index()
    country_risk.columns = ['Country', 'Trade_Risk']
    
    # Sort by risk descending and take top N
    top_countries = country_risk.sort_values('Trade_Risk', ascending=False).head(top_n)
    
    # Save to CSV
    top_countries.to_csv(output_path, index=False)
    
    print(f"✓ Saved top {top_n} countries")


def print_summary(df: pd.DataFrame):
    """Print summary statistics."""
    print("\n" + "="*50)
    print("TRADE RISK PREDICTION SUMMARY")
    print("="*50)
    
    # Total predictions
    print(f"Total Predictions: {len(df)}")
    
    # Risk statistics
    print(f"\nRisk Statistics:")
    print(f"  Min:  {df['Trade_Risk'].min():.4f}")
    print(f"  Max:  {df['Trade_Risk'].max():.4f}")
    print(f"  Mean: {df['Trade_Risk'].mean():.4f}")
    print(f"  Std:  {df['Trade_Risk'].std():.4f}")
    
    # Top 5 risky countries
    print(f"\nTop 5 Risky Countries:")
    country_risk = df.groupby('Country')['Trade_Risk'].mean().sort_values(ascending=False)
    for i, (country, risk) in enumerate(country_risk.head(5).items(), 1):
        print(f"  {i}. {country}: {risk:.4f}")
    
    print("="*50)


def main():
    """Main pipeline for generating trade risk outputs."""
    print("="*50)
    print("TRADE RISK OUTPUT GENERATION")
    print("="*50)
    
    # File paths
    input_file = "data/processed/trade/trade_features_clean.csv"
    model_path = "models/trained/trade_model.pkl"
    output_dir = "data/processed/trade/"
    
    # Output files
    output_full = os.path.join(output_dir, "trade_risk_output.csv")
    output_country = os.path.join(output_dir, "trade_risk_country.csv")
    output_top = os.path.join(output_dir, "trade_risk_top.csv")
    
    # Step 1: Load data
    df = load_data(input_file)
    
    # Step 2: Clean data
    df = clean_data(df)
    
    # Step 3: Load model
    model = load_model(model_path)
    
    # Step 4: Prepare features
    X, feature_columns = prepare_features(df)
    
    # Step 5: Generate predictions
    df = predict_risk(model, X, df)
    
    # Step 6: Save full output
    save_full_output(df, output_full)
    
    # Step 7: Aggregate by country
    aggregate_by_country(df, output_country)
    
    # Step 8: Get top countries
    get_top_countries(df, output_top)
    
    # Step 9: Print summary
    print_summary(df)
    
    print("\n✅ TRADE RISK OUTPUT GENERATION COMPLETED SUCCESSFULLY!")
    print(f"📁 Output files saved to: {output_dir}")
    print(f"  - {output_full}")
    print(f"  - {output_country}")
    print(f"  - {output_top}")


if __name__ == "__main__":
    main()
