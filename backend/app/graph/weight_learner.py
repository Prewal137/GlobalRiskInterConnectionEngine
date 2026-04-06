"""
🔥 Weight Learner for Graph Interconnection System

Learns relationships between sectors using:
1. Correlation-based weights (simple, interpretable)
2. Regression-based weights (captures directional influence)

NO fixed/manual weights - ALL learned from data!

Author: Global Risk Platform
Date: 2026-04-05
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from typing import Dict, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')


def learn_weights_correlation(df: pd.DataFrame) -> Dict[Tuple[str, str], float]:
    """
    METHOD 1: Learn weights using correlation matrix.
    
    Computes pairwise correlations between all sectors.
    Correlation values become edge weights in the graph.
    
    Args:
        df: DataFrame with sector risk columns
        
    Returns:
        Dictionary: {(source_sector, target_sector): weight}
    """
    print("\n" + "="*70)
    print("📊 METHOD 1: CORRELATION-BASED WEIGHT LEARNING")
    print("="*70)
    
    # Get sector columns (exclude Year and Month)
    sector_cols = [col for col in df.columns if col not in ['Year', 'Month']]
    
    # Compute correlation matrix
    corr_matrix = df[sector_cols].corr()
    
    print(f"\n📈 Correlation Matrix ({len(sector_cols)} sectors):")
    print(corr_matrix.round(3).to_string())
    
    # Convert to edge weights
    weights = {}
    for source in sector_cols:
        for target in sector_cols:
            if source != target:
                # Use absolute correlation as weight
                # (positive and negative correlations both indicate influence)
                weight = abs(corr_matrix.loc[source, target])
                weights[(source, target)] = weight
    
    print(f"\n✅ Learned {len(weights)} edges from correlation")
    print(f"   Weight range: [{min(weights.values()):.4f}, {max(weights.values()):.4f}]")
    print(f"   Average weight: {np.mean(list(weights.values())):.4f}")
    
    # Print top 10 strongest relationships
    sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    print(f"\n🔝 Top 10 Strongest Relationships:")
    for i, ((source, target), weight) in enumerate(sorted_weights[:10], 1):
        print(f"   {i:2d}. {source:20s} → {target:20s} = {weight:.4f}")
    
    return weights


def learn_weights_regression(df: pd.DataFrame, use_temporal: bool = True) -> Dict[Tuple[str, str], float]:
    """
    METHOD 2: Learn weights using regression (RECOMMENDED).
    
    For each target sector, trains a LinearRegression model:
    - X = all other sectors
    - y = target sector
    - Extract coefficients as edge weights
    
    Args:
        df: DataFrame with sector risk columns
        use_temporal: If True, use lagged features to prevent data leakage
        
    Returns:
        Dictionary: {(source_sector, target_sector): weight}
    """
    print("\n" + "="*70)
    print("🔥 METHOD 2: REGRESSION-BASED WEIGHT LEARNING")
    print("="*70)
    
    if use_temporal:
        print("   ⚡ Using TEMPORAL approach (lagged features) to prevent data leakage")
        df = create_lagged_features(df)
    
    # Get sector columns (exclude Year, Month, and lagged columns)
    sector_cols = [col for col in df.columns 
                   if col not in ['Year', 'Month'] and '_lag' not in col]
    
    weights = {}
    model_r2_scores = {}
    
    print(f"\n🎯 Training regression models for {len(sector_cols)} target sectors:")
    
    for target_sector in sector_cols:
        # Features = all other sectors
        source_sectors = [s for s in sector_cols if s != target_sector]
        
        X = df[source_sectors].values
        y = df[target_sector].values
        
        # Remove NaN rows (from lagging)
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_valid = X[valid_mask]
        y_valid = y[valid_mask]
        
        if len(X_valid) < 10:
            print(f"   ⚠️  Insufficient data for {target_sector}, skipping")
            continue
        
        # Train LinearRegression
        model = LinearRegression()
        model.fit(X_valid, y_valid)
        
        # Extract coefficients as weights
        r2_score = model.score(X_valid, y_valid)
        model_r2_scores[target_sector] = r2_score
        
        print(f"\n   📊 Target: {target_sector}")
        print(f"      R² Score: {r2_score:.4f}")
        print(f"      Training samples: {len(X_valid)}")
        
        for i, source_sector in enumerate(source_sectors):
            weight = abs(model.coef_[i])  # Use absolute value for edge weight
            weights[(source_sector, target_sector)] = weight
            
            if abs(model.coef_[i]) > 0.01:  # Only print significant weights
                print(f"      {source_sector:20s} → coefficient: {model.coef_[i]:+.4f}, weight: {weight:.4f}")
    
    print(f"\n✅ Learned {len(weights)} edges from regression")
    if weights:
        print(f"   Weight range: [{min(weights.values()):.4f}, {max(weights.values()):.4f}]")
        print(f"   Average weight: {np.mean(list(weights.values())):.4f}")
    
    # Print model performance
    print(f"\n📈 Model Performance:")
    for sector, r2 in sorted(model_r2_scores.items(), key=lambda x: x[1], reverse=True):
        print(f"   {sector:20s}: R² = {r2:.4f}")
    
    return weights


def create_lagged_features(df: pd.DataFrame, lag: int = 1) -> pd.DataFrame:
    """
    Create lagged features to prevent data leakage.
    
    Uses PAST sector risks to predict CURRENT sector risks.
    
    Args:
        df: DataFrame with sector risk columns
        lag: Number of periods to lag (default: 1)
        
    Returns:
        DataFrame with lagged features
    """
    df_lagged = df.copy()
    
    sector_cols = [col for col in df.columns if col not in ['Year', 'Month']]
    
    for col in sector_cols:
        df_lagged[f'{col}_lag{lag}'] = df_lagged[col].shift(lag)
    
    # Drop rows with NaN from lagging
    df_lagged = df_lagged.dropna().reset_index(drop=True)
    
    print(f"   Created lag-{lag} features: {len(sector_cols)} lagged columns")
    print(f"   Remaining samples after lagging: {len(df_lagged)}")
    
    return df_lagged


def learn_weights(df: pd.DataFrame, method: str = 'regression') -> Dict[Tuple[str, str], float]:
    """
    Main function to learn edge weights from sector risk data.
    
    Args:
        df: DataFrame with sector risk columns
        method: 'correlation' or 'regression' (default: 'regression')
        
    Returns:
        Dictionary: {(source_sector, target_sector): weight}
    """
    if method == 'correlation':
        return learn_weights_correlation(df)
    elif method == 'regression':
        return learn_weights_regression(df, use_temporal=True)
    else:
        raise ValueError(f"Unknown method: {method}. Use 'correlation' or 'regression'")


def compare_methods(df: pd.DataFrame) -> Dict[str, Dict[Tuple[str, str], float]]:
    """
    Compare correlation and regression methods.
    
    Args:
        df: DataFrame with sector risk columns
        
    Returns:
        Dictionary with weights from both methods
    """
    print("\n" + "="*70)
    print("🔍 COMPARING WEIGHT LEARNING METHODS")
    print("="*70)
    
    corr_weights = learn_weights_correlation(df)
    reg_weights = learn_weights_regression(df)
    
    return {
        'correlation': corr_weights,
        'regression': reg_weights
    }


if __name__ == "__main__":
    # Import and load data
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from graph.risk_loader import load_risk_timeseries
    
    # Load data
    df = load_risk_timeseries()
    
    # Compare both methods
    results = compare_methods(df)
    
    print("\n" + "="*70)
    print("✅ WEIGHT LEARNING COMPLETE")
    print("="*70)
