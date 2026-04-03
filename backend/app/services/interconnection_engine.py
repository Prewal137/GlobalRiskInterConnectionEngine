"""
🌍 Global Risk Interconnection Engine

This module calculates interconnected risk scores across multiple sectors
by combining climate, trade, and geopolitics risks into unified country-level scores.

Sectors:
- Climate Risk (district level → aggregated to country)
- Trade Risk (country level)
- Geopolitics Risk (country level)
- Combined Interconnected Risk
"""

import pandas as pd
import numpy as np
import os

# ================================================================
# 📂 LOAD DATA
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# Climate data (district level)
climate_path = os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_district.csv")
climate_df = pd.read_csv(climate_path)

print("✅ Climate data loaded for interconnection analysis!")
print(f"   Districts: {len(climate_df)}")
print(f"   Mean Climate Risk: {climate_df['predicted_risk'].mean():.4f}")

# Trade data (country level)
trade_path = os.path.join(BASE_PATH, "data", "processed", "trade", "trade_risk_country.csv")
trade_df = pd.read_csv(trade_path)

print("\n✅ Trade data loaded for interconnection analysis!")
print(f"   Countries: {len(trade_df)}")
print(f"   Mean Trade Risk: {trade_df['Trade_Risk'].mean():.4f}")

# Geopolitics data (country level)
geopolitics_path = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output_country.csv")
geopolitics_df = pd.read_csv(geopolitics_path)

print("\n✅ Geopolitics data loaded for interconnection analysis!")
print(f"   Countries: {len(geopolitics_df)}")
print(f"   Mean Geopolitics Risk: {geopolitics_df['latest_risk'].mean():.4f}")

# ================================================================
# 🔧 AGGREGATE CLIMATE TO COUNTRY LEVEL
# ================================================================

def aggregate_climate_to_country(climate_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate district-level climate risk to country/state level.
    
    Args:
        climate_df (pd.DataFrame): District-level climate risk data
        
    Returns:
        pd.DataFrame: Country/State-level aggregated climate risk
    """
    print("\n🔧 Aggregating climate risk to country/state level...")
    
    # Group by State and calculate mean predicted_risk
    climate_country = climate_df.groupby('State')['predicted_risk'].mean().reset_index()
    climate_country.columns = ['Country', 'climate_risk']
    
    print(f"   ✅ Aggregated {len(climate_df)} districts to {len(climate_country)} regions")
    print(f"   ✅ Mean climate risk: {climate_country['climate_risk'].mean():.4f}")
    
    return climate_country


# ================================================================
# 🔧 MERGE DATASETS
# ================================================================

def merge_risk_datasets(climate_country: pd.DataFrame, trade_df: pd.DataFrame, geopolitics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge climate, trade, and geopolitics risk datasets on country level.
    
    This function performs sequential merges to combine all three risk sources.
    Handles special case where climate data contains Indian states.
    
    Args:
        climate_country (pd.DataFrame): Country/State-level climate risk
        trade_df (pd.DataFrame): Country-level trade risk
        geopolitics_df (pd.DataFrame): Country-level geopolitics risk
        
    Returns:
        pd.DataFrame: Merged risk dataset with all three sectors
    """
    print("\n🔧 Merging climate, trade, and geopolitics risk datasets...")
    
    # Check if climate data contains Indian states
    indian_states_keywords = ['Andhra', 'Bihar', 'Gujarat', 'Karnataka', 'Kerala', 
                              'Maharashtra', 'Tamil', 'Uttar', 'West Bengal', 'Rajasthan']
    
    is_india_climate = any(any(keyword in str(state) for keyword in indian_states_keywords) 
                          for state in climate_country['Country'].unique())
    
    if is_india_climate:
        print("   ℹ️  Climate data contains Indian states. Mapping to India...")
        
        # Aggregate Indian states to single India entry
        india_climate_risk = climate_country['climate_risk'].mean()
        climate_country = pd.DataFrame({'Country': ['India'], 'climate_risk': [india_climate_risk]})
        print(f"   ✅ Aggregated Indian states to India (climate risk: {india_climate_risk:.4f})")
    
    # Step 1: Merge climate and trade
    merged_df = pd.merge(climate_country, trade_df, on='Country', how='inner')
    print(f"   ✅ Climate-Trade match: {len(merged_df)} countries")
    
    # Step 2: Merge with geopolitics
    merged_df = pd.merge(merged_df, geopolitics_df[['Country', 'latest_risk']], 
                         on='Country', how='inner')
    merged_df.columns = ['Country', 'climate_risk', 'Trade_Risk', 'geopolitics_risk']
    
    print(f"   ✅ Climate-Trade-Geopolitics match: {len(merged_df)} countries")
    print(f"      Mean Climate Risk: {merged_df['climate_risk'].mean():.4f}")
    print(f"      Mean Trade Risk: {merged_df['Trade_Risk'].mean():.4f}")
    print(f"      Mean Geopolitics Risk: {merged_df['geopolitics_risk'].mean():.4f}")
    
    return merged_df


# ================================================================
# 🔧 CALCULATE INTERCONNECTED RISK
# ================================================================

def calculate_interconnected_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate interconnected risk scores combining climate, trade, and geopolitics risks.
    
    This function creates a unified risk score by:
    1. Normalizing each sector to [0, 1] scale (CRITICAL for fair weighting!)
    2. Computing weighted average of all three sector risks
    3. Applying cascading risk multipliers for compound hazards
    4. Normalizing final risk scores to [0, 1] range
    
    WHY NORMALIZE FIRST?
    Climate risk range: [0.02, 0.24] - Very compressed!
    Trade risk range:   [0.01, 0.83] - Full spread
    Geo risk range:     [0.03, 0.93] - Full spread
    
    Without normalization, climate would be marginalized despite 30% weight.
    
    Interconnection weights (equal importance after normalization):
    - Climate: 33.3%
    - Trade: 33.3%
    - Geopolitics: 33.4%
    
    Args:
        df (pd.DataFrame): DataFrame with 'climate_risk', 'Trade_Risk', and 'geopolitics_risk' columns
    
    Returns:
        pd.DataFrame: DataFrame with interconnected risk scores
    """
    df = df.copy()
    
    print("\n🔮 Calculating interconnected risks (Climate + Trade + Geopolitics)...")
    
    # ================================================================
    # 🔧 STEP 0: NORMALIZE EACH SECTOR TO [0, 1] (CRITICAL FIX!)
    # ================================================================
    print("   ⚠️  CRITICAL: Normalizing sectors before combination...")
    print(f"      Climate range before: [{df['climate_risk'].min():.4f}, {df['climate_risk'].max():.4f}]")
    print(f"      Trade range before:   [{df['Trade_Risk'].min():.4f}, {df['Trade_Risk'].max():.4f}]")
    print(f"      Geo range before:     [{df['geopolitics_risk'].min():.4f}, {df['geopolitics_risk'].max():.4f}]")
    
    # Normalize each sector independently
    for sector_col in ['climate_risk', 'Trade_Risk', 'geopolitics_risk']:
        min_val = df[sector_col].min()
        max_val = df[sector_col].max()
        
        if max_val - min_val > 0:
            df[f'{sector_col}_norm'] = (df[sector_col] - min_val) / (max_val - min_val)
        else:
            df[f'{sector_col}_norm'] = df[sector_col]
    
    print(f"      All sectors normalized to [0, 1] ✅")
    
    # ================================================================
    # 🔧 STEP 1: WEIGHTED RISK (using normalized values)
    # ================================================================
    print("\n   📊 Calculating weighted risk (equal weights after normalization)...")
    
    # Equal weights since all sectors now on same scale
    df['weighted_risk'] = (0.333 * df['climate_risk_norm'] + 
                          0.333 * df['Trade_Risk_norm'] + 
                          0.334 * df['geopolitics_risk_norm'])
    print("   ✅ Weighted risk calculated (Climate:33.3%, Trade:33.3%, Geopolitics:33.4%)")
    
    # ================================================================
    # 🔧 STEP 2: CASCADING RISK (using normalized values)
    # ================================================================
    print("\n   🔗 Calculating cascading risk multiplier...")
    
    # Compound hazard multiplier: if 2+ sectors have high risk (>0.6), amplify by 25%
    # Now using normalized values so threshold is meaningful across all sectors
    df['high_risk_count'] = ((df['climate_risk_norm'] > 0.6).astype(int) + 
                            (df['Trade_Risk_norm'] > 0.6).astype(int) + 
                            (df['geopolitics_risk_norm'] > 0.6).astype(int))
    
    df['cascading_risk'] = np.where(
        df['high_risk_count'] >= 2,
        df['weighted_risk'] * 1.25,  # 25% amplification for compound risks
        df['weighted_risk']
    )
    print("   ✅ Cascading risk calculated (compound hazard multiplier: 25% when 2+ sectors high)")
    
    # ================================================================
    # 🔧 STEP 3: FINAL NORMALIZATION TO [0, 1]
    # ================================================================
    print("\n   ⚖️  Normalizing final risk to [0, 1] range...")
    
    min_risk = df['cascading_risk'].min()
    max_risk = df['cascading_risk'].max()
    
    # Avoid division by zero
    if max_risk - min_risk > 0:
        df['final_risk'] = (df['cascading_risk'] - min_risk) / (max_risk - min_risk)
    else:
        df['final_risk'] = df['cascading_risk']
    
    print("   ✅ Final risk normalized to [0, 1] range")
    
    # Print summary statistics
    print(f"\n   📊 Final Risk Statistics:")
    print(f"      Min: {df['final_risk'].min():.4f}")
    print(f"      Max: {df['final_risk'].max():.4f}")
    print(f"      Mean: {df['final_risk'].mean():.4f}")
    
    return df


def get_risk_level(score: float) -> str:
    """
    Classify risk level based on normalized score (0-1).
    
    Args:
        score (float): Normalized risk score (0.0 to 1.0)
    
    Returns:
        str: Risk level category
    """
    if score < 0.05:
        return "VERY LOW"
    elif score < 0.10:
        return "LOW"
    elif score < 0.20:
        return "MEDIUM"
    elif score < 0.40:
        return "HIGH"
    else:
        return "VERY HIGH"

# ================================================================
# 🚀 RUN ENGINE
# ================================================================

def run_interconnection():
    """
    Run the interconnection engine combining climate, trade, and geopolitics risks.
    
    Returns:
        pd.DataFrame: DataFrame with all interconnected risk scores
    """
    print("\n" + "="*60)
    print("🌍 RUNNING GLOBAL RISK INTERCONNECTION ENGINE (CLIMATE + TRADE + GEOPOLITICS)")
    print("="*60)
    
    # Step 1: Aggregate climate to country level
    climate_country = aggregate_climate_to_country(climate_df)
    
    # Step 2: Merge datasets (climate + trade + geopolitics)
    merged_df = merge_risk_datasets(climate_country, trade_df, geopolitics_df)
    
    # Step 3: Handle missing values
    merged_df = merged_df.dropna()
    print(f"\n🔧 Cleaned dataset: {len(merged_df)} countries (after dropping NaN)")
    
    # Step 4: Calculate interconnected risk
    df = calculate_interconnected_risk(merged_df)
    
    # Step 5: Apply risk level classification
    df['risk_level'] = df['final_risk'].apply(get_risk_level)
    print("   ✅ Risk levels classified")
    
    print("\n📊 INTERCONNECTION ANALYSIS COMPLETE")
    print("-" * 60)
    print(f"   Total countries analyzed: {len(df)}")
    print(f"   Mean Climate Risk: {df['climate_risk'].mean():.4f}")
    print(f"   Mean Trade Risk: {df['Trade_Risk'].mean():.4f}")
    print(f"   Mean Geopolitics Risk: {df['geopolitics_risk'].mean():.4f}")
    print(f"   Mean Final Risk: {df['final_risk'].mean():.4f}")
    print("="*60)
    
    return df

# ================================================================
# 💾 SAVE OUTPUT
# ================================================================

def save_output(df: pd.DataFrame):
    """
    Save interconnected risk data to CSV.
    
    Args:
        df (pd.DataFrame): DataFrame with interconnected risk scores
    """
    output_path = os.path.join(
        BASE_PATH, 
        "data", 
        "processed", 
        "interconnection", 
        "global_risk.csv"
    )
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Select relevant columns for output (includes normalized and original values)
    output_df = df[['Country', 'climate_risk', 'climate_risk_norm', 
                    'Trade_Risk', 'Trade_Risk_norm',
                    'geopolitics_risk', 'geopolitics_risk_norm',
                    'weighted_risk', 'cascading_risk', 'final_risk', 'risk_level']].copy()
    
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Interconnection data saved → {output_path}")
    
    # Display top 10 high-risk countries
    print("\n🔴 TOP 10 HIGHEST RISK COUNTRIES:")
    print("-" * 80)
    top_10 = df.nlargest(10, 'final_risk')
    for idx, row in top_10.iterrows():
        print(f"   {row['Country']:35s} | Final: {row['final_risk']:.4f} ({row['risk_level']})")
        print(f"      Climate: {row['climate_risk']:.4f}→{row['climate_risk_norm']:.4f} | "
              f"Trade: {row['Trade_Risk']:.4f}→{row['Trade_Risk_norm']:.4f} | "
              f"Geo: {row['geopolitics_risk']:.4f}→{row['geopolitics_risk_norm']:.4f} | "
              f"Weighted: {row['weighted_risk']:.4f} | Cascading: {row['cascading_risk']:.4f}")
        print()

# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    df = run_interconnection()
    save_output(df)
    
    print("\n" + "="*60)
    print("🎉 INTERCONNECTION ENGINE EXECUTION COMPLETE")
    print("="*60)
