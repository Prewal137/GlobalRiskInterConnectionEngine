"""
🌍 Global Risk Interconnection Engine

This module calculates interconnected risk scores across multiple sectors
by combining climate and trade risks into unified country-level scores.

Sectors:
- Climate Risk (district level → aggregated to country)
- Trade Risk (country level)
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
# 🔧 MERGE DATASEETS
# ================================================================

def merge_risk_datasets(climate_country: pd.DataFrame, trade_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge climate and trade risk datasets on country level.
    
    This function performs an inner join to find matching countries between
    climate and trade datasets. For datasets with different geographical scopes
    (e.g., Indian states vs global countries), it uses fuzzy matching or 
    manual mapping.
    
    Args:
        climate_country (pd.DataFrame): Country/State-level climate risk
        trade_df (pd.DataFrame): Country-level trade risk
        
    Returns:
        pd.DataFrame: Merged risk dataset
    """
    print("\n🔧 Merging climate and trade risk datasets...")
    
    # Strategy 1: Direct inner join for exact matches
    merged_df = pd.merge(climate_country, trade_df, on='Country', how='inner')
    
    if len(merged_df) > 0:
        print(f"   ✅ Direct match found: {len(merged_df)} countries")
        return merged_df
    
    # Strategy 2: If no direct matches, check for special cases
    # Example: Climate data has Indian states, trade has "India"
    print("   ⚠️ No direct matches found. Attempting special case handling...")
    
    # Check if climate data contains Indian states
    indian_states_keywords = ['Andhra', 'Bihar', 'Gujarat', 'Karnataka', 'Kerala', 
                              'Maharashtra', 'Tamil', 'Uttar', 'West Bengal', 'Rajasthan']
    
    is_india_climate = any(any(keyword in str(state) for keyword in indian_states_keywords) 
                          for state in climate_country['Country'].unique())
    
    if is_india_climate:
        # Check if India exists in trade data
        india_trade = trade_df[trade_df['Country'].str.contains('India', case=False, na=False)]
        
        if len(india_trade) > 0:
            print(f"   ✅ Found India in trade data. Mapping Indian states to India's trade risk...")
            
            # Get India's trade risk
            india_risk = india_trade['Trade_Risk'].values[0]
            
            # Add India's trade risk to all Indian states
            climate_country['Trade_Risk'] = india_risk
            
            merged_df = climate_country.copy()
            print(f"   ✅ Mapped {len(merged_df)} Indian states with India's trade risk: {india_risk:.4f}")
    
    return merged_df


# ================================================================
# 🔧 CALCULATE INTERCONNECTED RISK
# ================================================================

def calculate_interconnected_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate interconnected risk scores combining climate and trade risks.
    
    This function creates a unified risk score by:
    1. Computing weighted average of climate and trade risks
    2. Applying cascading risk multipliers for compound hazards
    3. Normalizing final risk scores to [0, 1] range
    
    Args:
        df (pd.DataFrame): DataFrame with 'climate_risk' and 'Trade_Risk' columns
    
    Returns:
        pd.DataFrame: DataFrame with interconnected risk scores
    """
    df = df.copy()
    
    print("\n🔮 Calculating interconnected risks...")
    
    # Step 1: Weighted Risk (equal weights for both sectors)
    df['weighted_risk'] = 0.5 * df['climate_risk'] + 0.5 * df['Trade_Risk']
    print("   ✅ Weighted risk calculated")
    
    # Step 2: Cascading Risk (amplify when both risks are high)
    # If climate_risk > 0.7 AND Trade_Risk > 0.6: amplify by 20%
    df['cascading_risk'] = np.where(
        (df['climate_risk'] > 0.7) & (df['Trade_Risk'] > 0.6),
        df['weighted_risk'] * 1.2,
        df['weighted_risk']
    )
    print("   ✅ Cascading risk calculated (compound hazard multiplier applied)")
    
    # Step 3: Normalize Final Risk to [0, 1]
    min_risk = df['cascading_risk'].min()
    max_risk = df['cascading_risk'].max()
    
    # Avoid division by zero
    if max_risk - min_risk > 0:
        df['final_risk'] = (df['cascading_risk'] - min_risk) / (max_risk - min_risk)
    else:
        df['final_risk'] = df['cascading_risk']
    
    print("   ✅ Final risk normalized to [0, 1] range")
    
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
    Run the interconnection engine combining climate and trade risks.
    
    Returns:
        pd.DataFrame: DataFrame with all interconnected risk scores
    """
    print("\n" + "="*60)
    print("🌍 RUNNING GLOBAL RISK INTERCONNECTION ENGINE")
    print("="*60)
    
    # Step 1: Aggregate climate to country level
    climate_country = aggregate_climate_to_country(climate_df)
    
    # Step 2: Merge datasets
    merged_df = merge_risk_datasets(climate_country, trade_df)
    
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
    
    # Select relevant columns for output
    output_df = df[['Country', 'climate_risk', 'Trade_Risk', 'weighted_risk', 
                    'cascading_risk', 'final_risk', 'risk_level']].copy()
    
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Interconnection data saved → {output_path}")
    
    # Display top 10 high-risk countries
    print("\n🔴 TOP 10 HIGHEST RISK COUNTRIES:")
    print("-" * 80)
    top_10 = df.nlargest(10, 'final_risk')
    for idx, row in top_10.iterrows():
        print(f"   {row['Country']:35s} | Final: {row['final_risk']:.4f} ({row['risk_level']})")
        print(f"      Climate: {row['climate_risk']:.4f} | Trade: {row['Trade_Risk']:.4f} | "
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
