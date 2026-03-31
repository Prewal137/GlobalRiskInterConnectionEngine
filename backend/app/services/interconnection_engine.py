"""
🌍 Global Risk Interconnection Engine

This module calculates interconnected risk scores across multiple sectors
based on climate risk predictions.

Sectors:
- Climate Risk (input)
- Migration Risk
- Economic Risk
- Infrastructure Risk
"""

import pandas as pd
import os

# ================================================================
# 📂 LOAD CLIMATE DATA
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

climate_path = os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_district.csv")

climate_df = pd.read_csv(climate_path)

print("✅ Climate data loaded for interconnection analysis!")
print(f"   Districts: {len(climate_df)}")
print(f"   Mean Climate Risk: {climate_df['predicted_risk'].mean():.4f}")

# ================================================================
# 🔧 DEFINE INTERCONNECTION LOGIC
# ================================================================

def calculate_interconnected_risk(df):
    """
    Calculate interconnected risk scores across multiple sectors.
    
    This function applies sector-specific multipliers to climate risk
    to estimate cascading risks in migration, economy, and infrastructure.
    
    Args:
        df (pd.DataFrame): DataFrame with 'predicted_risk' column
    
    Returns:
        pd.DataFrame: DataFrame with additional risk columns
    """
    df = df.copy()
    
    print("\n🔮 Calculating interconnected risks...")
    
    # Migration risk increases with climate stress
    # Multiplier: 0.7 (strong correlation)
    df['migration_risk'] = df['predicted_risk'] * 0.7
    
    # Economic risk slightly dependent on climate
    # Multiplier: 0.5 (moderate correlation)
    df['economic_risk'] = df['predicted_risk'] * 0.5
    
    # Infrastructure risk depends on extreme climate
    # Multiplier: 0.6 (significant impact)
    df['infrastructure_risk'] = df['predicted_risk'] * 0.6
    
    # Calculate composite risk score (average of all sectors)
    df['composite_risk'] = df[[
        'predicted_risk',
        'migration_risk',
        'economic_risk',
        'infrastructure_risk'
    ]].mean(axis=1)
    
    print("   ✅ Migration risk calculated")
    print("   ✅ Economic risk calculated")
    print("   ✅ Infrastructure risk calculated")
    print("   ✅ Composite risk calculated")
    
    return df

def get_risk_level(score):
    """
    Classify risk level based on normalized score (0-1).
    
    Args:
        score (float): Normalized risk score (0.0 to 1.0)
    
    Returns:
        str: Risk level category
    """
    if score < 0.05:
        return "VERY LOW"
    elif score < 0.08:
        return "LOW"
    elif score < 0.12:
        return "MEDIUM"
    elif score < 0.20:
        return "HIGH"
    else:
        return "VERY HIGH"

# ================================================================
# 🚀 RUN ENGINE
# ================================================================

def run_interconnection():
    """
    Run the interconnection engine on climate risk data.
    
    Returns:
        pd.DataFrame: DataFrame with all interconnected risk scores
    """
    print("\n" + "="*60)
    print("🌍 RUNNING GLOBAL RISK INTERCONNECTION ENGINE")
    print("="*60)
    
    df = climate_df.copy()
    
    df = calculate_interconnected_risk(df)
    
    print("\n📊 INTERCONNECTION ANALYSIS COMPLETE")
    print("-" * 60)
    print(f"   Total districts analyzed: {len(df)}")
    print(f"   Mean Climate Risk: {df['predicted_risk'].mean():.4f}")
    print(f"   Mean Migration Risk: {df['migration_risk'].mean():.4f}")
    print(f"   Mean Economic Risk: {df['economic_risk'].mean():.4f}")
    print(f"   Mean Infrastructure Risk: {df['infrastructure_risk'].mean():.4f}")
    print(f"   Mean Composite Risk: {df['composite_risk'].mean():.4f}")
    print("="*60)
    
    return df

# ================================================================
# 💾 SAVE OUTPUT
# ================================================================

def save_output(df):
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
        "interconnected_risk.csv"
    )
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Interconnection data saved → {output_path}")
    
    # Display top 5 high-risk districts
    print("\n🔴 TOP 5 HIGH-RISK DISTRICTS (Composite Score):")
    print("-" * 70)
    top_5 = df.nlargest(5, 'composite_risk')
    for idx, row in top_5.iterrows():
        print(f"   {row['District']:30s} ({row['State']})")
        print(f"      Climate: {row['predicted_risk']:.4f} | "
              f"Migration: {row['migration_risk']:.4f} | "
              f"Economic: {row['economic_risk']:.4f} | "
              f"Infrastructure: {row['infrastructure_risk']:.4f}")
        print(f"      → Composite Risk: {row['composite_risk']:.4f} ({get_risk_level(row['composite_risk'])})")
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
