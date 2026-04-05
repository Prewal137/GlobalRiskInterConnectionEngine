"""
🔗 Interconnection Engine - Multi-Sector Risk Integration

Combines climate, economy, trade, geopolitics, migration, social, and infrastructure risk data to simulate
cascading effects and calculate global interconnected risk.

Input Files:
    - data/processed/climate/climate_risk_output.csv (State/District level)
    - data/processed/economy/economic_risk_final.csv (Country level)
    - data/processed/trade/trade_risk_output.csv (Country level, annual)
    - data/processed/geopolitics/geopolitics_risk_output.csv (Country level)
    - data/processed/migration/migration_risk_output.csv (Country level, annual)
    - data/processed/social/social_risk_output.csv (State level, monthly)
    - data/processed/infrastructure/infrastructure_risk_output.csv (State level, annual)

Output:
    - data/processed/interconnection/interconnected_risk.csv

Key Features:
    - Aggregates climate risk from State/District to Country level
    - Aggregates social risk from State to Country level
    - Aggregates infrastructure risk from State to Country level
    - Standardizes column names across sectors
    - Merges on Country + Year + Month (inner join)
    - Applies cascading impact formulas including migration, social, and infrastructure influences
    - Normalizes all risks to 0-1 scale
"""

import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Go up 3 levels: services/ → app/ → backend/ → project_root/
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

INPUT_FILES = {
    'climate': os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_output.csv"),
    'economy': os.path.join(BASE_PATH, "data", "processed", "economy", "economic_risk_final.csv"),
    'trade': os.path.join(BASE_PATH, "data", "processed", "trade", "trade_risk_output.csv"),
    'geopolitics': os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output.csv"),
    'migration': os.path.join(BASE_PATH, "data", "processed", "migration", "migration_risk_output.csv"),
    'social': os.path.join(BASE_PATH, "data", "processed", "social", "social_risk_output.csv"),
    'infrastructure': os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_risk_output.csv")
}

OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "interconnection", "interconnected_risk.csv")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ================================================================
# 🔧 STEP 1: LOAD DATA
# ================================================================

def load_all_data():
    """
    Load all 5 sector datasets with validation.
    
    Returns:
        dict: Dictionary of DataFrames by sector
    """
    print("\n" + "="*70)
    print("📥 STEP 1: LOADING ALL SECTOR DATA")
    print("="*70)
    
    data = {}
    
    for sector, filepath in INPUT_FILES.items():
        if not os.path.exists(filepath):
            print(f"   ⚠️  WARNING: {sector} file not found: {filepath}")
            continue
        
        df = pd.read_csv(filepath)
        data[sector] = df
        
        print(f"   ✅ {sector:<15s}: {len(df):>6,} rows, {len(df.columns)} columns")
        print(f"      Columns: {list(df.columns)}")
    
    return data

# ================================================================
# 🔧 STEP 2: STANDARDIZE CLIMATE DATA
# ================================================================

def standardize_climate(df):
    """
    Aggregate climate risk from State/District level to Country level.
    
    Since climate data is for Indian states/districts, we:
    1. Group by Year + Month
    2. Calculate MEDIAN risk across all states/districts (robust to outliers)
    3. Assign Country = 'IND'
    4. Normalize to [0, 1] using MinMaxScaler
    
    Args:
        df (pd.DataFrame): Climate risk output
        
    Returns:
        pd.DataFrame: Standardized climate data with Country, Year, Month, climate_risk
    """
    print("\n" + "="*70)
    print("🌍 STEP 2: STANDARDIZING CLIMATE DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    print(f"   States: {df['State'].nunique()}")
    print(f"   Districts: {df['District'].nunique()}")
    
    # Group by Year + Month and aggregate using MEDIAN (robust to outliers)
    climate_agg = df.groupby(['Year', 'Month'])['predicted_risk'].median().reset_index()
    climate_agg.columns = ['Year', 'Month', 'climate_risk']
    
    # Add Country column
    climate_agg['Country'] = 'IND'
    
    # Reorder columns
    climate_agg = climate_agg[['Country', 'Year', 'Month', 'climate_risk']]
    
    # Normalize to [0, 1] BEFORE merging
    min_val = climate_agg['climate_risk'].min()
    max_val = climate_agg['climate_risk'].max()
    if max_val > min_val:
        climate_agg['climate_risk'] = (climate_agg['climate_risk'] - min_val) / (max_val - min_val)
    
    print(f"   ✅ Aggregated to country level (using MEDIAN)")
    print(f"   ✅ Normalized to [0, 1] range")
    print(f"   Final shape: {climate_agg.shape}")
    print(f"   Date range: {climate_agg['Year'].min()}/{climate_agg['Month'].min()} - {climate_agg['Year'].max()}/{climate_agg['Month'].max()}")
    print(f"   Risk range: [{climate_agg['climate_risk'].min():.4f}, {climate_agg['climate_risk'].max():.4f}]")
    
    return climate_agg

# ================================================================
# 🔧 STEP 3: STANDARDIZE ECONOMY DATA
# ================================================================

def standardize_economy(df):
    """
    Rename economy risk column to standard format and normalize.
    
    Args:
        df (pd.DataFrame): Economy risk final
        
    Returns:
        pd.DataFrame: Standardized economy data with normalized risks
    """
    print("\n" + "="*70)
    print("💰 STEP 3: STANDARDIZING ECONOMY DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    
    # Select only needed columns
    df_std = df[['Country', 'Year', 'Month', 'predicted_risk']].copy()
    df_std.columns = ['Country', 'Year', 'Month', 'economic_risk']
    
    # Normalize to [0, 1] BEFORE merging
    min_val = df_std['economic_risk'].min()
    max_val = df_std['economic_risk'].max()
    if max_val > min_val:
        df_std['economic_risk'] = (df_std['economic_risk'] - min_val) / (max_val - min_val)
    
    print(f"   ✅ Renamed predicted_risk → economic_risk")
    print(f"   ✅ Normalized to [0, 1] range")
    print(f"   Final shape: {df_std.shape}")
    print(f"   Risk range: [{df_std['economic_risk'].min():.4f}, {df_std['economic_risk'].max():.4f}]")
    
    return df_std

# ================================================================
# 🔧 STEP 4: STANDARDIZE TRADE DATA
# ================================================================

def standardize_trade(df):
    """
    Standardize trade data (annual → monthly by forward fill).
    
    Trade data is annual, so we:
    1. Rename Trade_Risk → trade_risk
    2. Standardize country codes (e.g., 'India' → 'IND')
    3. Expand to monthly by repeating each year 12 times
    4. Normalize AFTER expansion (will be done globally before merge)
    5. Assign Month 1-12
    
    Args:
        df (pd.DataFrame): Trade risk output
        
    Returns:
        pd.DataFrame: Standardized trade data with monthly frequency
    """
    print("\n" + "="*70)
    print("🚢 STEP 4: STANDARDIZING TRADE DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    print(f"   Countries: {df['Country'].nunique()}")
    print(f"   Years: {df['Year'].min()} - {df['Year'].max()}")
    
    # Rename column
    df_std = df[['Country', 'Year', 'Trade_Risk']].copy()
    df_std.columns = ['Country', 'Year', 'trade_risk']
    
    # Standardize country codes
    country_mapping = {
        'India': 'IND',
        'United States': 'USA',
        'China': 'CHN',
        # Add more mappings as needed
    }
    
    df_std['Country'] = df_std['Country'].replace(country_mapping)
    print(f"   ✅ Standardized country codes (e.g., India → IND)")
    
    # Expand to monthly FIRST
    monthly_rows = []
    for _, row in df_std.iterrows():
        for month in range(1, 13):
            monthly_rows.append({
                'Country': row['Country'],
                'Year': int(row['Year']),
                'Month': month,
                'trade_risk': float(row['trade_risk'])
            })
    
    df_monthly = pd.DataFrame(monthly_rows)
    
    # Normalize PER-COUNTRY after expansion
    # This ensures each country's risk variation is preserved
    normalized_rows = []
    for country in df_monthly['Country'].unique():
        country_mask = df_monthly['Country'] == country
        country_data = df_monthly[country_mask].copy()
        
        min_val = country_data['trade_risk'].min()
        max_val = country_data['trade_risk'].max()
        
        if max_val > min_val:
            country_data['trade_risk'] = (country_data['trade_risk'] - min_val) / (max_val - min_val)
        else:
            # If all values are the same, set to 0.5 (neutral)
            country_data['trade_risk'] = 0.5
        
        normalized_rows.append(country_data)
    
    df_monthly = pd.concat(normalized_rows, ignore_index=True)
    
    print(f"   ✅ Expanded annual → monthly")
    print(f"   ✅ Normalized per-country to [0, 1] range")
    print(f"   Final shape: {df_monthly.shape}")
    print(f"   Risk range: [{df_monthly['trade_risk'].min():.4f}, {df_monthly['trade_risk'].max():.4f}]")
    
    return df_monthly

# ================================================================
# 🔧 STEP 5: STANDARDIZE GEOPOLITICS DATA
# ================================================================

def standardize_geopolitics(df):
    """
    Rename geopolitics risk column to standard format and normalize.
    
    Args:
        df (pd.DataFrame): Geopolitics risk output
        
    Returns:
        pd.DataFrame: Standardized geopolitics data with normalized risks
    """
    print("\n" + "="*70)
    print("🌐 STEP 5: STANDARDIZING GEOPOLITICS DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    
    # Select only needed columns
    df_std = df[['Country', 'Year', 'Month', 'risk_score']].copy()
    df_std.columns = ['Country', 'Year', 'Month', 'geopolitical_risk']
    
    # Normalize to [0, 1] BEFORE merging (per-country normalization)
    normalized_rows = []
    for country in df_std['Country'].unique():
        country_mask = df_std['Country'] == country
        country_data = df_std[country_mask].copy()
        
        min_val = country_data['geopolitical_risk'].min()
        max_val = country_data['geopolitical_risk'].max()
        
        if max_val > min_val:
            country_data['geopolitical_risk'] = (country_data['geopolitical_risk'] - min_val) / (max_val - min_val)
        else:
            country_data['geopolitical_risk'] = 0.5
        
        normalized_rows.append(country_data)
    
    df_std = pd.concat(normalized_rows, ignore_index=True)
    
    print(f"   ✅ Renamed risk_score → geopolitical_risk")
    print(f"   ✅ Normalized per-country to [0, 1] range")
    print(f"   Final shape: {df_std.shape}")
    print(f"   Risk range: [{df_std['geopolitical_risk'].min():.4f}, {df_std['geopolitical_risk'].max():.4f}]")
    
    return df_std

# ================================================================
# 🔧 STEP 6: STANDARDIZE MIGRATION DATA
# ================================================================

def standardize_migration(df):
    """
    Standardize migration data (annual → monthly by forward fill).
    
    Migration data is annual, so we:
    1. Rename migration_risk column (already standardized)
    2. Standardize country codes if needed
    3. Expand to monthly by repeating each year 12 times
    4. Normalize per-country AFTER expansion
    5. Assign Month 1-12
    
    Args:
        df (pd.DataFrame): Migration risk output
        
    Returns:
        pd.DataFrame: Standardized migration data with monthly frequency
    """
    print("\n" + "="*70)
    print("🚶 STEP 6: STANDARDIZING MIGRATION DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    print(f"   Countries: {df['Country'].nunique()}")
    print(f"   Years: {df['Year'].min()} - {df['Year'].max()}")
    
    # Select only needed columns
    df_std = df[['Country', 'Year', 'migration_risk']].copy()
    
    # Standardize country codes (if needed)
    country_mapping = {
        'India': 'IND',
        'United States': 'USA',
        'China': 'CHN',
        # Add more mappings as needed
    }
    
    df_std['Country'] = df_std['Country'].replace(country_mapping)
    print(f"   ✅ Standardized country codes")
    
    # Expand to monthly FIRST
    monthly_rows = []
    for _, row in df_std.iterrows():
        for month in range(1, 13):
            monthly_rows.append({
                'Country': row['Country'],
                'Year': int(row['Year']),
                'Month': month,
                'migration_risk': float(row['migration_risk'])
            })
    
    df_monthly = pd.DataFrame(monthly_rows)
    
    # Normalize PER-COUNTRY after expansion
    normalized_rows = []
    for country in df_monthly['Country'].unique():
        country_mask = df_monthly['Country'] == country
        country_data = df_monthly[country_mask].copy()
        
        min_val = country_data['migration_risk'].min()
        max_val = country_data['migration_risk'].max()
        
        if max_val > min_val:
            country_data['migration_risk'] = (country_data['migration_risk'] - min_val) / (max_val - min_val)
        else:
            # If all values are the same, set to 0.5 (neutral)
            country_data['migration_risk'] = 0.5
        
        normalized_rows.append(country_data)
    
    df_monthly = pd.concat(normalized_rows, ignore_index=True)
    
    print(f"   ✅ Expanded annual → monthly")
    print(f"   ✅ Normalized per-country to [0, 1] range")
    print(f"   Final shape: {df_monthly.shape}")
    print(f"   Risk range: [{df_monthly['migration_risk'].min():.4f}, {df_monthly['migration_risk'].max():.4f}]")
    
    return df_monthly

# ================================================================
# 🔧 STEP 7: STANDARDIZE SOCIAL DATA
# ================================================================

def standardize_social(df):
    """
    Aggregate social risk from State level to Country level.
    
    Since social data is for Indian states, we:
    1. Group by Year + Month
    2. Calculate MEDIAN risk across all states (robust to outliers)
    3. Assign Country = 'IND'
    4. Normalize to [0, 1] using MinMaxScaler
    
    Args:
        df (pd.DataFrame): Social risk output
        
    Returns:
        pd.DataFrame: Standardized social data with Country, Year, Month, social_risk
    """
    print("\n" + "="*70)
    print("👥 STEP 7: STANDARDIZING SOCIAL DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    print(f"   States: {df['State'].nunique()}")
    
    # Group by Year + Month and aggregate using MEDIAN (robust to outliers)
    social_agg = df.groupby(['Year', 'Month'])['predicted_risk'].median().reset_index()
    social_agg.columns = ['Year', 'Month', 'social_risk']
    
    # Add Country column
    social_agg['Country'] = 'IND'
    
    # Reorder columns
    social_agg = social_agg[['Country', 'Year', 'Month', 'social_risk']]
    
    # Normalize to [0, 1] BEFORE merging
    min_val = social_agg['social_risk'].min()
    max_val = social_agg['social_risk'].max()
    if max_val > min_val:
        social_agg['social_risk'] = (social_agg['social_risk'] - min_val) / (max_val - min_val)
    
    print(f"   ✅ Aggregated to country level (using MEDIAN)")
    print(f"   ✅ Normalized to [0, 1] range")
    print(f"   Final shape: {social_agg.shape}")
    print(f"   Date range: {social_agg['Year'].min()}/{social_agg['Month'].min()} - {social_agg['Year'].max()}/{social_agg['Month'].max()}")
    print(f"   Risk range: [{social_agg['social_risk'].min():.4f}, {social_agg['social_risk'].max():.4f}]")
    
    return social_agg

# ================================================================
# 🔧 STEP 8: STANDARDIZE INFRASTRUCTURE DATA
# ================================================================

def standardize_infrastructure(df):
    """
    Aggregate infrastructure risk from State level to Country level.
    
    Since infrastructure data is for Indian states, we:
    1. Group by Year (infrastructure is annual)
    2. Calculate MEDIAN risk across all states (robust to outliers)
    3. Assign Country = 'IND'
    4. Add Month column (default = 1 for annual data)
    5. Normalize to [0, 1] using MinMaxScaler
    
    Args:
        df (pd.DataFrame): Infrastructure risk output
        
    Returns:
        pd.DataFrame: Standardized infrastructure data with Country, Year, Month, infrastructure_risk
    """
    print("\n" + "="*70)
    print("🏗️  STEP 8: STANDARDIZING INFRASTRUCTURE DATA")
    print("="*70)
    
    print(f"   Original shape: {df.shape}")
    print(f"   States: {df['state'].nunique()}")
    
    # Group by Year and aggregate using MEDIAN (robust to outliers)
    infra_agg = df.groupby(['year'])['predicted_risk'].median().reset_index()
    infra_agg.columns = ['Year', 'infrastructure_risk']
    
    # Add Country column
    infra_agg['Country'] = 'IND'
    
    # Add Month column (default = 1 for annual data)
    infra_agg['Month'] = 1
    
    # Reorder columns
    infra_agg = infra_agg[['Country', 'Year', 'Month', 'infrastructure_risk']]
    
    # Normalize to [0, 1] BEFORE merging
    min_val = infra_agg['infrastructure_risk'].min()
    max_val = infra_agg['infrastructure_risk'].max()
    if max_val > min_val:
        infra_agg['infrastructure_risk'] = (infra_agg['infrastructure_risk'] - min_val) / (max_val - min_val)
    
    print(f"   ✅ Aggregated to country level (using MEDIAN)")
    print(f"   ✅ Added Month column (default = 1)")
    print(f"   ✅ Normalized to [0, 1] range")
    print(f"   Final shape: {infra_agg.shape}")
    print(f"   Year range: {infra_agg['Year'].min()} - {infra_agg['Year'].max()}")
    print(f"   Risk range: [{infra_agg['infrastructure_risk'].min():.4f}, {infra_agg['infrastructure_risk'].max():.4f}]")
    
    return infra_agg

# ================================================================
# 🔧 STEP 9: MERGE ALL SECTORS
# ================================================================

def merge_all_sectors(climate, economy, trade, geopolitics, migration, social, infrastructure):
    """
    Merge all 7 sectors on Country + Year + Month using INNER JOIN.
    
    This ensures we only keep time periods where ALL sectors have data.
    
    Args:
        climate (pd.DataFrame): Standardized climate data
        economy (pd.DataFrame): Standardized economy data
        trade (pd.DataFrame): Standardized trade data
        geopolitics (pd.DataFrame): Standardized geopolitics data
        migration (pd.DataFrame): Standardized migration data
        social (pd.DataFrame): Standardized social data
        infrastructure (pd.DataFrame): Standardized infrastructure data
        
    Returns:
        pd.DataFrame: Merged dataset
    """
    print("\n" + "="*70)
    print("🔗 STEP 9: MERGING ALL SECTORS")
    print("="*70)
    
    # Start with climate (smallest dataset - India only)
    merged = climate.copy()
    print(f"   Starting with climate: {len(merged)} rows")
    
    # Merge economy
    merged = merged.merge(economy, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging economy: {len(merged)} rows")
    
    # Merge trade
    merged = merged.merge(trade, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging trade: {len(merged)} rows")
    
    # Merge geopolitics
    merged = merged.merge(geopolitics, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging geopolitics: {len(merged)} rows")
    
    # Merge migration
    merged = merged.merge(migration, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging migration: {len(merged)} rows")
    
    # Merge social
    merged = merged.merge(social, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging social: {len(merged)} rows")
    
    # Merge infrastructure
    merged = merged.merge(infrastructure, on=['Country', 'Year', 'Month'], how='inner')
    print(f"   After merging infrastructure: {len(merged)} rows")
    
    print(f"\n   ✅ INNER JOIN completed (only common timeline)")
    print(f"   Final shape: {merged.shape}")
    print(f"   Countries: {merged['Country'].unique()}")
    print(f"   Date range: {merged['Year'].min()}/{merged['Month'].min()} - {merged['Year'].max()}/{merged['Month'].max()}")
    
    return merged

# ================================================================
# 🔧 STEP 9: APPLY INTERCONNECTION LOGIC
# ================================================================

def apply_interconnection_logic(df):
    """
    Apply cascading risk formulas including migration, social, and infrastructure influences:
    
    economic_impact = 0.35 * climate_risk + 0.22 * geopolitical_risk + 0.18 * migration_risk + 0.13 * social_risk + 0.12 * infrastructure_risk
    trade_impact = 0.30 * economic_risk + 0.22 * geopolitical_risk + 0.22 * migration_risk + 0.13 * social_risk + 0.13 * infrastructure_risk
    global_risk = 0.18 * climate_risk + 0.18 * economic_risk + 0.10 * trade_risk + 0.16 * geopolitical_risk + 0.14 * migration_risk + 0.12 * social_risk + 0.12 * infrastructure_risk
    
    Migration influences:
        - migration → trade influence (people movement affects labor, supply chains)
        - migration → global risk (social stability, demographic shifts)
    
    Social influences:
        - social → economic impact (unrest affects productivity, investment)
        - social → trade impact (protests disrupt logistics, exports)
        - social → global risk (civil stability indicator)
    
    Infrastructure influences:
        - infrastructure → economic impact (poor infrastructure reduces productivity)
        - infrastructure → trade impact (logistics bottlenecks affect trade)
        - infrastructure → global risk (development capacity indicator)
    
    Args:
        df (pd.DataFrame): Merged dataset
        
    Returns:
        pd.DataFrame: Dataset with impact columns added
    """
    print("\n" + "="*70)
    print("⚡ STEP 10: APPLYING INTERCONNECTION LOGIC")
    print("="*70)
    
    df = df.copy()
    
    # Economic impact (climate + geopolitics + migration + social + infrastructure affect economy)
    df['economic_impact'] = (
        0.35 * df['climate_risk'] + 
        0.22 * df['geopolitical_risk'] + 
        0.18 * df['migration_risk'] + 
        0.13 * df['social_risk'] + 
        0.12 * df['infrastructure_risk']
    )
    print(f"   ✅ economic_impact = 0.35×climate + 0.22×geopolitical + 0.18×migration + 0.13×social + 0.12×infrastructure")
    
    # Trade impact (economy + geopolitics + migration + social + infrastructure affect trade)
    df['trade_impact'] = (
        0.30 * df['economic_risk'] + 
        0.22 * df['geopolitical_risk'] + 
        0.22 * df['migration_risk'] + 
        0.13 * df['social_risk'] + 
        0.13 * df['infrastructure_risk']
    )
    print(f"   ✅ trade_impact = 0.30×economic + 0.22×geopolitical + 0.22×migration + 0.13×social + 0.13×infrastructure")
    
    # Global risk (weighted combination of all 7 sectors)
    df['global_risk'] = (
        0.18 * df['climate_risk'] +
        0.18 * df['economic_risk'] +
        0.10 * df['trade_risk'] +
        0.16 * df['geopolitical_risk'] +
        0.14 * df['migration_risk'] +
        0.12 * df['social_risk'] +
        0.12 * df['infrastructure_risk']
    )
    print(f"   ✅ global_risk = 0.18×climate + 0.18×economic + 0.10×trade + 0.16×geopolitical + 0.14×migration + 0.12×social + 0.12×infrastructure")
    
    return df

# ================================================================
# 🔧 STEP 9: VALIDATE NORMALIZATION (No re-normalization!)
# ================================================================

def validate_normalization(df):
    """
    Validate that all risk columns are already in [0, 1] range.
    
    NOTE: We do NOT normalize here - each sector was normalized BEFORE merging.
    This preserves signal and prevents extreme values.
    
    Args:
        df (pd.DataFrame): Dataset with risk columns
        
    Returns:
        pd.DataFrame: Same dataset (no changes)
    """
    print("\n" + "="*70)
    print("📏 STEP 11: VALIDATING NORMALIZATION")
    print("="*70)
    
    # Columns to check
    risk_cols = [
        'climate_risk', 'economic_risk', 'trade_risk', 'geopolitical_risk', 
        'migration_risk', 'social_risk', 'infrastructure_risk'
    ]
    
    print(f"   ✅ All sectors were normalized BEFORE merging")
    print(f"   ✅ No post-merge normalization applied (preserves signal)")
    print(f"   Range check:")
    for col in risk_cols:
        min_val = df[col].min()
        max_val = df[col].max()
        status = "✅" if 0 <= min_val and max_val <= 1 else "⚠️"
        print(f"      {status} {col:<25s}: [{min_val:.4f}, {max_val:.4f}]")
    
    return df

# ================================================================
# 🔧 STEP 11: FINAL OUTPUT
# ================================================================

def create_final_output(df):
    """
    Create final structured output with correct column order.
    
    Final columns:
        Country, Year, Month,
        climate_risk, economic_risk, trade_risk, geopolitical_risk, migration_risk, social_risk, infrastructure_risk,
        economic_impact, trade_impact, global_risk
    
    Args:
        df (pd.DataFrame): Processed dataset
        
    Returns:
        pd.DataFrame: Final output
    """
    print("\n" + "="*70)
    print("📋 STEP 12: CREATING FINAL OUTPUT")
    print("="*70)
    
    # Define final column order
    final_columns = [
        'Country', 'Year', 'Month',
        'climate_risk', 'economic_risk', 'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ]
    
    df_final = df[final_columns].copy()
    
    # Ensure correct data types
    df_final['Year'] = df_final['Year'].astype(int)
    df_final['Month'] = df_final['Month'].astype(int)
    
    for col in final_columns[3:]:  # All risk columns
        df_final[col] = df_final[col].astype(float)
    
    print(f"   ✅ Final structure created")
    print(f"   Shape: {df_final.shape}")
    print(f"   Columns: {list(df_final.columns)}")
    
    # Display sample
    print(f"\n   📄 SAMPLE OUTPUT (first 5 rows):")
    print("-" * 160)
    print(df_final.head(5).to_string(index=False))
    print("-" * 160)
    
    return df_final

# ================================================================
# 🔧 STEP 12: SAVE OUTPUT
# ================================================================

def save_output(df, output_path=OUTPUT_FILE):
    """
    Save final interconnected risk data to CSV.
    
    Args:
        df (pd.DataFrame): Final dataset
        output_path (str): Output file path
    """
    print("\n" + "="*70)
    print("💾 STEP 11: SAVING OUTPUT")
    print("="*70)
    
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(output_path) / 1024
    print(f"   ✅ Output saved to: {output_path}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Rows: {len(df):,}")
    print(f"   📋 Columns: {len(df.columns)}")

# ================================================================
# 🔧 VALIDATION
# ================================================================

def validate_output(df):
    """
    Perform quality checks on final output.
    
    Args:
        df (pd.DataFrame): Final dataset
    """
    print("\n" + "="*70)
    print("✅ VALIDATION & QUALITY CHECKS")
    print("="*70)
    
    issues = []
    
    # Check 1: No missing values
    missing = df.isna().sum().sum()
    if missing > 0:
        issues.append(f"Found {missing} missing values")
    else:
        print(f"   ✅ No missing values")
    
    # Check 2: Base risks in [0, 1] (normalized before merge)
    base_risk_cols = ['climate_risk', 'economic_risk', 'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk']
    out_of_range = False
    for col in base_risk_cols:
        if df[col].min() < 0 or df[col].max() > 1:
            issues.append(f"{col} out of [0,1] range: [{df[col].min():.4f}, {df[col].max():.4f}]")
            out_of_range = True
    
    if not out_of_range:
        print(f"   ✅ All base risks normalized to [0, 1]")
    
    # Check 3: Derived impacts should be reasonable (not all 0s or 1s)
    derived_cols = ['economic_impact', 'trade_impact', 'global_risk']
    for col in derived_cols:
        unique_vals = df[col].nunique()
        if unique_vals <= 2:
            issues.append(f"{col} has only {unique_vals} unique values (loss of signal)")
        else:
            print(f"   ✅ {col} has good variation ({unique_vals} unique values)")
    
    # Check 4: Time ordering
    is_sorted = df.equals(df.sort_values(['Country', 'Year', 'Month']))
    if not is_sorted:
        issues.append("Data not properly sorted")
    else:
        print(f"   ✅ Time ordering preserved")
    
    # Summary
    print("\n" + "-"*70)
    if len(issues) == 0:
        print("   ALL VALIDATION CHECKS PASSED!")
    else:
        print(f"   {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"      - {issue}")
    print("-"*70)

# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    print("\n" + "="*70)
    print("🔗 INTERCONNECTION ENGINE - MULTI-SECTOR RISK INTEGRATION")
    print("="*70)
    
    try:
        # ========================================
        # STEP 1: LOAD DATA
        # ========================================
        data = load_all_data()
        
        # Verify all sectors loaded
        required_sectors = ['climate', 'economy', 'trade', 'geopolitics', 'migration', 'social', 'infrastructure']
        missing_sectors = [s for s in required_sectors if s not in data]
        
        if missing_sectors:
            raise ValueError(f"Missing required sectors: {missing_sectors}")
        
        # ========================================
        # STEPS 2-8: STANDARDIZE EACH SECTOR
        # ========================================
        climate_std = standardize_climate(data['climate'])
        economy_std = standardize_economy(data['economy'])
        trade_std = standardize_trade(data['trade'])
        geopolitics_std = standardize_geopolitics(data['geopolitics'])
        migration_std = standardize_migration(data['migration'])
        social_std = standardize_social(data['social'])
        infrastructure_std = standardize_infrastructure(data['infrastructure'])
        
        # ========================================
        # STEP 9: MERGE ALL SECTORS
        # ========================================
        merged = merge_all_sectors(climate_std, economy_std, trade_std, geopolitics_std, migration_std, social_std, infrastructure_std)
        
        if len(merged) == 0:
            raise ValueError("Merge resulted in empty dataset! Check date ranges.")
        
        # ========================================
        # STEP 10: APPLY INTERCONNECTION LOGIC
        # ========================================
        interconnected = apply_interconnection_logic(merged)
        
        # ========================================
        # STEP 11: VALIDATE (No re-normalization!)
        # ========================================
        validated = validate_normalization(interconnected)
        
        # ========================================
        # STEP 12: CREATE FINAL OUTPUT
        # ========================================
        df_final = create_final_output(validated)
        
        # ========================================
        # STEP 13: SAVE
        # ========================================
        save_output(df_final)
        
        # ========================================
        # VALIDATION
        # ========================================
        validate_output(df_final)
        
        # ========================================
        # FINAL SUMMARY
        # ========================================
        print("\n" + "="*70)
        print("✅ INTERCONNECTION ENGINE COMPLETE")
        print("="*70)
        
        print(f"\n   📊 FINAL SUMMARY:")
        print(f"      Total records: {len(df_final):,}")
        print(f"      Countries: {df_final['Country'].nunique()}")
        print(f"      Date range: {df_final['Year'].min()}/{df_final['Month'].min()} - {df_final['Year'].max()}/{df_final['Month'].max()}")
        
        print(f"\n   📋 SECTORS INTEGRATED:")
        print(f"      ✅ Climate Risk (aggregated from State/District)")
        print(f"      ✅ Economic Risk")
        print(f"      ✅ Trade Risk (expanded to monthly)")
        print(f"      ✅ Geopolitical Risk")
        print(f"      ✅ Migration Risk (expanded to monthly)")
        print(f"      ✅ Social Risk (aggregated from State)")
        print(f"      ✅ Infrastructure Risk (aggregated from State)")
        
        print(f"\n   ⚡ CASCADING EFFECTS CALCULATED:")
        print(f"      ✅ Economic Impact (climate + geopolitics + migration + social + infrastructure)")
        print(f"      ✅ Trade Impact (economy + geopolitics + migration + social + infrastructure)")
        print(f"      ✅ Global Risk (all 7 sectors)")
        
        print(f"\n   💾 OUTPUT FILE:")
        print(f"      {OUTPUT_FILE}")
        
        print("\n" + "="*70)
        print("🎯 INTERCONNECTED RISK READY FOR ANALYSIS")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
