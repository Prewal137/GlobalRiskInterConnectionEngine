"""
📊 Risk Data Loader for Graph Interconnection System

Loads all sector risk outputs and merges them into a unified time series.
Handles both monthly and annual data gracefully.

Author: Global Risk Platform
Date: 2026-04-05
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, Optional, Tuple
from pathlib import Path


# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


# Sector configuration: (folder, filename, risk_column, time_columns)
SECTOR_CONFIG = {
    'climate': {
        'folder': 'climate',
        'filename': 'climate_risk_output.csv',
        'risk_column': 'predicted_risk',
        'has_month': True
    },
    'economy': {
        'folder': 'economy',
        'filename': 'economic_risk_output.csv',
        'risk_column': 'predicted_risk',
        'has_month': True
    },
    'trade': {
        'folder': 'trade',
        'filename': 'trade_risk_output.csv',
        'risk_column': 'Trade_Risk',  # Different column name
        'has_month': False
    },
    'geopolitics': {
        'folder': 'geopolitics',
        'filename': 'geopolitics_risk_output.csv',
        'risk_column': 'risk_score',  # Different column name
        'has_month': True
    },
    'migration': {
        'folder': 'migration',
        'filename': 'migration_risk_output.csv',
        'risk_column': 'migration_risk',  # Different column name
        'has_month': False
    },
    'social': {
        'folder': 'social',
        'filename': 'social_risk_output.csv',
        'risk_column': 'predicted_risk',
        'has_month': True
    },
    'infrastructure': {
        'folder': 'infrastructure',
        'filename': 'infrastructure_risk_output.csv',
        'risk_column': 'predicted_risk',
        'has_month': False  # Annual data only
    }
}


def load_single_sector(sector_name: str, config: Dict) -> Optional[pd.DataFrame]:
    """
    Load risk data for a single sector.
    
    Args:
        sector_name: Name of the sector (e.g., 'climate')
        config: Sector configuration dictionary
        
    Returns:
        DataFrame with columns: ['Year', 'Month', sector_name] or None
    """
    folder = config['folder']
    filename = config['filename']
    risk_column = config['risk_column']
    has_month = config['has_month']
    
    file_path = os.path.join(PROCESSED_DIR, folder, filename)
    
    if not os.path.exists(file_path):
        print(f"   ⚠️  File not found: {file_path}")
        return None
    
    try:
        # Load data
        df = pd.read_csv(file_path)
                
        # Normalize column names (handle case sensitivity)
        if 'year' in df.columns and 'Year' not in df.columns:
            df = df.rename(columns={'year': 'Year'})
        if 'month' in df.columns and 'Month' not in df.columns:
            df = df.rename(columns={'month': 'Month'})
        if 'country' in df.columns and 'Country' not in df.columns:
            df = df.rename(columns={'country': 'Country'})
        if 'state' in df.columns and 'State' not in df.columns:
            df = df.rename(columns={'state': 'State'})
                
        # For climate data (state-level), aggregate to country level
        if sector_name == 'climate':
            if 'State' in df.columns or 'state' in df.columns:
                # Group by Year and Month, take mean across states
                if has_month and 'Month' in df.columns:
                    df = df.groupby(['Year', 'Month'])[risk_column].mean().reset_index()
                else:
                    df = df.groupby('Year')[risk_column].mean().reset_index()
        
        # For India-only sectors (infrastructure), no need to filter
        # For others, check if Country column exists
        if 'Country' in df.columns:
            # If multi-country, take mean or filter
            if df['Country'].nunique() > 1:
                print(f"   ⚠️  Multiple countries detected in {sector_name}, using mean")
                if has_month and 'Month' in df.columns:
                    df = df.groupby(['Year', 'Month'])[risk_column].mean().reset_index()
                else:
                    df = df.groupby('Year')[risk_column].mean().reset_index()
            else:
                # Single country, just keep relevant columns
                cols_to_keep = ['Year']
                if has_month and 'Month' in df.columns:
                    cols_to_keep.append('Month')
                cols_to_keep.append(risk_column)
                df = df[cols_to_keep]
        
        # Add Month column if missing (for annual data)
        if not has_month or 'Month' not in df.columns:
            df['Month'] = 1  # Default to January for annual data
        
        # Rename risk column to sector name
        df = df.rename(columns={risk_column: sector_name})
        
        # Keep only Year, Month, and sector risk
        df = df[['Year', 'Month', sector_name]]
        
        # Remove NaN values
        df = df.dropna()
        
        print(f"   ✅ Loaded {sector_name}: {len(df)} rows, Year range: {df['Year'].min()}-{df['Year'].max()}")
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error loading {sector_name}: {e}")
        return None


def load_risk_timeseries(sectors: Optional[list] = None) -> pd.DataFrame:
    """
    Load and merge all sector risk outputs into a unified time series.
    
    Args:
        sectors: List of sectors to load (default: all sectors)
        
    Returns:
        Merged DataFrame with columns: ['Year', 'Month', 'climate', 'economy', ...]
    """
    print("\n" + "="*70)
    print("📊 LOADING SECTOR RISK TIME SERIES")
    print("="*70)
    
    # Use all sectors if not specified
    if sectors is None:
        sectors = list(SECTOR_CONFIG.keys())
    
    # Load each sector
    sector_dfs = {}
    for sector_name in sectors:
        if sector_name not in SECTOR_CONFIG:
            print(f"   ⚠️  Unknown sector: {sector_name}, skipping")
            continue
        
        config = SECTOR_CONFIG[sector_name]
        df = load_single_sector(sector_name, config)
        
        if df is not None and not df.empty:
            sector_dfs[sector_name] = df
    
    if not sector_dfs:
        raise ValueError("No sector data could be loaded!")
    
    # Merge all sectors on Year and Month
    print("\n🔗 Merging sector data...")
    
    merged_df = None
    for sector_name, df in sector_dfs.items():
        if merged_df is None:
            merged_df = df.copy()
        else:
            # Outer join to keep all time points
            merged_df = merged_df.merge(
                df,
                on=['Year', 'Month'],
                how='outer'
            )
    
    # Sort by time
    merged_df = merged_df.sort_values(['Year', 'Month']).reset_index(drop=True)
    
    # Fill NaN values with forward fill then backward fill
    # (only for the risk columns, not Year/Month)
    risk_cols = [col for col in merged_df.columns if col not in ['Year', 'Month']]
    merged_df[risk_cols] = merged_df[risk_cols].ffill().bfill()
    
    print(f"\n✅ Merged dataset: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
    print(f"   Time range: {merged_df['Year'].min()}/{merged_df['Month'].min()} - {merged_df['Year'].max()}/{merged_df['Month'].max()}")
    print(f"   Sectors loaded: {len(risk_cols)}")
    
    # Check for remaining NaN values
    nan_count = merged_df[risk_cols].isna().sum().sum()
    if nan_count > 0:
        print(f"   ⚠️  Remaining NaN values: {nan_count}")
    else:
        print(f"   ✅ No missing values")
    
    print("="*70)
    
    return merged_df


def load_latest_risk(sectors: Optional[list] = None) -> Dict[str, float]:
    """
    Load the latest risk values for all sectors.
    
    Args:
        sectors: List of sectors to load (default: all sectors)
        
    Returns:
        Dictionary: {sector_name: risk_value}
    """
    merged_df = load_risk_timeseries(sectors)
    
    # Get the latest row
    latest_row = merged_df.iloc[-1]
    
    risk_dict = {}
    for col in merged_df.columns:
        if col not in ['Year', 'Month']:
            risk_dict[col] = float(latest_row[col])
    
    print(f"\n📍 Latest risk values ({int(latest_row['Year'])}/{int(latest_row['Month'])}):")
    for sector, risk in risk_dict.items():
        print(f"   {sector}: {risk:.4f}")
    
    return risk_dict


if __name__ == "__main__":
    # Test loading
    merged_df = load_risk_timeseries()
    print("\n📄 Sample data:")
    print(merged_df.head(10))
    
    print("\n" + "="*70)
    print("📍 Testing latest risk load:")
    risk_dict = load_latest_risk()
    print(f"\n✅ Loaded {len(risk_dict)} sector risks")
