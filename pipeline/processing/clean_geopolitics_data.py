"""
🧹 Geopolitics Data Cleaning (Phase 3)

Converts multiple geopolitics datasets into a unified time-series format.
Final format: Country | Year | Month | features

Input: 
  - data/raw/geopolitics/
  - data/processed/geopolitics/geopolitics_selected_files.txt

Output:
  - data/processed/geopolitics/final_geopolitics.csv
"""

import pandas as pd
import os
import numpy as np
from pathlib import Path

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
GEOPOLITICS_RAW = os.path.join(BASE_PATH, "data", "raw", "geopolitics")
GEOPOLITICS_PROCESSED = os.path.join(BASE_PATH, "data", "processed", "geopolitics")
SELECTED_FILES = os.path.join(GEOPOLITICS_PROCESSED, "geopolitics_selected_files.txt")
OUTPUT_FILE = os.path.join(GEOPOLITICS_PROCESSED, "final_geopolitics.csv")


# ================================================================
# 🌍 COUNTRY STANDARDIZATION FUNCTIONS
# ================================================================

def convert_to_iso3(country_name):
    """
    Convert country name to ISO-3 code using pycountry with custom mappings.
    
    Args:
        country_name: Country name or code
    
    Returns:
        ISO-3 country code or original name if conversion fails
    """
    try:
        import pycountry
        
        # Custom mappings for problematic names
        custom_mappings = {
            'Brunei': 'BRN',
            'East Timor': 'TLS',
            'Ivory Coast': 'CIV',
            'Kosovo': 'XKX',  # Not officially recognized but commonly used
            'Palestine': 'PSE',
            'Russia': 'RUS',
            'Turkey': 'TUR',
            'Vatican City': 'VAT',
            'Kingdom of eSwatini': 'SWZ',
            'Swaziland': 'SWZ',
            'Myanmar': 'MMR',
            'Burma': 'MMR',
            'DR Congo': 'COD',
            'Zaire': 'COD',
            'South Sudan': 'SSD',
            'Sudan': 'SDN',
            'Iran': 'IRN',
            'North Yemen': 'YEM',
            'Yemen': 'YEM',
        }
        
        # Try direct lookup first
        try:
            return pycountry.countries.lookup(country_name).alpha_3
        except (AttributeError, KeyError, LookupError):
            pass
        
        # Try custom mappings
        for key, code in custom_mappings.items():
            if key.lower() in str(country_name).lower():
                return code
        
        # If all fails, return original name
        return country_name
        
    except Exception:
        return country_name


def convert_countries_to_iso3(df):
    """
    Convert all country names in dataframe to ISO-3 codes.
    
    Args:
        df: DataFrame with 'Country' column
    
    Returns:
        DataFrame with standardized country codes
    """
    print(f"      📊 Unique countries before conversion: {df['Country'].nunique()}")
    
    # Apply conversion
    df['Country'] = df['Country'].apply(convert_to_iso3)
    
    # Remove duplicates after conversion (e.g., "India" and "IND" both become "IND")
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['Country', 'Year', 'Month'])
    removed = initial_rows - len(df)
    print(f"      ✅ Converted to ISO-3, removed {removed} duplicate rows")
    print(f"      📊 Unique countries after conversion: {df['Country'].nunique()}")
    
    # Show sample conversions
    print(f"      📋 Sample country codes: {sorted(df['Country'].unique())[:10]}")
    
    return df


def create_complete_monthly_grid(df):
    """
    Create complete monthly grid for all Country × Year combinations.
    
    Args:
        df: DataFrame with Country, Year, Month columns
    
    Returns:
        DataFrame with complete monthly grid (12 months per year)
    """
    print(f"      📊 Shape before grid creation: {df.shape}")
    
    # Get unique countries and years
    unique_countries = df['Country'].unique()
    unique_years = df['Year'].unique()
    all_months = range(1, 13)
    
    # Create complete MultiIndex
    complete_index = pd.MultiIndex.from_product(
        [unique_countries, unique_years, all_months],
        names=["Country", "Year", "Month"]
    )
    
    # Set index and reindex to complete grid
    df_indexed = df.set_index(["Country", "Year", "Month"])
    df_complete = df_indexed.reindex(complete_index)
    
    # Reset index
    df_complete = df_complete.reset_index()
    
    # Fill missing values
    print(f"         Filling missing values in complete grid...")
    
    # Fill conflict/fatality columns with 0
    zero_fill_cols = ['conflict_count', 'fatalities_sum', 'deaths_total']
    for col in zero_fill_cols:
        if col in df_complete.columns:
            df_complete[col] = df_complete[col].fillna(0).astype(int)
    
    # Forward fill uncertainty columns within each country
    ffill_cols = ['policy_uncertainty', 'global_uncertainty', 'conflict_intensity']
    for col in ffill_cols:
        if col in df_complete.columns:
            df_complete[col] = df_complete.groupby('Country')[col].ffill()
    
    # Fill any remaining NaN with 0
    df_complete = df_complete.fillna(0)
    
    print(f"      ✅ Created complete monthly grid: {df_complete.shape}")
    print(f"      📅 Years covered: {df_complete['Year'].min()} - {df_complete['Year'].max()}")
    print(f"      📊 Months per year: {df_complete.groupby(['Country', 'Year']).size().unique()[0]} (should be 12)")
    
    return df_complete


# ================================================================
# 📂 DATA LOADING FUNCTIONS
# ================================================================

def load_selected_files():
    """Load list of selected files from Phase 2."""
    print("\n📂 Loading selected files list...")
    
    with open(SELECTED_FILES, 'r') as f:
        lines = f.readlines()
    
    # Extract file names (skip comments and separators)
    selected = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith('='):
            # Remove numbering (e.g., "1. ")
            if '. ' in line:
                line = line.split('. ', 1)[1]
            selected.append(line)
    
    print(f"✅ Found {len(selected)} selected files:")
    for f in selected:
        print(f"   - {f}")
    
    return selected


def load_acled_data():
    """
    Load and process ACLED conflict event data.
    
    Processing:
    - Use event_date column
    - Extract Year, Month
    - Use country column
    - Aggregate by Country + Year + Month
    """
    print("\n🔹 Loading ACLED Data...")
    
    file_path = os.path.join(GEOPOLITICS_RAW, "ACLED Data_2026-04-02.csv")
    
    try:
        df = pd.read_csv(file_path)
        
        # Convert event_date to datetime
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
        
        # Extract Year and Month
        df['Year'] = df['event_date'].dt.year
        df['Month'] = df['event_date'].dt.month
        
        # Use country column
        df['Country'] = df['country'].str.strip() if df['country'].dtype == 'object' else df['country']
        
        # Aggregate by Country + Year + Month
        aggregated = df.groupby(['Country', 'Year', 'Month']).agg(
            conflict_count=('event_id_cnty', 'count'),
            fatalities_sum=('fatalities', 'sum')
        ).reset_index()
        
        print(f"   ✅ Loaded {len(aggregated)} rows (Country-Year-Month combinations)")
        print(f"   📅 Year range: {aggregated['Year'].min()} - {aggregated['Year'].max()}")
        print(f"   🌍 Countries: {aggregated['Country'].nunique()}")
        
        return aggregated
    
    except Exception as e:
        print(f"   ❌ Error loading ACLED: {e}")
        return None


def load_ged_event_data():
    """
    Load and process GED Event data.
    
    Processing:
    - Use year column
    - Use country column
    - Convert yearly to monthly (duplicate for 12 months)
    - Create deaths_total from 'best' column
    """
    print("\n🔹 Loading GEDEvent Data...")
    
    file_path = os.path.join(GEOPOLITICS_RAW, "GEDEvent_v25_1.csv")
    
    try:
        df = pd.read_csv(file_path)
        
        # Use year and country columns
        df['Year'] = df['year']
        df['Country'] = df['country'].str.strip() if df['country'].dtype == 'object' else df['country']
        
        # Create deaths_total from 'best' column
        df['deaths_total'] = df['best']
        
        # Keep only needed columns
        df = df[['Country', 'Year', 'deaths_total']].drop_duplicates()
        
        # Convert yearly to monthly (duplicate for 12 months)
        monthly_dfs = []
        for month in range(1, 13):
            month_df = df.copy()
            month_df['Month'] = month
            monthly_dfs.append(month_df)
        
        monthly = pd.concat(monthly_dfs, ignore_index=True)
        
        print(f"   ✅ Loaded {len(monthly)} rows (converted to monthly)")
        print(f"   📅 Year range: {monthly['Year'].min()} - {monthly['Year'].max()}")
        print(f"   🌍 Countries: {monthly['Country'].nunique()}")
        
        return monthly
    
    except Exception as e:
        print(f"   ❌ Error loading GEDEvent: {e}")
        return None


def load_ucdp_prio_data():
    """
    Load and process UCDP/PRIO Conflict data.
    
    Processing:
    - Use year column
    - Use location or territory_name (map to Country)
    - Convert yearly to monthly
    - Create conflict_intensity from intensity_level
    """
    print("\n🔹 Loading UCDP/PRIO Data...")
    
    file_path = os.path.join(GEOPOLITICS_RAW, "UcdpPrioConflict_v25_1.csv")
    
    try:
        df = pd.read_csv(file_path)
        
        # Use year column
        df['Year'] = df['year']
        
        # Use location or territory_name as Country
        # Prefer 'location' if available, otherwise use 'territory_name'
        if 'location' in df.columns:
            df['Country'] = df['location'].str.strip() if df['location'].dtype == 'object' else df['location']
        elif 'territory_name' in df.columns:
            df['Country'] = df['territory_name'].str.strip() if df['territory_name'].dtype == 'object' else df['territory_name']
        else:
            print("   ⚠️  No location/territory column found, using 'Unknown'")
            df['Country'] = 'Unknown'
        
        # Create conflict_intensity from intensity_level
        df['conflict_intensity'] = df['intensity_level']
        
        # Keep only needed columns
        df = df[['Country', 'Year', 'conflict_intensity']].drop_duplicates()
        
        # Convert yearly to monthly (duplicate for 12 months)
        monthly_dfs = []
        for month in range(1, 13):
            month_df = df.copy()
            month_df['Month'] = month
            monthly_dfs.append(month_df)
        
        monthly = pd.concat(monthly_dfs, ignore_index=True)
        
        print(f"   ✅ Loaded {len(monthly)} rows (converted to monthly)")
        print(f"   📅 Year range: {monthly['Year'].min()} - {monthly['Year'].max()}")
        print(f"   🌍 Countries: {monthly['Country'].nunique()}")
        
        return monthly
    
    except Exception as e:
        print(f"   ❌ Error loading UCDP/PRIO: {e}")
        return None


def load_policy_uncertainty_data():
    """
    Load and process India Policy Uncertainty data.
    ROBUST VERSION: Filter only numeric Year rows to auto-clean metadata.
    """
    print("\n🔹 Loading Policy Uncertainty Data (Robust)...")
    
    file_path = os.path.join(GEOPOLITICS_RAW, "India_Policy_Uncertainty_Data.xlsx")
    
    try:
        import pandas as pd
        
        df = pd.read_excel(file_path)
        
        print(f"   📊 Raw shape: {df.shape}")
        print(f"   📋 Columns: {list(df.columns)}")
        
        # Keep only rows where Year is numeric (auto-cleans metadata)
        year_col = df.columns[0]  # First column should be Year
        df = df[df[year_col].astype(str).str.isnumeric()]
        
        print(f"   🧹 After filtering non-numeric years: {df.shape}")
        
        # Convert types
        df["Year"] = df["Year"].astype(int)
        df["Month"] = df["Month"].astype(int)
        
        # Rename column dynamically (third column is policy uncertainty)
        df = df.rename(columns={df.columns[2]: "policy_uncertainty"})
        
        # Add Country
        df["Country"] = "India"
        
        # Select final columns
        df = df[["Country", "Year", "Month", "policy_uncertainty"]]
        
        print(f"   ✅ Loaded {len(df)} rows")
        print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
        
        return df
    
    except Exception as e:
        print(f"   ❌ Error loading Policy Uncertainty: {e}")
        import traceback
        traceback.print_exc()
        return None


def load_wui_data():
    """
    Load and process World Uncertainty Index (WUI) data.
    CORRECTED VERSION: Use T2 sheet with proper melt() transformation.
    """
    print("\n🔹 Loading WUI Data (Corrected)...")
    
    file_path = os.path.join(GEOPOLITICS_RAW, "WUI_Data.xlsx")
    
    try:
        import pandas as pd
        
        # Load T2 sheet which contains country-level data
        df = pd.read_excel(file_path, sheet_name="T2")
        
        print(f"   📊 Raw shape: {df.shape}")
        print(f"   📋 Columns (first 10): {list(df.columns)[:10]}")
        
        # Rename first column to 'Quarter'
        df = df.rename(columns={df.columns[0]: "Quarter"})
        
        # Melt wide → long format
        df = df.melt(id_vars=["Quarter"], var_name="Country_Code", value_name="global_uncertainty")
        
        # Drop rows with NaN values
        df = df.dropna(subset=["global_uncertainty"])
        
        print(f"   📊 After melting: {df.shape}")
        
        # Extract year + quarter from Quarter column (e.g., "1990q1")
        df["Year"] = df["Quarter"].str.extract(r"(\d{4})").astype(int)
        df["Quarter_Num"] = df["Quarter"].str.extract(r"q(\d)", expand=False).astype(int)
        
        # Convert quarter → month
        # Q1 (Jan-Mar) → Month 1, Q2 (Apr-Jun) → Month 4, Q3 (Jul-Sep) → Month 7, Q4 (Oct-Dec) → Month 10
        df["Month"] = (df["Quarter_Num"] - 1) * 3 + 1
        
        # Drop the Quarter and Quarter_Num columns
        df = df.drop(columns=["Quarter", "Quarter_Num"])
        
        # Drop any remaining rows with NaN in global_uncertainty
        df = df.dropna(subset=["global_uncertainty"])
        
        # Remove fake countries (rows where Country_Code contains patterns like "2000q")
        df = df[~df["Country_Code"].str.contains(r"\d{4}q", na=False)]
        
        # Rename Country_Code to Country for consistency
        df = df.rename(columns={"Country_Code": "Country"})
        
        print(f"   🌍 Countries: {df['Country'].nunique()}")
        print(f"   📅 Year range: {df['Year'].min()} - {df['Year'].max()}")
        print(f"   📊 Sample countries: {sorted(df['Country'].unique())[:10]}")
        
        return df[["Country", "Year", "Month", "global_uncertainty"]]
    
    except Exception as e:
        print(f"   ❌ Error loading WUI: {e}")
        import traceback
        traceback.print_exc()
        return None


# ================================================================
# 🔗 MERGING FUNCTIONS
# ================================================================

def merge_datasets(datasets_dict):
    """
    Merge all datasets on Country + Year + Month using outer join.
    
    Args:
        datasets_dict: Dictionary of {name: dataframe}
    
    Returns:
        pd.DataFrame: Merged dataset
    """
    print("\n🔗 Merging datasets...")
    
    # Start with ACLED (primary dataset)
    merged = datasets_dict.get('acled')
    if merged is None:
        print("   ❌ No primary dataset (ACLED) to start merging!")
        return None
    
    print(f"   Starting with ACLED: {merged.shape}")
    
    # Merge other datasets
    merge_order = ['ged_event', 'ucdp_prio', 'policy_uncertainty', 'wui']
    
    for name in merge_order:
        if name in datasets_dict and datasets_dict[name] is not None:
            df_to_merge = datasets_dict[name]
            
            print(f"   Merging with {name}: {df_to_merge.shape}")
            
            merged = pd.merge(
                merged,
                df_to_merge,
                on=['Country', 'Year', 'Month'],
                how='outer'
            )
            
            print(f"   → Result: {merged.shape}")
    
    return merged


def clean_merged_data(df):
    """
    Apply cleaning rules to merged dataset.
    
    Rules:
    1. Remove useless early years (Year >= 2000)
    2. Convert country names to ISO-3 codes
    3. Fill missing values with 0
    4. Forward fill uncertainty/intensity metrics WITHIN EACH COUNTRY
    5. Ensure Year, Month = int
    6. Remove duplicates
    7. Remove fake countries (containing patterns like "2000q")
    8. Create complete monthly grid (Country × Year × Month 1-12)
    """
    print("\n🧹 Applying cleaning rules...")
    
    # Remove useless early years - keep only Year >= 2000
    print(f"   📅 Filtering years < 2000...")
    initial_rows = len(df)
    df = df[df['Year'] >= 2000].copy()
    removed_years = initial_rows - len(df)
    print(f"   ✅ Removed {removed_years} rows with Year < 2000")
    
    # Remove fake countries (rows where Country contains patterns like "2000q")
    print(f"   🧹 Removing fake countries...")
    initial_rows = len(df)
    df = df[~df['Country'].str.contains(r"\d{4}q", na=False)]
    removed_fake = initial_rows - len(df)
    print(f"   ✅ Removed {removed_fake} rows with fake country names")
    
    # Convert country names to ISO-3 codes
    print(f"   🌍 Converting country names to ISO-3 codes...")
    df = convert_countries_to_iso3(df)
    
    # Fill missing values with 0
    fill_zero_cols = ['conflict_count', 'fatalities_sum', 'deaths_total']
    for col in fill_zero_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
            print(f"   ✅ Filled {col} with 0")
    
    # Forward fill uncertainty and intensity metrics WITHIN EACH COUNTRY
    forward_fill_cols = ['policy_uncertainty', 'conflict_intensity', 'global_uncertainty']
    for col in forward_fill_cols:
        if col in df.columns:
            # Sort before forward fill
            df = df.sort_values(['Country', 'Year', 'Month'])
            df[col] = df.groupby('Country')[col].ffill()
            print(f"   ✅ Forward filled {col} within each country")
    
    # Fill remaining NaN values with 0 (for policy_uncertainty since only India has it)
    if 'policy_uncertainty' in df.columns:
        df['policy_uncertainty'] = df['policy_uncertainty'].fillna(0)
        print(f"   ✅ Filled remaining NaN in policy_uncertainty with 0")
    
    # Ensure Year and Month are integers
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)
    print(f"   ✅ Converted Year/Month to int")
    
    # Remove duplicates
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['Country', 'Year', 'Month'])
    removed = initial_rows - len(df)
    print(f"   ✅ Removed {removed} duplicate rows")
    
    # Create complete monthly grid (Country × Year × Month 1-12)
    print(f"   📅 Creating complete monthly grid...")
    df = create_complete_monthly_grid(df)
    
    # Sort by Country, Year, Month
    df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
    print(f"   ✅ Sorted by Country, Year, Month")
    
    return df


# ================================================================
# 📤 OUTPUT FUNCTIONS
# ================================================================

def save_final_output(df):
    """Save final cleaned dataset to CSV."""
    print("\n📤 Saving final output...")
    
    # Ensure directory exists
    os.makedirs(GEOPOLITICS_PROCESSED, exist_ok=True)
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"   ✅ Saved to {OUTPUT_FILE}")
    print(f"   📊 Shape: {df.shape}")


def print_final_summary(df):
    """Print final summary statistics."""
    print("\n" + "="*70)
    print("🎉 PHASE 3 COMPLETE - GEOPOLITICS DATA CLEANING")
    print("="*70)
    
    print(f"\n📊 FINAL DATASET SHAPE: {df.shape}")
    
    print(f"\n🌍 NUMBER OF COUNTRIES: {df['Country'].nunique()}")
    if df['Country'].nunique() < 20:
        print(f"   Countries: {sorted(df['Country'].unique())}")
    
    print(f"\n📅 NUMBER OF YEARS: {df['Year'].nunique()}")
    print(f"   Year range: {df['Year'].min()} - {df['Year'].max()}")
    
    print(f"\n📋 LIST OF COLUMNS ({len(df.columns)} total):")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i}. {col}")
    
    # Count non-null values for key columns
    print(f"\n📈 NON-NULL VALUE COUNTS:")
    if 'policy_uncertainty' in df.columns:
        non_null_policy = df['policy_uncertainty'].notna().sum()
        print(f"   ✓ policy_uncertainty: {non_null_policy} rows ({non_null_policy/len(df)*100:.1f}%)")
    
    if 'global_uncertainty' in df.columns:
        non_null_wui = df['global_uncertainty'].notna().sum()
        print(f"   ✓ global_uncertainty: {non_null_wui} rows ({non_null_wui/len(df)*100:.1f}%)")
    
    if 'conflict_intensity' in df.columns:
        non_null_intensity = df['conflict_intensity'].notna().sum()
        print(f"   ✓ conflict_intensity: {non_null_intensity} rows ({non_null_intensity/len(df)*100:.1f}%)")
    
    print(f"\n📈 SAMPLE DATA (first 5 rows):")
    print(df.head().to_string(index=False))
    
    print("\n" + "="*70)


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function for Phase 3."""
    print("\n🚀 Starting Geopolitics Data Cleaning (Phase 3)...")
    print("="*70)
    
    # Step 1: Load selected files list
    selected_files = load_selected_files()
    
    # Step 2: Load individual datasets
    datasets = {}
    
    # Load each dataset if it's in selected files
    if "ACLED Data_2026-04-02.csv" in selected_files:
        datasets['acled'] = load_acled_data()
    
    if "GEDEvent_v25_1.csv" in selected_files:
        datasets['ged_event'] = load_ged_event_data()
    
    if "UcdpPrioConflict_v25_1.csv" in selected_files:
        datasets['ucdp_prio'] = load_ucdp_prio_data()
    
    if "India_Policy_Uncertainty_Data.xlsx" in selected_files:
        datasets['policy_uncertainty'] = load_policy_uncertainty_data()
    
    if "WUI_Data.xlsx" in selected_files:
        datasets['wui'] = load_wui_data()
    
    # Check if we have at least one dataset
    if not any(v is not None for v in datasets.values()):
        print("\n❌ No datasets loaded! Exiting...")
        return
    
    # Step 3: Merge datasets
    merged = merge_datasets(datasets)
    
    if merged is None:
        print("\n❌ Merging failed! Exiting...")
        return
    
    # Step 4: Clean merged data
    cleaned = clean_merged_data(merged)
    
    # Step 5: Save final output
    save_final_output(cleaned)
    
    # Step 6: Print final summary
    print_final_summary(cleaned)
    
    print("\n✅ Phase 3 completed successfully!")
    print(f"📄 Final output: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error during Phase 3: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
