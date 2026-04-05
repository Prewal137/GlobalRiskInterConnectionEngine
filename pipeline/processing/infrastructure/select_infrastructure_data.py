"""
🎯 Infrastructure Sector Data Selection Script

Filters and selects high-quality infrastructure datasets based on:
- Keyword relevance (electricity, water, sanitation, etc.)
- Temporal coverage (multi-year data)
- Data quality (missing values < 40%)
- No duplicates

Input:  data/processed/infrastructure/infrastructure_summary.csv
Output: data/processed/infrastructure/selected_files.csv
        data/processed/infrastructure/selected_raw/ (copied files)
"""

import os
import pandas as pd
import shutil
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 3 levels from pipeline/processing/infrastructure/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

SUMMARY_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_summary.csv")
SELECTED_FILES_CSV = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "selected_files.csv")
RAW_FOLDER = os.path.join(BASE_PATH, "data", "raw", "infrastructure")
SELECTED_RAW_FOLDER = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "selected_raw")

# Ensure output directories exist
os.makedirs(os.path.dirname(SELECTED_FILES_CSV), exist_ok=True)
os.makedirs(SELECTED_RAW_FOLDER, exist_ok=True)

print("="*70)
print("🎯 INFRASTRUCTURE SECTOR DATA SELECTION")
print("="*70)
print(f"\n📂 Loading summary from: {SUMMARY_FILE}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def contains_any_keyword(text, keywords):
    """
    Check if text contains any of the given keywords (case-insensitive).
    
    Args:
        text (str): Text to search in
        keywords (list): List of keywords to check
        
    Returns:
        bool: True if any keyword found
    """
    if pd.isna(text):
        return False
    
    text_lower = str(text).lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def select_datasets(df):
    """
    Apply selection criteria to filter datasets.
    
    Criteria:
    1. Must contain KEEP_KEYWORDS in column_names
    2. Must NOT contain REMOVE_KEYWORDS in column_names
    3. Must have multi-year coverage (min_year != max_year)
    4. Must have missing_percent < 40%
    5. Remove duplicates based on column_names
    
    Args:
        df (pd.DataFrame): Summary DataFrame
        
    Returns:
        pd.DataFrame: Filtered DataFrame with selected datasets
    """
    
    print("\n" + "="*70)
    print("🔍 APPLYING SELECTION CRITERIA")
    print("="*70)
    
    initial_count = len(df)
    print(f"\n   Starting with: {initial_count} datasets")
    
    # ----------------------------------------------------------
    # Criterion 1: Keep rows with KEEP_KEYWORDS
    # ----------------------------------------------------------
    KEEP_KEYWORDS = [
        "electricity",
        "water",
        "sanitation",
        "latrine",
        "urban population",
        "growth",
        "house",
        "household",
        "room",
        "revenue",
        "expenditure",
        "capital"
    ]
    
    print(f"\n   📋 Step 1: Filtering by KEEP_KEYWORDS...")
    print(f"      Keywords: {', '.join(KEEP_KEYWORDS)}")
    
    mask_keep = df['column_names'].apply(lambda x: contains_any_keyword(x, KEEP_KEYWORDS))
    df_filtered = df[mask_keep].copy()
    
    removed_step1 = initial_count - len(df_filtered)
    print(f"      ✅ Kept: {len(df_filtered)} datasets")
    print(f"      ❌ Removed: {removed_step1} datasets (no relevant keywords)")
    
    # ----------------------------------------------------------
    # Criterion 2: Remove rows with REMOVE_KEYWORDS
    # ----------------------------------------------------------
    REMOVE_KEYWORDS = [
        "slum",
        "duplicate",
        "same"
    ]
    
    print(f"\n   📋 Step 2: Filtering by REMOVE_KEYWORDS...")
    print(f"      Keywords: {', '.join(REMOVE_KEYWORDS)}")
    
    mask_remove = df_filtered['column_names'].apply(lambda x: contains_any_keyword(x, REMOVE_KEYWORDS))
    df_filtered = df_filtered[~mask_remove].copy()
    
    removed_step2 = mask_remove.sum()
    print(f"      ✅ Kept: {len(df_filtered)} datasets")
    print(f"      ❌ Removed: {removed_step2} datasets (contains remove keywords)")
    
    # ----------------------------------------------------------
    # Criterion 3: Remove single-year datasets
    # ----------------------------------------------------------
    print(f"\n   📋 Step 3: Removing single-year datasets...")
    
    before_count = len(df_filtered)
    df_filtered = df_filtered[df_filtered["min_year"] != df_filtered["max_year"]].copy()
    
    removed_step3 = before_count - len(df_filtered)
    print(f"      ✅ Kept: {len(df_filtered)} datasets (multi-year)")
    print(f"      ❌ Removed: {removed_step3} datasets (single year only)")
    
    # ----------------------------------------------------------
    # Criterion 4: Remove duplicates based on column_names
    # ----------------------------------------------------------
    print(f"\n   📋 Step 4: Removing duplicate column structures...")
    
    before_count = len(df_filtered)
    df_filtered = df_filtered.drop_duplicates(subset=["column_names"]).copy()
    
    removed_step4 = before_count - len(df_filtered)
    print(f"      ✅ Kept: {len(df_filtered)} unique datasets")
    print(f"      ❌ Removed: {removed_step4} duplicate datasets")
    
    # ----------------------------------------------------------
    # Criterion 5: Quality filter - missing_percent < 40%
    # ----------------------------------------------------------
    print(f"\n   📋 Step 5: Filtering by data quality (missing < 40%)...")
    
    before_count = len(df_filtered)
    df_filtered = df_filtered[df_filtered["missing_percent"] < 40].copy()
    
    removed_step5 = before_count - len(df_filtered)
    print(f"      ✅ Kept: {len(df_filtered)} datasets (good quality)")
    print(f"      ❌ Removed: {removed_step5} datasets (too many missing values)")
    
    # ----------------------------------------------------------
    # Final Summary
    # ----------------------------------------------------------
    total_removed = initial_count - len(df_filtered)
    
    print(f"\n" + "="*70)
    print(f"📊 SELECTION SUMMARY")
    print("="*70)
    print(f"   Total original files: {initial_count}")
    print(f"   Total selected files: {len(df_filtered)}")
    print(f"   Total removed files: {total_removed}")
    print(f"   Selection rate: {len(df_filtered)/initial_count*100:.1f}%")
    
    print(f"\n   Removal breakdown:")
    print(f"      - No relevant keywords: {removed_step1}")
    print(f"      - Contains remove keywords: {removed_step2}")
    print(f"      - Single-year datasets: {removed_step3}")
    print(f"      - Duplicate structures: {removed_step4}")
    print(f"      - Poor data quality: {removed_step5}")
    
    return df_filtered


def copy_selected_files(selected_df):
    """
    Copy selected raw files to processed/selected_raw folder.
    
    Args:
        selected_df (pd.DataFrame): DataFrame with selected file names
    """
    print(f"\n" + "="*70)
    print(f"📦 COPYING SELECTED FILES")
    print("="*70)
    
    copied_count = 0
    errors = []
    
    for idx, row in selected_df.iterrows():
        filename = row['file_name']
        src_path = os.path.join(RAW_FOLDER, filename)
        dst_path = os.path.join(SELECTED_RAW_FOLDER, filename)
        
        try:
            if os.path.exists(src_path):
                shutil.copy2(src_path, dst_path)
                copied_count += 1
                print(f"   ✅ Copied: {filename}")
            else:
                print(f"   ⚠️  File not found: {filename}")
                errors.append(filename)
        except Exception as e:
            print(f"   ❌ Error copying {filename}: {e}")
            errors.append(filename)
    
    print(f"\n   Successfully copied: {copied_count}/{len(selected_df)} files")
    
    if errors:
        print(f"   ⚠️  Errors: {len(errors)} files")
        for err in errors[:5]:  # Show first 5 errors
            print(f"      - {err}")


def save_selection(selected_df):
    """
    Save selected files list to CSV.
    
    Args:
        selected_df (pd.DataFrame): Selected datasets
    """
    print(f"\n" + "="*70)
    print(f"💾 SAVING SELECTION RESULTS")
    print("="*70)
    
    # Save to CSV
    selected_df.to_csv(SELECTED_FILES_CSV, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(SELECTED_FILES_CSV) / 1024
    print(f"   ✅ Saved to: {SELECTED_FILES_CSV}")
    print(f"   📦 File size: {file_size:.2f} KB")
    print(f"   📊 Selected datasets: {len(selected_df)}")
    
    # Print selected files summary
    print(f"\n   📋 SELECTED FILES OVERVIEW:")
    print(f"   {'-'*100}")
    print(f"   {'File Name':<50} {'Years':<15} {'Missing %':<10}")
    print(f"   {'-'*100}")
    
    for idx, row in selected_df.iterrows():
        year_range = f"{int(row['min_year'])}-{int(row['max_year'])}"
        print(f"   {row['file_name']:<50} {year_range:<15} {row['missing_percent']:<10.2f}")
    
    print(f"   {'-'*100}")


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main function to select infrastructure datasets."""
    
    # Step 1: Load summary
    print("\n📥 Loading infrastructure summary...")
    
    if not os.path.exists(SUMMARY_FILE):
        print(f"\n❌ ERROR: Summary file not found: {SUMMARY_FILE}")
        print(f"   Please run inspect_sector_infrastructure.py first.")
        return
    
    df = pd.read_csv(SUMMARY_FILE)
    print(f"   ✅ Loaded {len(df)} datasets")
    
    # Step 2: Apply selection criteria
    selected_df = select_datasets(df)
    
    if selected_df.empty:
        print(f"\n⚠️  WARNING: No datasets passed selection criteria!")
        return
    
    # Step 3: Copy selected files
    copy_selected_files(selected_df)
    
    # Step 4: Save results
    save_selection(selected_df)
    
    # Final message
    print(f"\n" + "="*70)
    print(f"✅ INFRASTRUCTURE DATA SELECTION COMPLETE")
    print("="*70)
    print(f"\n📁 Next steps:")
    print(f"   1. Review selected files in: {SELECTED_RAW_FOLDER}")
    print(f"   2. Proceed to Phase 3: Data Cleaning")
    print(f"   3. Create infrastructure_cleaner.py")
    print(f"\n" + "="*70)


# ================================================================
# 🚀 RUN
# ================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
