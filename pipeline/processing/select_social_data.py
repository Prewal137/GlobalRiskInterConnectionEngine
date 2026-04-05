"""
📋 Social Sector Data Selection Script

Selects and copies specific CSV files from raw to processed folder.

Selected Files:
- sathwiksalian1515_17708937592866747.csv
- sathwiksalian1515_17708937834479353.csv
- sathwiksalian1515_17708937963474379.csv
- sathwiksalian1515_1770894338777049.csv

Output: data/processed/social/selected/
"""

import os
import shutil

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

# Get project root (go up 2 levels from pipeline/processing/)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

RAW_FOLDER = os.path.join(BASE_PATH, "data", "raw", "social")
OUTPUT_FOLDER = os.path.join(BASE_PATH, "data", "processed", "social", "selected")

# List of files to select (based on inspection results)
SELECTED_FILES = [
    "sathwiksalian1515_17708937592866747.csv",
    "sathwiksalian1515_17708937834479353.csv",
    "sathwiksalian1515_17708937963474379.csv",
    "sathwiksalian1515_1770894338777049.csv"
]

print("="*70)
print("📋 SOCIAL SECTOR DATA SELECTION")
print("="*70)
print(f"\n📂 Source: {RAW_FOLDER}")
print(f"📁 Destination: {OUTPUT_FOLDER}")
print("-"*70)

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

def ensure_directory_exists(folder_path):
    """
    Create directory if it doesn't exist.
    
    Args:
        folder_path (str): Path to directory
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)
        print(f"   ✅ Created directory: {folder_path}")
    else:
        print(f"   ✓ Directory exists: {folder_path}")


def copy_selected_files():
    """
    Copy selected files from raw to processed folder.
    
    Returns:
        tuple: (success_count, skipped_count, error_count)
    """
    # Check if raw folder exists
    if not os.path.exists(RAW_FOLDER):
        print(f"\n❌ ERROR: Raw folder not found: {RAW_FOLDER}")
        return 0, 0, 0
    
    # Ensure output directory exists
    ensure_directory_exists(OUTPUT_FOLDER)
    
    # Get all CSV files in raw folder
    all_files = [f for f in os.listdir(RAW_FOLDER) if f.endswith('.csv')]
    
    if not all_files:
        print(f"\n⚠️  WARNING: No CSV files found in {RAW_FOLDER}")
        return 0, 0, 0
    
    print(f"\n📊 Total files in raw folder: {len(all_files)}")
    print(f"🎯 Files to select: {len(SELECTED_FILES)}")
    print("="*70)
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    # Process each file in raw folder
    for filename in sorted(all_files):
        source_path = os.path.join(RAW_FOLDER, filename)
        dest_path = os.path.join(OUTPUT_FOLDER, filename)
        
        if filename in SELECTED_FILES:
            # Selected file - copy it
            try:
                shutil.copy2(source_path, dest_path)
                file_size = os.path.getsize(dest_path) / 1024
                print(f"✅ Selected: {filename} ({file_size:.1f} KB)")
                success_count += 1
            except Exception as e:
                print(f"❌ ERROR copying {filename}: {str(e)}")
                error_count += 1
        else:
            # Not selected - skip it
            print(f"⏭️  Skipped: {filename}")
            skipped_count += 1
    
    return success_count, skipped_count, error_count


def verify_selection():
    """
    Verify that all selected files were copied successfully.
    
    Returns:
        bool: True if all files present, False otherwise
    """
    print("\n" + "="*70)
    print("🔍 Verifying selection...")
    
    missing_files = []
    
    for filename in SELECTED_FILES:
        dest_path = os.path.join(OUTPUT_FOLDER, filename)
        if os.path.exists(dest_path):
            file_size = os.path.getsize(dest_path) / 1024
            print(f"   ✓ {filename} ({file_size:.1f} KB)")
        else:
            print(f"   ❌ MISSING: {filename}")
            missing_files.append(filename)
    
    if missing_files:
        print(f"\n⚠️  WARNING: {len(missing_files)} file(s) missing!")
        return False
    else:
        print(f"\n   ✅ All {len(SELECTED_FILES)} files verified successfully")
        return True


# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

def main():
    """Main execution function."""
    
    # Copy selected files
    success_count, skipped_count, error_count = copy_selected_files()
    
    # Print summary
    print("\n" + "="*70)
    print("📊 SELECTION SUMMARY")
    print("="*70)
    print(f"   ✅ Successfully copied: {success_count}")
    print(f"   ⏭️  Skipped: {skipped_count}")
    print(f"   ❌ Errors: {error_count}")
    
    # Verify selection
    verification_passed = verify_selection()
    
    # Final status
    print("\n" + "="*70)
    if verification_passed and error_count == 0:
        print("✅ Social dataset selection completed")
    elif error_count > 0:
        print(f"⚠️  Social dataset selection completed with {error_count} error(s)")
    else:
        print("⚠️  Social dataset selection completed with verification issues")
    print("="*70)


if __name__ == "__main__":
    main()
