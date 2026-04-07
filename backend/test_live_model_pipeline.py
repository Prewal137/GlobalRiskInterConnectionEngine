"""
🧪 Test Live Model Pipeline

Tests the complete live data pipeline:
1. Fetch live data from APIs
2. Map to model features
3. Run predictions with trained models
4. Return risk scores
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))


def test_live_pipeline():
    """Test complete live data pipeline with ML models"""
    print("\n" + "="*70)
    print("🚀 TESTING LIVE MODEL PIPELINE")
    print("="*70)
    
    try:
        from app.live.live_processor import process_live_data
        
        print("\n📡 Running live data pipeline...")
        print("   This will:")
        print("   1. Fetch live data from APIs")
        print("   2. Map to model features")
        print("   3. Predict risk using trained models")
        
        # Run pipeline
        risk = process_live_data()
        
        # Display results
        print("\n" + "="*70)
        print("📊 PREDICTED RISK SCORES")
        print("="*70)
        print("\nSector                 Risk Score    Status")
        print("-" * 70)
        
        for sector, score in sorted(risk.items()):
            # Determine risk level
            if score < 0.3:
                level = "🟢 LOW"
            elif score < 0.6:
                level = "🟡 MEDIUM"
            elif score < 0.8:
                level = "🟠 HIGH"
            else:
                level = "🔴 CRITICAL"
            
            print(f"{sector:22s} {score:12.4f}    {level}")
        
        print("\n" + "="*70)
        print("✅ PIPELINE TEST SUCCESSFUL!")
        print("="*70)
        
        # Verify all sectors present
        expected_sectors = [
            "climate", "economy", "trade", "geopolitics",
            "migration", "social", "infrastructure"
        ]
        
        missing = [s for s in expected_sectors if s not in risk]
        if missing:
            print(f"\n⚠️  Missing sectors: {missing}")
            return False
        
        # Verify all scores are 0-1
        invalid = [s for s, v in risk.items() if not (0 <= v <= 1)]
        if invalid:
            print(f"\n❌ Invalid risk scores (not 0-1): {invalid}")
            return False
        
        print(f"\n✅ All {len(risk)} sectors present with valid risk scores")
        return True
        
    except Exception as e:
        print(f"\n❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_feature_mapping():
    """Test feature mapping from live data"""
    print("\n" + "="*70)
    print("🔄 TESTING FEATURE MAPPING")
    print("="*70)
    
    try:
        from app.live.live_fetcher import fetch_all_live_data
        from app.live.feature_mapper import map_to_model_features
        
        # Fetch live data
        print("\n📡 Fetching live data...")
        raw_data = fetch_all_live_data()
        
        # Map to features
        print("\n🔄 Mapping to features...")
        features = map_to_model_features(raw_data)
        
        # Display results
        print("\n📊 Feature Mapping Results:")
        for sector, feat in features.items():
            if feat:
                print(f"\n   ✅ {sector}: {len(feat)} features")
                for key, val in list(feat.items())[:3]:  # Show first 3
                    print(f"      {key}: {val}")
            else:
                print(f"\n   ⚠️  {sector}: No features (placeholder data)")
        
        print("\n✅ Feature mapping test successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Feature mapping test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "🚀"*35)
    print("  LIVE MODEL PIPELINE TEST SUITE")
    print("🚀"*35)
    
    # Test 1: Feature mapping
    test1_passed = test_feature_mapping()
    
    # Test 2: Full pipeline
    test2_passed = test_live_pipeline()
    
    # Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    print(f"{'✅ PASS' if test1_passed else '❌ FAIL'} - Feature Mapping")
    print(f"{'✅ PASS' if test2_passed else '❌ FAIL'} - Live Model Pipeline")
    
    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("   Live AI pipeline is fully operational")
        return True
    else:
        print(f"\n⚠️  Some tests failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
