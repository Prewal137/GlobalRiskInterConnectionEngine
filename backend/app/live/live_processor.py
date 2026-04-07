"""
⚡ Live Data Processor

End-to-end pipeline:
1. Fetch live data from all sectors
2. Map to model features
3. Run predictions with trained models
4. Return risk scores (0-1 normalized)

Status: IMPLEMENTED ✅
"""

import joblib
import os
import sys
import numpy as np
from typing import Dict, Optional

# Add project root to path for model loading
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.app.live.live_fetcher import fetch_all_live_data
from backend.app.live.feature_mapper import map_to_model_features


# ================================================================
# 🤖 LOAD TRAINED MODELS
# ================================================================

MODELS_PATH = os.path.join(PROJECT_ROOT, "models", "trained")

print("\n📦 Loading trained models...")

try:
    climate_model = joblib.load(os.path.join(MODELS_PATH, "climate_model.pkl"))
    print("   ✅ Climate model loaded")
except Exception as e:
    print(f"   ⚠️  Climate model failed: {e}")
    climate_model = None

try:
    economy_model = joblib.load(os.path.join(MODELS_PATH, "economy_model.pkl"))
    print("   ✅ Economy model loaded")
except Exception as e:
    print(f"   ⚠️  Economy model failed: {e}")
    economy_model = None

try:
    trade_model = joblib.load(os.path.join(MODELS_PATH, "trade_model.pkl"))
    print("   ✅ Trade model loaded")
except Exception as e:
    print(f"   ⚠️  Trade model failed: {e}")
    trade_model = None

try:
    geopolitics_model = joblib.load(os.path.join(MODELS_PATH, "geopolitics_model.pkl"))
    print("   ✅ Geopolitics model loaded")
except Exception as e:
    print(f"   ⚠️  Geopolitics model failed: {e}")
    geopolitics_model = None

try:
    # Migration model may not exist
    migration_model_path = os.path.join(MODELS_PATH, "migration_model.pkl")
    if os.path.exists(migration_model_path):
        migration_model = joblib.load(migration_model_path)
        print("   ✅ Migration model loaded")
    else:
        print("   ⚠️  Migration model not found (will use fallback)")
        migration_model = None
except Exception as e:
    print(f"   ⚠️  Migration model failed: {e}")
    migration_model = None

try:
    social_model = joblib.load(os.path.join(MODELS_PATH, "social_model.pkl"))
    print("   ✅ Social model loaded")
except Exception as e:
    print(f"   ⚠️  Social model failed: {e}")
    social_model = None

try:
    infrastructure_model = joblib.load(os.path.join(MODELS_PATH, "infrastructure_model.pkl"))
    print("   ✅ Infrastructure model loaded")
except Exception as e:
    print(f"   ⚠️  Infrastructure model failed: {e}")
    infrastructure_model = None


# ================================================================
# 🎯 MAIN PROCESSING FUNCTION
# ================================================================

def process_live_data() -> Dict[str, float]:
    """
    Process live data through complete ML pipeline.
    
    Pipeline:
    1. Fetch real-time data from all sectors
    2. Map raw data to model-ready features
    3. Run predictions using trained models
    4. Normalize risk scores to 0-1 range
    
    Returns:
        Dictionary with risk scores per sector (0-1 normalized)
    """
    print("\n" + "="*70)
    print("⚡ PROCESSING LIVE DATA THROUGH ML PIPELINE")
    print("="*70)
    
    # Step 1: Fetch live data from all sectors
    print("\n📡 Step 1: Fetching live data...")
    raw_data = fetch_all_live_data()
    
    # Step 2: Map to model features
    print("\n🔄 Step 2: Mapping to model features...")
    features = map_to_model_features(raw_data)
    
    # Step 3: Run model predictions
    print("\n🤖 Step 3: Running model predictions...")
    risk_output = {}
    
    # Climate
    try:
        if climate_model and features.get("climate"):
            feature_values = list(features["climate"].values())
            prediction = climate_model.predict([feature_values])[0]
            risk_output["climate"] = float(prediction)
            print(f"   ✅ Climate risk: {risk_output['climate']:.4f}")
        else:
            risk_output["climate"] = 0.5
            print(f"   ⚠️  Climate: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Climate prediction error: {e}")
        risk_output["climate"] = 0.5
    
    # Economy
    try:
        if economy_model and features.get("economy"):
            feature_values = list(features["economy"].values())
            prediction = economy_model.predict([feature_values])[0]
            risk_output["economy"] = float(prediction)
            print(f"   ✅ Economy risk: {risk_output['economy']:.4f}")
        else:
            risk_output["economy"] = 0.5
            print(f"   ⚠️  Economy: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Economy prediction error: {e}")
        risk_output["economy"] = 0.5
    
    # Trade
    try:
        if trade_model and features.get("trade"):
            feature_values = list(features["trade"].values())
            prediction = trade_model.predict([feature_values])[0]
            risk_output["trade"] = float(prediction)
            print(f"   ✅ Trade risk: {risk_output['trade']:.4f}")
        else:
            risk_output["trade"] = 0.5
            print(f"   ⚠️  Trade: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Trade prediction error: {e}")
        risk_output["trade"] = 0.5
    
    # Geopolitics
    try:
        if geopolitics_model and features.get("geopolitics"):
            feature_values = list(features["geopolitics"].values())
            prediction = geopolitics_model.predict([feature_values])[0]
            risk_output["geopolitics"] = float(prediction)
            print(f"   ✅ Geopolitics risk: {risk_output['geopolitics']:.4f}")
        else:
            risk_output["geopolitics"] = 0.5
            print(f"   ⚠️  Geopolitics: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Geopolitics prediction error: {e}")
        risk_output["geopolitics"] = 0.5
    
    # Migration
    try:
        if migration_model and features.get("migration"):
            feature_values = list(features["migration"].values())
            prediction = migration_model.predict([feature_values])[0]
            risk_output["migration"] = float(prediction)
            print(f"   ✅ Migration risk: {risk_output['migration']:.4f}")
        else:
            risk_output["migration"] = 0.5
            print(f"   ⚠️  Migration: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Migration prediction error: {e}")
        risk_output["migration"] = 0.5
    
    # Social
    try:
        if social_model and features.get("social"):
            feature_values = list(features["social"].values())
            prediction = social_model.predict([feature_values])[0]
            risk_output["social"] = float(prediction)
            print(f"   ✅ Social risk: {risk_output['social']:.4f}")
        else:
            risk_output["social"] = 0.5
            print(f"   ⚠️  Social: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Social prediction error: {e}")
        risk_output["social"] = 0.5
    
    # Infrastructure
    try:
        if infrastructure_model and features.get("infrastructure"):
            feature_values = list(features["infrastructure"].values())
            prediction = infrastructure_model.predict([feature_values])[0]
            risk_output["infrastructure"] = float(prediction)
            print(f"   ✅ Infrastructure risk: {risk_output['infrastructure']:.4f}")
        else:
            risk_output["infrastructure"] = 0.5
            print(f"   ⚠️  Infrastructure: Using fallback (0.5)")
    except Exception as e:
        print(f"   ❌ Infrastructure prediction error: {e}")
        risk_output["infrastructure"] = 0.5
    
    # Step 4: Normalize to 0-1 range
    print("\n📊 Step 4: Normalizing risk scores...")
    for key in risk_output:
        risk_output[key] = max(0.0, min(1.0, risk_output[key]))
    
    print("\n✅ Live data processing complete!")
    print(f"   Risk scores generated: {len(risk_output)}")
    
    return risk_output
