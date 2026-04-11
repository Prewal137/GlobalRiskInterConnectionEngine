"""
🌍 Global Risk Interconnection Platform - Configuration

Centralized configuration for file paths, constants, and model hyperparameters.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ================================================================
# 📂 BASE PATHS
# ================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Data directories
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")
DATA_OUTPUT = os.path.join(BASE_DIR, "data", "output")

# Pipeline directories
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline")
PIPELINE_OUTPUT = os.path.join(PIPELINE_DIR, "output")

# Models directories
MODELS_DIR = os.path.join(BASE_DIR, "models")
MODELS_TRAINED = os.path.join(MODELS_DIR, "trained")
MODELS_CHECKPOINTS = os.path.join(MODELS_DIR, "checkpoints")

# ================================================================
# 📊 DATA FILE PATHS
# ================================================================

# Climate Data
CLIMATE_RISK_DISTRICT = os.path.join(DATA_PROCESSED, "climate", "climate_risk_district.csv")
CLIMATE_RISK_STATE = os.path.join(DATA_PROCESSED, "climate", "climate_risk_state.csv")
CLIMATE_RISK_OUTPUT = os.path.join(DATA_PROCESSED, "climate", "climate_risk_output.csv")
CLIMATE_FEATURES = os.path.join(DATA_PROCESSED, "climate", "climate_features.csv")

# Trade Data
TRADE_FEATURES_CLEAN = os.path.join(DATA_PROCESSED, "trade", "trade_features_clean.csv")
TRADE_RISK_OUTPUT = os.path.join(DATA_PROCESSED, "trade", "trade_risk_output.csv")
TRADE_RISK_COUNTRY = os.path.join(DATA_PROCESSED, "trade", "trade_risk_country.csv")
TRADE_RISK_TOP = os.path.join(DATA_PROCESSED, "trade", "trade_risk_top.csv")

# Interconnection Data
GLOBAL_RISK = os.path.join(DATA_PROCESSED, "interconnection", "global_risk.csv")
INTERCONNECTED_RISK = os.path.join(DATA_PROCESSED, "interconnection", "interconnected_risk.csv")

# ================================================================
# 🤖 MODEL PATHS
# ================================================================

# Trained Models
CLIMATE_MODEL_PATH = os.path.join(MODELS_TRAINED, "climate_model.pkl")
CLIMATE_SCALER_PATH = os.path.join(MODELS_TRAINED, "climate_scaler.pkl")
TRADE_MODEL_PATH = os.path.join(MODELS_TRAINED, "trade_model.pkl")

# ================================================================
# ⚙️ MODEL HYPERPARAMETERS
# ================================================================

# XGBoost Model (Trade)
XGB_N_ESTIMATORS = 200
XGB_MAX_DEPTH = 5
XGB_LEARNING_RATE = 0.05
XGB_RANDOM_STATE = 42

# Train-Test Split
TEST_SIZE = 0.2
RANDOM_STATE = 42

# ================================================================
# 📈 RISK CLASSIFICATION THRESHOLDS
# ================================================================

RISK_LEVELS = {
    "VERY_LOW": 0.05,
    "LOW": 0.10,
    "MEDIUM": 0.20,
    "HIGH": 0.40,
    # VERY_HIGH: >= 0.40
}

# ================================================================
# 🔗 INTERCONNECTION WEIGHTS
# ================================================================

# Weights for combining climate and trade risks
CLIMATE_WEIGHT = 0.5
TRADE_WEIGHT = 0.5

# Cascading risk multiplier (when both risks are high)
CASCADING_MULTIPLIER = 1.2

# Thresholds for cascading risk
CLIMATE_HIGH_THRESHOLD = 0.7
TRADE_HIGH_THRESHOLD = 0.6

# ================================================================
# 🌐 API CONFIGURATION
# ================================================================

API_TITLE = "Global Risk Interconnection Platform API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "REST API for accessing multi-sector risk predictions"

# Default query limits
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

# ================================================================
# 🔑 LIVE DATA API KEYS
# ================================================================

# API keys loaded from environment variables (.env file)
# Reload .env to ensure we have the latest values
load_dotenv(override=True)

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
ACLED_EMAIL = os.getenv("ACLED_EMAIL", "")
ACLED_PASSWORD = os.getenv("ACLED_PASSWORD", "")

# Future API keys (add as needed)
# OPEN_METEO_API_KEY = os.getenv("OPEN_METEO_API_KEY", "")
# WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "")
# ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

# ================================================================
# 📝 VALIDATION
# ================================================================

def validate_paths():
    """Validate that all required data files exist."""
    required_files = [
        CLIMATE_RISK_DISTRICT,
        TRADE_FEATURES_CLEAN,
        GLOBAL_RISK,
        CLIMATE_MODEL_PATH,
        TRADE_MODEL_PATH
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("⚠️  Warning: Some required files are missing:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        return False
    
    print("✅ All required files found!")
    return True
