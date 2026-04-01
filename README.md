# 🌍 Global Risk Interconnection Platform

A comprehensive multi-sector risk prediction and analysis system that combines climate, trade, and economic data to provide unified risk assessments.

![Feature Correlations](docs/images/feature_correlations_enhanced.png)

## 📊 Project Overview

This platform integrates multiple risk factors across different sectors to create interconnected risk scores. It uses machine learning models (XGBoost) to predict trade shocks and climate risks, then combines them into unified country-level risk assessments.

### Key Features

- ✅ **Climate Risk Prediction** - District and state-level climate risk assessment
- ✅ **Trade Shock Prediction** - ML-powered trade shock forecasting (92% accuracy)
- ✅ **Interconnection Engine** - Combines climate + trade risks with cascading effects
- ✅ **REST API** - FastAPI backend with comprehensive endpoints
- ✅ **Multi-Sector Analysis** - Climate, Trade, Economy, Migration, Infrastructure

---

## 🏗️ Architecture

```
global-risk-interconnection-platform/
├── backend/app/              # FastAPI application
│   ├── routes/              # API endpoints
│   │   ├── climate.py       # Climate risk endpoints
│   │   ├── trade.py         # Trade risk endpoints
│   │   └── interconnection.py # Global risk endpoints
│   ├── services/            # Business logic
│   │   └── interconnection_engine.py
│   ├── core/                # Configuration
│   │   └── config.py
│   └── main.py              # Application entry point
│
├── pipeline/processing/      # Data processing scripts
│   ├── trade_model.py       # Trade model training
│   ├── trade_output.py      # Trade risk generation
│   └── [climate processing scripts]
│
├── data/
│   ├── raw/                 # Raw data (not tracked in Git)
│   ├── processed/           # Cleaned features & predictions ✅
│   └── output/              # Generated outputs
│
├── models/trained/          # Trained ML models ✅
│   ├── climate_model.pkl
│   └── trade_model.pkl
│
└── docs/images/             # Documentation assets
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip package manager

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd global-risk-interconnection-platform
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Verify setup**
```bash
python backend/app/core/config.py
```

### Running the API

```bash
cd backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Access the interactive API documentation at: `http://localhost:8000/docs`

---

## 📡 API Endpoints

### Climate Risk Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /climate-risk/state/{state}` | Get climate risk for a specific state |
| `GET /climate-risk/district/{district}` | Get climate risk for a specific district |
| `GET /climate-risk/top-states` | Get top high-risk states |
| `GET /climate-risk/states` | Get all states data |

### Trade Risk Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /trade-risk/country/{country}` | Get trade risk for a specific country |
| `GET /trade-risk/top` | Get top 10 risky countries |
| `GET /trade-risk/all` | Get full trade risk dataset |
| `GET /trade-risk/summary` | Get trade risk summary statistics |

### Global Risk Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /global-risk/summary` | Get multi-sector risk summary |
| `GET /global-risk/top` | Get top 10 high-risk regions |
| `GET /global-risk/state/{state}` | Get detailed risk for a region |
| `GET /global-risk/all` | Get all regions data |

### Example Usage

```bash
# Get summary of global risks
curl http://localhost:8000/global-risk/summary

# Get top risky countries
curl http://localhost:8000/trade-risk/top

# Get specific country trade risk
curl http://localhost:8000/trade-risk/country/India
```

---

## 🤖 Machine Learning Models

### Trade Shock Prediction Model

**Algorithm:** XGBoost Classifier  
**Accuracy:** 92.00%  
**Features:** 10 (Export, Import, Trade_Balance, Volatility, etc.)

#### Performance Metrics
```
Accuracy:  92.00%
Precision: 91.63%
Recall:    92.00%
F1 Score:  91.58%
```

#### Top Feature Importances
1. Export (40.64%)
2. Volatility_3 (24.94%)
3. Year (17.98%)
4. Rolling_Mean_3 (16.45%)

### Climate Risk Model

Predicts climate-related risks at district and state levels across India.

### Interconnection Engine

Combines climate and trade risks using weighted averaging:

```python
weighted_risk = 0.5 * climate_risk + 0.5 * trade_risk

# Cascading effect when both risks are high
if climate_risk > 0.7 AND trade_risk > 0.6:
    cascading_risk = weighted_risk * 1.2
```

---

## 📊 Data Pipeline

### Step 1: Data Collection
- Raw data from multiple sources (climate, trade, economy, geopolitics)
- Stored in `data/raw/` (not tracked in Git)

### Step 2: Data Processing
```bash
python pipeline/processing/trade_model.py  # Train trade model
python pipeline/processing/trade_output.py  # Generate predictions
```

### Step 3: Risk Integration
```bash
python backend/app/services/interconnection_engine.py
```

### Output Files (Tracked in Git)
- ✅ `data/processed/climate/climate_risk_district.csv`
- ✅ `data/processed/trade/trade_features_clean.csv`
- ✅ `data/processed/trade/trade_risk_*.csv`
- ✅ `data/processed/interconnection/global_risk.csv`

---

## 🔧 Configuration

All file paths and constants are centralized in `backend/app/core/config.py`:

```python
from app.core.config import (
    CLIMATE_RISK_DISTRICT,
    TRADE_FEATURES_CLEAN,
    GLOBAL_RISK,
    CLIMATE_MODEL_PATH,
    TRADE_MODEL_PATH
)
```

---

## 📈 Risk Classification

| Level | Score Range | Color |
|-------|-------------|-------|
| VERY LOW | < 0.05 | 🟢 |
| LOW | 0.05 - 0.10 | 🟡 |
| MEDIUM | 0.10 - 0.20 | 🟠 |
| HIGH | 0.20 - 0.40 | 🔶 |
| VERY HIGH | ≥ 0.40 | 🔴 |

---

## 🎯 Current Results

### Top 5 High-Risk Regions (Global Risk Score)

1. **Chhattisgarh** - 1.0000 (VERY HIGH) ⚠️
2. **Arunachal Pradesh** - 0.5081 (VERY HIGH)
3. **Kerala** - 0.4972 (VERY HIGH)
4. **Gujarat** - 0.4717 (VERY HIGH)
5. **Andhra Pradesh** - 0.4394 (VERY HIGH)

### Summary Statistics
- **Total Regions Analyzed:** 30 Indian states
- **Mean Climate Risk:** 6.90%
- **Mean Trade Risk:** 4.36%
- **Mean Final Risk:** 29.51%

---

## 🧪 Testing

### Run the Full Pipeline

```bash
# 1. Train trade model
python pipeline/processing/trade_model.py

# 2. Generate trade risk outputs
python pipeline/processing/trade_output.py

# 3. Run interconnection engine
python backend/app/services/interconnection_engine.py

# 4. Start API server
cd backend && uvicorn app.main:app --reload
```

### Test API Endpoints

```bash
# Health check
curl http://localhost:8000/health

# Get API info
curl http://localhost:8000/
```

---

## 📦 Dependencies

Key packages:
- `fastapi>=0.100.0` - Web framework
- `uvicorn>=0.23.0` - ASGI server
- `pandas>=2.0.0` - Data manipulation
- `xgboost>=1.7.0` - ML model
- `scikit-learn>=1.3.0` - ML utilities
- `joblib>=1.3.0` - Model serialization

See `requirements.txt` for complete list.

---

## 🔒 Data Privacy & Git Tracking

### What's Tracked in Git ✅
- All code files (.py, .md, .json)
- Processed feature datasets (< 1MB)
- Model prediction outputs
- Trained models (< 50MB)
- Documentation and images

### What's NOT Tracked ❌
- Raw data files (`data/raw/`)
- Large intermediate CSVs (> 1MB)
- Python cache files (`__pycache__/`)
- Environment files (`.env`)
- Log files

---

## 🤝 Team Collaboration

### For New Team Members

1. **Clone and setup**
   ```bash
   git clone <repo-url>
   pip install -r requirements.txt
   ```

2. **Access data files**
   - Essential CSVs are tracked in Git
   - Raw data should be added separately if needed

3. **Run tests**
   ```bash
   python backend/app/core/config.py  # Validate paths
   ```

### Adding New Data

1. Place raw data in `data/raw/` (will be ignored by Git)
2. Process and save cleaned version to `data/processed/`
3. Update `backend/app/core/config.py` with new file paths

---

## 📝 License

[Add your license here]

---

## 👥 Contributors

[Add contributor information]

---

## 📞 Support

For issues or questions:
- Open an issue on GitHub
- Check API docs: `http://localhost:8000/docs`
- Review configuration in `backend/app/core/config.py`

---

## 🎯 Roadmap

- [ ] Add real-time data fetching
- [ ] Implement additional risk sectors (migration, infrastructure)
- [ ] Create dashboard frontend
- [ ] Add automated retraining pipeline
- [ ] Deploy to cloud (AWS/Azure/GCP)

---

**Last Updated:** April 2026  
**Version:** 1.0.0
