# 🌐 Live Data Integration Framework

**Status:** ✅ STRUCTURE READY (No real API calls yet)  
**Created:** 2026-04-06  
**Sectors:** 7 (Climate, Economy, Trade, Geopolitics, Migration, Social, Infrastructure)

---

## 📁 Structure

```
backend/app/live/
 ├── __init__.py                 # Package exports
 ├── climate_fetcher.py          # 🌧️ Climate data (Open-Meteo, IMD)
 ├── economy_fetcher.py          # 📈 Economy data (World Bank, yfinance)
 ├── trade_fetcher.py            # 💹 Trade data (UN Comtrade)
 ├── geopolitics_fetcher.py      # ⚔️ Conflict data (ACLED, GDELT)
 ├── migration_fetcher.py        # 🌍 Migration data (World Bank)
 ├── social_fetcher.py           # 📢 Social data (GDELT, ACLED)
 ├── infrastructure_fetcher.py   # 🏗️ Infrastructure data (World Bank)
 ├── live_fetcher.py             # 🌐 Master fetcher (orchestrates all)
 ├── feature_mapper.py           # 🔄 Maps raw data → model features
 └── live_processor.py           # ⚡ End-to-end pipeline
```

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r backend/requirements.txt
```

Or manually:

```bash
pip install yfinance requests python-dotenv
```

### 2. Configure API Keys

Copy and edit `.env` file:

```bash
backend/.env
```

Add your API keys:

```env
NEWS_API_KEY=your_key_here
ACLED_API_KEY=your_key_here
```

**Note:** .env is gitignored - never commit real keys!

### 3. Test Setup

```bash
python backend/test_live_setup.py
```

Expected output:

```
✅ ALL TESTS PASSED!
   Framework structure is ready for API integration
```

---

## 📊 Usage

### Fetch All Live Data

```python
from app.live.live_fetcher import fetch_all_live_data

data = fetch_all_live_data()

print(data["climate"])
# {'rainfall': None, 'temperature': None, ...}
```

### Fetch Single Sector

```python
from app.live.economy_fetcher import fetch_economy

economy_data = fetch_economy()
print(economy_data)
# {'nifty': None, 'vix': None, 'inflation': None, ...}
```

### Process Live Data (Future)

```python
from app.live.live_processor import process_live_data

risk_scores = process_live_data()
# Returns: {'climate': 0.0, 'economy': 0.0, ...}
```

---

## 🔧 Current Status

### ✅ Completed
- [x] Folder structure created
- [x] All 7 sector fetchers (placeholder functions)
- [x] Master fetcher orchestration
- [x] Feature mapper (empty logic)
- [x] Live processor (placeholder)
- [x] Environment configuration (.env)
- [x] API key management (config.py)
- [x] Dependencies installed
- [x] Test suite passing

### ⏳ TODO: Implementation
- [ ] Implement real API calls in each fetcher
- [ ] Feature engineering in feature_mapper.py
- [ ] Model inference in live_processor.py
- [ ] Error handling & retries
- [ ] Caching layer
- [ ] Scheduled data updates
- [ ] API rate limiting
- [ ] Data validation

---

## 🎯 API Recommendations

| Sector | Recommended API | Status |
|--------|----------------|--------|
| Climate | Open-Meteo (FREE) | Ready to implement |
| Economy | World Bank + yfinance (FREE) | Ready to implement |
| Trade | UN Comtrade (FREE) | Needs registration |
| Geopolitics | ACLED (FREE for research) | Needs API key |
| Migration | World Bank (FREE) | Ready to implement |
| Social | GDELT + ACLED (FREE) | Needs setup |
| Infrastructure | World Bank (FREE) | Ready to implement |

---

## 📝 Next Steps

### Phase 1: Basic Integration (Week 1)
1. Implement Open-Meteo in `climate_fetcher.py`
2. Implement yfinance in `economy_fetcher.py`
3. Implement World Bank API in `migration_fetcher.py`
4. Test with real data

### Phase 2: Advanced APIs (Week 2)
1. Register for ACLED API key
2. Implement ACLED in `geopolitics_fetcher.py`
3. Implement GDELT in `social_fetcher.py`
4. Implement UN Comtrade in `trade_fetcher.py`

### Phase 3: Feature Engineering (Week 3)
1. Implement lag features in `feature_mapper.py`
2. Implement rolling statistics
3. Implement derived features
4. Test feature pipeline

### Phase 4: Model Integration (Week 4)
1. Load trained models
2. Implement prediction pipeline
3. Integrate with FastAPI endpoints
4. End-to-end testing

---

## 🧪 Testing

Run tests:

```bash
python backend/test_live_setup.py
```

Test individual fetchers:

```python
from app.live.climate_fetcher import fetch_climate

data = fetch_climate()
assert isinstance(data, dict)
assert "rainfall" in data
print("✅ Climate fetcher structure OK")
```

---

## 🔒 Security

- ✅ API keys stored in `.env` (gitignored)
- ✅ Keys loaded via `python-dotenv`
- ✅ No hardcoded credentials
- ✅ Safe for production deployment

---

## 📚 Documentation

For detailed API requirements and recommendations, see project documentation.

---

**Last Updated:** 2026-04-06  
**Maintainer:** Global Risk Platform Team  
**Status:** Ready for API Integration
