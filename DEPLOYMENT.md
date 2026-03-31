# 🚀 Deployment Guide - Climate Risk Prediction System

## 📦 Model Management for Production

### Current Setup

Your trained model is at: `models/trained/climate_model.pkl` (~5-10 MB)

This file **SHOULD be committed to Git** for deployment purposes.

---

## ✅ Option 1: Simple Deployment (Recommended for MVP)

### Commit Model to Repository

Since your climate model is small (< 50MB), you can commit it directly:

```bash
# Add the model to Git
git add models/trained/climate_model.pkl
git add models/trained/climate_scaler.pkl
git commit -m "Add trained climate risk model for production"
git push
```

**Why this works:**
- XGBoost models are relatively small
- Single file, easy to manage
- No external dependencies
- Perfect for startups/MVPs

**Deployment platforms that support this:**
- ✅ Heroku
- ✅ Railway
- ✅ Render
- ✅ Google Cloud Run
- ✅ AWS App Runner
- ✅ Vercel (with serverless functions)

---

## 🐳 Option 2: Docker Deployment (Production Ready)

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Download/generate data if needed
RUN python pipeline/processing/climate_features.py

# Expose port
EXPOSE 8000

# Run FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Then deploy with:
```bash
docker build -t climate-risk-api .
docker run -p 8000:8000 climate-risk-api
```

---

## ☁️ Option 3: Cloud Storage + Download on Deploy

Store models in cloud storage, download during deployment:

### AWS S3 Example:

```python
# backend/app/services/model_loader.py
import boto3
import os

def load_model_from_s3():
    s3 = boto3.client('s3')
    bucket = 'your-model-bucket'
    
    # Download model
    s3.download_file(bucket, 'climate_model.pkl', 'models/trained/climate_model.pkl')
    s3.download_file(bucket, 'climate_scaler.pkl', 'models/trained/climate_scaler.pkl')
```

**Deployment flow:**
1. Train model locally
2. Upload to S3: `aws s3 cp models/trained/ s3://bucket/models/`
3. Deploy app (downloads model on startup)

---

## 🔄 Handling New Data in Production

### Scenario 1: User Passes New District Data

**API Endpoint Flow:**

```python
@router.post("/climate-risk/predict")
async def predict_new_district(data: DistrictData):
    """
    Accept new data and return prediction
    
    Request:
    {
        "state": "Karnataka",
        "district": "Udupi",
        "rainfall": 1200,
        "groundwater": 45.2,
        ...
    }
    """
    # 1. Load model (cached in memory)
    model = get_cached_model()
    
    # 2. Preprocess input
    features = preprocess(data)
    
    # 3. Make prediction
    risk_score = model.predict(features)
    
    # 4. Normalize and return
    return {
        "district": data.district,
        "predicted_risk": float(risk_score),
        "risk_level": get_risk_level(risk_score)
    }
```

**No need to retrain!** The model generalizes to new inputs.

---

### Scenario 2: Retrain with Fresh Data

When you have NEW training data (e.g., 2024 climate data):

```bash
# 1. Add new raw data
cp new_climate_2024.csv data/raw/climate/

# 2. Regenerate features
python pipeline/processing/climate_features.py

# 3. Retrain model
python pipeline/processing/climate_model.py

# 4. Test new model
# (validation shows improved performance)

# 5. Deploy updated model
git add models/trained/climate_model.pkl
git commit -m "Update model with 2024 data"
git push
```

**Retraining triggers:**
- New year of data available
- Model performance degrading
- New districts added
- Feature engineering improvements

---

## 📊 Production Architecture

```
┌─────────────┐
│   User/UI   │
└──────┬──────┘
       │ HTTP Request
       ▼
┌─────────────────────────┐
│   FastAPI Server        │
│   (backend/app/main.py) │
├─────────────────────────┤
│  Cached Model in RAM    │ ← Load once, reuse
│  - climate_model.pkl    │
│  - climate_scaler.pkl   │
└────────┬────────────────┘
         │ Predict
         ▼
┌─────────────────────┐
│  Interconnection    │
│  Engine             │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Response JSON      │
│  - risk scores      │
│  - risk levels      │
└─────────────────────┘
```

---

## 🔧 Implementation Steps

### Step 1: Add Model to Git (Now)

```bash
# Your climate model is ~5-10MB, safe to commit
git add models/trained/climate_model.pkl
git add models/trained/climate_scaler.pkl
git commit -m "Add production climate risk model"
git push origin main
```

### Step 2: Choose Deployment Platform

**Easy options:**

**Railway.app** (Recommended for beginners):
```bash
# Install Railway CLI
npm i -g @railway/cli

# Deploy
railway login
railway init
railway up
```

**Google Cloud Run**:
```bash
# Build container
docker build -t gcr.io/YOUR_PROJECT/climate-risk .

# Deploy
gcloud run deploy climate-risk \
  --image gcr.io/YOUR_PROJECT/climate-risk \
  --platform managed \
  --region us-central1
```

**Heroku**:
```bash
# Install Heroku CLI
heroku login
heroku create climate-risk-api
git push heroku main
```

### Step 3: Environment Configuration

Create `.env.example` (commit this):
```env
# Environment variables template
MODEL_PATH=models/trained/climate_model.pkl
DATA_PATH=data/processed/climate
LOG_LEVEL=INFO
PORT=8000
```

Create `.env` (DO NOT commit):
```env
# Your actual values
MODEL_PATH=models/trained/climate_model.pkl
DATA_PATH=data/processed/climate
LOG_LEVEL=INFO
PORT=8000
```

### Step 4: Model Caching in Production

Optimize by loading model once:

```python
# backend/app/services/model_cache.py
from functools import lru_cache
import joblib

@lru_cache()
def get_model():
    """Load model once, cache forever"""
    return joblib.load('models/trained/climate_model.pkl')

@lru_cache()
def get_scaler():
    """Load scaler once, cache forever"""
    return joblib.load('models/trained/climate_scaler.pkl')
```

Use in endpoints:
```python
@router.get("/climate-risk/state/{state}")
def get_state_risk(state: str):
    model = get_model()  # Cached!
    scaler = get_scaler()  # Cached!
    # ... use for prediction
```

---

## 📈 Monitoring & Updates

### Track Model Performance

```python
# Log predictions for monitoring
import logging

logger = logging.getLogger(__name__)

@router.post("/predict")
def predict(data: InputData):
    prediction = model.predict(data)
    
    logger.info(f"Prediction: {prediction} for {data.district}")
    
    return {"risk": prediction}
```

### When to Update Model

Monitor these metrics:
- Prediction distribution shifts
- User feedback on accuracy
- New data availability
- Performance degradation

**Update frequency:**
- Quarterly (every 3 months)
- When major climate events occur
- Annually (when govt releases new data)

---

## 🎯 Summary: What to Do NOW

### Immediate Actions:

1. **Commit the model** (it's small enough):
   ```bash
   git add models/trained/*.pkl
   git commit -m "Add production model"
   git push
   ```

2. **Create deployment config** (choose platform):
   - Railway: Create `railway.json`
   - Heroku: Create `Procfile`
   - Docker: Create `Dockerfile`

3. **Test deployment locally**:
   ```bash
   # Simulate production
   docker build -t test-climate .
   docker run -p 8000:8000 test-climate
   
   # Test API
   curl http://localhost:8000/climate-risk/state/Odisha
   ```

### For New Data:

- **Individual predictions**: Model handles automatically ✅
- **Bulk updates**: Retrain quarterly or when new data arrives ✅

---

## ❓ FAQ

**Q: Won't the repo become large?**  
A: XGBoost models are ~5-10MB. Even with 10 versions, that's only 100MB - acceptable for most projects.

**Q: What if the model is 500MB+?**  
A: Use cloud storage (S3, GCS) and download on deploy.

**Q: How do I handle 1000s of requests?**  
A: Model is cached in RAM after first load. FastAPI can handle 1000s req/sec easily.

**Q: Can I update without downtime?**  
A: Yes! Use blue-green deployment or rolling updates (supported by all major platforms).

---

## 🔗 Resources

- [FastAPI Deployment Guide](https://fastapi.tiangolo.com/deployment/)
- [Docker Best Practices](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)
- [ML Model Deployment Patterns](https://www.mlopsguide.com/)

---

**Ready to deploy? Let me know which platform you prefer and I'll help set it up!** 🚀
