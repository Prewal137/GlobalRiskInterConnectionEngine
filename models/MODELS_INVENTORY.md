# Trained Models Inventory

This file tracks all trained models in the `models/trained/` directory.

## ✅ Available Models

| Model | File | Status | Size | Trained On |
|-------|------|--------|------|------------|
| Climate Risk | `climate_model.pkl` | ✅ Trained | ~5 MB | 2024-03-31 |
| Climate Scaler | `climate_scaler.pkl` | ✅ Trained | ~1 KB | 2024-03-31 |
| Economy Risk | `economy_model.pkl` | ⏳ Pending | - | - |
| Geopolitics Risk | `geopolitics_model.pkl` | ⏳ Pending | - | - |
| Infrastructure Risk | `infrastructure_model.pkl` | ⏳ Pending | - | - |
| Migration Risk | `migration_model.pkl` | ⏳ Pending | - | - |
| Social Risk | `social_model.pkl` | ⏳ Pending | - | - |

## 📊 Model Performance Summary

### Climate Model (Completed)
- **R² Score**: 0.2377 (+31% improvement)
- **MAE**: 7.09 (normalized)
- **Features**: 51 engineered features
- **Target**: 5-month rolling climate risk
- **Normalization**: Min-max scaling (0-1 range)

## 🚀 Deployment Checklist

Before deploying to production:

- [ ] All 6 models trained and tested
- [ ] Model performance validated
- [ ] API endpoints working locally
- [ ] Interconnection engine tested
- [ ] Models committed to Git
- [ ] Deployment platform configured
- [ ] Environment variables set
- [ ] Health checks passing
- [ ] Monitoring setup

## 📦 Deployment Notes

When ready to deploy:

```bash
# Commit all models together
git add models/trained/*.pkl
git commit -m "Add all production risk models"
git push origin main

# Then deploy to chosen platform
# Railway, Heroku, Google Cloud Run, etc.
```

## 🔄 Model Retraining Schedule

- **Frequency**: Quarterly or when new data available
- **Trigger**: New year data, performance degradation, feature improvements
- **Process**: 
  1. Add new raw data to `data/raw/`
  2. Regenerate features
  3. Retrain all models
  4. Compare performance
  5. Deploy if improved

## 💾 Backup Strategy

Models are stored locally at:
```
D:\global-risk-interconnection-platform\models\trained\
```

Backup locations:
- [ ] External hard drive
- [ ] Cloud storage (Google Drive/OneDrive)
- [ ] AWS S3 bucket (for production)

## 📈 Next Steps

1. Train remaining 5 models
2. Integrate all into interconnection engine
3. Update API endpoints for each sector
4. Test end-to-end predictions
5. Deploy complete system

---

**Last Updated**: 2024-03-31  
**Status**: 1/6 models completed, 5 pending
