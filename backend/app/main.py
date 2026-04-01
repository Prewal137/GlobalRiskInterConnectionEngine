from fastapi import FastAPI
from app.routes import climate, trade, interconnection

app = FastAPI(
    title="Climate Risk Prediction API",
    description="REST API for accessing climate risk predictions across Indian states and districts",
    version="1.0.0"
)

# Include routers
app.include_router(climate.router)
app.include_router(trade.router)
app.include_router(interconnection.router)

@app.get("/")
def root():
    """
    Root endpoint - API information
    """
    return {
        "message": "Climate & Trade Risk Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "climate_risk": "/climate-risk/",
            "trade_risk": "/trade-risk/",
            "global_risk": "/global-risk/",
            "docs": "/docs"
        },
        "trade_endpoints": {
            "country_risk": "/trade-risk/country/{country}",
            "top_countries": "/trade-risk/top",
            "all_data": "/trade-risk/all",
            "summary": "/trade-risk/summary"
        },
        "global_risk_endpoints": {
            "summary": "/global-risk/summary",
            "top_states": "/global-risk/top",
            "state_risk": "/global-risk/state/{state}",
            "all_states": "/global-risk/all"
        }
    }

@app.get("/health")
def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"}