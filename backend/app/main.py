from fastapi import FastAPI
from app.routes import climate

app = FastAPI(
    title="Climate Risk Prediction API",
    description="REST API for accessing climate risk predictions across Indian states and districts",
    version="1.0.0"
)

# Include climate risk router
app.include_router(climate.router)

@app.get("/")
def root():
    """
    Root endpoint - API information
    """
    return {
        "message": "Climate Risk Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "state_risk": "/climate-risk/state/{state}",
            "district_risk": "/climate-risk/district/{district}",
            "top_states": "/climate-risk/top-states",
            "top_districts": "/climate-risk/top-districts",
            "all_states": "/climate-risk/states",
            "all_districts": "/climate-risk/districts"
        },
        "docs": "/docs"
    }

@app.get("/health")
def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"}