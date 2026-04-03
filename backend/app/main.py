from fastapi import FastAPI
from app.routes import climate, trade, interconnection, geopolitics

app = FastAPI(
    title="Global Risk Interconnection Platform API",
    description="REST API for accessing geopolitical, climate, and trade risk predictions across countries and regions",
    version="1.0.0"
)

# Include routers
app.include_router(climate.router)
app.include_router(trade.router)
app.include_router(interconnection.router)
app.include_router(geopolitics.router)

@app.get("/")
def root():
    """
    Root endpoint - API information
    """
    return {
        "message": "Global Risk Interconnection Platform API",
        "version": "1.0.0",
        "endpoints": {
            "climate_risk": "/climate-risk/",
            "trade_risk": "/trade-risk/",
            "geopolitics_risk": "/geopolitics-risk/",
            "global_risk": "/global-risk/",
            "docs": "/docs"
        },
        "geopolitics_endpoints": {
            "country_risk": "/geopolitics-risk/country/{country}",
            "top_countries": "/geopolitics-risk/top-countries",
            "global_summary": "/geopolitics-risk/global-summary",
            "compare": "/geopolitics-risk/compare?countries=IND,USA,CHN (🔥 NEW!)",
            "search": "/geopolitics-risk/search/{query}"
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
