from fastapi import FastAPI
from app.routes import climate, trade, interconnection, geopolitics, economy, migration, social, infrastructure

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
app.include_router(economy.router)
app.include_router(migration.router)
app.include_router(social.router)
app.include_router(infrastructure.router)

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
            "economy_risk": "/economy/",
            "social_risk": "/social/",
            "infrastructure_risk": "/infrastructure-risk/",  # NEW!
            "global_risk": "/global-risk/",
            "docs": "/docs"
        },
        "economy_endpoints": {
            "risk_by_date": "/economy/risk/{country}/{year}/{month}",
            "latest_risk": "/economy/latest/{country}",
            "risk_trend": "/economy/trend/{country}",
            "high_risk_months": "/economy/high-risk/{country}",
            "available_countries": "/economy/countries",
            "country_summary": "/economy/summary/{country}"
        },
        "social_endpoints": {  # 🆕 SOCIAL RISK
            "risk_by_date": "/social/risk/{state}/{year}/{month}",
            "latest_risk": "/social/latest/{state}",
            "risk_trend": "/social/trend/{state}",
            "high_risk_months": "/social/high-risk/{state}",
            "available_states": "/social/states",
            "state_summary": "/social/summary/{state}"
        },
        "infrastructure_endpoints": {  # 🏗️ INFRASTRUCTURE RISK
            "risk_by_year": "/infrastructure-risk/state/{state}/{year}",
            "latest_risk": "/infrastructure-risk/latest/{state}",
            "risk_trend": "/infrastructure-risk/trend/{state}",
            "top_risky_states": "/infrastructure-risk/top-states",
            "available_states": "/infrastructure-risk/states",
            "summary_stats": "/infrastructure-risk/summary"
        },
        "interconnection_endpoints": {  # 🔗 MULTI-SECTOR CASCADING RISK (7 Sectors)
            "risk_by_date": "/interconnection/risk/{country}/{year}/{month}",
            "latest_risk": "/interconnection/latest/{country}",
            "trend_analysis": "/interconnection/trend/{country}",
            "high_risk_months": "/interconnection/high-risk/{country}",
            "country_summary": "/interconnection/summary/{country}",
            "dynamic_graph": "/interconnection/dynamic",  # 🆕 LEARNING-BASED GRAPH
            "shock_simulation": "/interconnection/shock/{sector}/{value}",  # 🆕 SHOCK TEST
            "compare_models": "/interconnection/compare",  # 🆕 STATIC vs DYNAMIC
            "custom_scenario": "/interconnection/custom (POST)",  # 🆕 CUSTOM INPUT
            "sectors": ["climate", "economy", "trade", "geopolitics", "migration", "social", "infrastructure"]
        },
        "migration_endpoints": {  # 🌍 MIGRATION RISK
            "risk_by_year": "/migration/risk/{country}/{year}",
            "latest_risk": "/migration/latest/{country}",
            "trend_analysis": "/migration/trend/{country}",
            "high_risk_years": "/migration/high-risk/{country}",
            "country_summary": "/migration/summary/{country}"
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
