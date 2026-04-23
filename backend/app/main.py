from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import climate, trade, interconnection, geopolitics, economy, migration, social, infrastructure,live

app = FastAPI(
    title="Global Risk Interconnection Platform API",
    description="REST API for accessing infrastructre,economy,geopolitical,climate,migration,trade risk predictions across countries and regions",
    version="1.0.0"
)

# CORS Configuration - Allow frontend to access backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
app.include_router(live.router)

@app.get("/")
def root():
    return {
        "message": "Global Risk Interconnection Platform API",
        "version": "1.0.0",

        "available_modules": [
            "climate-risk",
            "trade-risk",
            "geopolitics-risk",
            "economy",
            "social",
            "infrastructure-risk",
            "interconnection",
            "migration",
            "live"
        ],

        "docs": "/docs"
    }