from fastapi import APIRouter

# 🔥 Import all fetchers
from app.live.live_fetcher import fetch_all_live_data
from app.live.climate_fetcher import fetch_climate
from app.live.economy_fetcher import fetch_economy
from app.live.trade_fetcher import fetch_trade
from app.live.geopolitics_fetcher import fetch_geopolitics
from app.live.migration_fetcher import fetch_migration
from app.live.social_fetcher import fetch_social
from app.live.infrastructure_fetcher import fetch_infrastructure
from app.live.live_processor import process_live_data
router = APIRouter(
    prefix="/live",
    tags=["Live Data"]
)

# =========================================================
# 🌐 1. ALL SECTORS
# =========================================================
@router.get("/")
def get_all_live_data():
    """
    Fetch live data from all 7 sectors
    """
    data = fetch_all_live_data()

    return {
        "status": "success",
        "sectors": list(data.keys()),
        "data": data
    }


# =========================================================
# 🌧️ 2. CLIMATE
# =========================================================
@router.get("/climate")
def get_climate(location: str = "delhi", days: int = 30):
    return fetch_climate(location, days)


# =========================================================
# 📈 3. ECONOMY
# =========================================================
@router.get("/economy")
def get_economy():
    return fetch_economy()


# =========================================================
# 💹 4. TRADE
# =========================================================
@router.get("/trade")
def get_trade():
    return fetch_trade()


# =========================================================
# ⚔️ 5. GEOPOLITICS
# =========================================================
@router.get("/geopolitics")
def get_geopolitics():
    return fetch_geopolitics()


# =========================================================
# 🌍 6. MIGRATION
# =========================================================
@router.get("/migration")
def get_migration(country: str = "IN"):
    return fetch_migration(country)


# =========================================================
# 📢 7. SOCIAL
# =========================================================
@router.get("/social")
def get_social():
    return fetch_social()


# =========================================================
# 🏗️ 8. INFRASTRUCTURE
# =========================================================
@router.get("/infrastructure")
def get_infrastructure(country: str = "IN"):
    return fetch_infrastructure(country)


# =========================================================
# 🔥 9. LIVE RISK (FOR FRONTEND)
# =========================================================
@router.get("/risk")
def get_live_risk():
    """
    ML-based live risk endpoint
    """
    try:
        # 🔥 Step 1: Get REAL ML predictions
        final_risk = process_live_data()

        # 🔥 Step 2: Initial risk (baseline)
        initial_risk = {k: 0.5 for k in final_risk.keys()}

        return {
            "initial_risk": initial_risk,
            "final_risk": final_risk
        }

    except Exception as e:
        print(f"❌ Error in live risk processing: {e}")

        # fallback (important for stability)
        fallback = {
            "climate": 0.5,
            "economy": 0.5,
            "trade": 0.5,
            "geopolitics": 0.5,
            "migration": 0.5,
            "social": 0.5,
            "infrastructure": 0.5
        }

        return {
            "initial_risk": fallback,
            "final_risk": fallback,
            "error": str(e)
        }