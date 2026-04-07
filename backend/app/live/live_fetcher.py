"""
🌐 Master Live Data Fetcher

Orchestrates data fetching from all 7 sector APIs.
"""

from .climate_fetcher import fetch_climate
from .economy_fetcher import fetch_economy
from .trade_fetcher import fetch_trade
from .geopolitics_fetcher import fetch_geopolitics
from .migration_fetcher import fetch_migration
from .social_fetcher import fetch_social
from .infrastructure_fetcher import fetch_infrastructure


def fetch_all_live_data() -> dict:
    """
    Fetch live data from all 7 sectors.
    
    Returns:
        Dictionary with all sector data
    """
    print("\n🌐 Fetching live data from all sectors...")
    
    data = {
        "climate": fetch_climate(),
        "economy": fetch_economy(),
        "trade": fetch_trade(),
        "geopolitics": fetch_geopolitics(),
        "migration": fetch_migration(),
        "social": fetch_social(),
        "infrastructure": fetch_infrastructure()
    }
    
    print("✅ Live data fetch complete")
    print(f"   Sectors fetched: {len(data)}")
    
    return data
