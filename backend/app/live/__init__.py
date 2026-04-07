"""
🌐 Live Data Integration Framework

Provides real-time/near-real-time data fetching for all 7 risk sectors.

Sectors:
- Climate
- Economy
- Trade
- Geopolitics
- Migration
- Social
- Infrastructure

Status: STRUCTURE READY (No real API calls yet)
"""

from .climate_fetcher import fetch_climate
from .economy_fetcher import fetch_economy
from .trade_fetcher import fetch_trade
from .geopolitics_fetcher import fetch_geopolitics
from .migration_fetcher import fetch_migration
from .social_fetcher import fetch_social
from .infrastructure_fetcher import fetch_infrastructure
from .live_fetcher import fetch_all_live_data
from .feature_mapper import map_to_model_features
from .live_processor import process_live_data

__all__ = [
    "fetch_climate",
    "fetch_economy",
    "fetch_trade",
    "fetch_geopolitics",
    "fetch_migration",
    "fetch_social",
    "fetch_infrastructure",
    "fetch_all_live_data",
    "map_to_model_features",
    "process_live_data"
]
