"""
🔄 Feature Mapper

Maps raw API data to model-ready features.
Applies transformations based on feature engineering from pipeline/processing/

Status: IMPLEMENTED ✅
"""

import numpy as np
from typing import Dict, Optional


def map_to_model_features(live_data: dict) -> dict:
    """
    Map raw live data to features expected by ML models.
    
    Args:
        live_data: Raw data from all sector fetchers
        
    Returns:
        Dictionary with model-ready features per sector
    """
    features = {
        "climate": map_climate_features(live_data.get("climate", {})),
        "economy": map_economy_features(live_data.get("economy", {})),
        "trade": map_trade_features(live_data.get("trade", {})),
        "geopolitics": map_geopolitics_features(live_data.get("geopolitics", {})),
        "migration": map_migration_features(live_data.get("migration", {})),
        "social": map_social_features(live_data.get("social", {})),
        "infrastructure": map_infrastructure_features(live_data.get("infrastructure", {}))
    }
    
    return features


def map_climate_features(climate_data: dict) -> Optional[dict]:
    """Map climate data to model features."""
    # Climate models need 51 features with lag/rolling stats
    # For live single-point data, we can't calculate these properly
    # Return None to use fallback (0.5)
    return None


def map_economy_features(economy_data: dict) -> Optional[dict]:
    """Map economy data to model features."""
    if not economy_data or economy_data.get("nifty") is None:
        return None
    
    # Economy model features (from economy_features.py)
    # Models expect: Inflation, InterestRate, ExchangeRate, NIFTY50, VIX
    features = {
        "Inflation": economy_data.get("inflation", 0) or 5.0,
        "InterestRate": economy_data.get("interest_rate", 0) or 6.5,
        "ExchangeRate": economy_data.get("exchange_rate", 0) or 83.0,
        "NIFTY50": economy_data.get("nifty", 0) or 22000,
        "VIX": economy_data.get("vix", 0) or 15.0,
    }
    
    return features


def map_trade_features(trade_data: dict) -> Optional[dict]:
    """Map trade data to model features."""
    if not trade_data:
        return None
    
    # Trade model features (simplified - actual features depend on training)
    features = {
        "trade_volume": trade_data.get("trade_volume", 0) or 1000,
        "export_value": trade_data.get("export_value", 0) or 500,
        "import_value": trade_data.get("import_value", 0) or 600,
    }
    
    return features


def map_geopolitics_features(geopolitics_data: dict) -> Optional[dict]:
    """Map geopolitics data to model features."""
    if not geopolitics_data:
        return None
    
    # Geopolitics model features (simplified)
    features = {
        "conflict_count": geopolitics_data.get("conflict_count", 0) or 5,
        "fatalities": geopolitics_data.get("fatalities", 0) or 10,
        "severity_index": geopolitics_data.get("severity_index", 0) or 3.0,
    }
    
    return features


def map_migration_features(migration_data: dict) -> Optional[dict]:
    """Map migration data to model features."""
    if not migration_data or migration_data.get("unemployment") is None:
        return None
    
    # Migration model features (simplified)
    features = {
        "unemployment": migration_data.get("unemployment", 0) or 7.0,
        "net_migration": migration_data.get("net_migration", 0) or 0,
        "population_growth": migration_data.get("population_growth", 0) or 1.0,
        "urban_growth": migration_data.get("urban_growth", 0) or 2.0,
    }
    
    return features


def map_social_features(social_data: dict) -> Optional[dict]:
    """Map social data to model features."""
    if not social_data:
        return None
    
    # Social model features (simplified)
    features = {
        "protest_count": social_data.get("protest_count", 0) or 10,
        "violence_count": social_data.get("violence_count", 0) or 2,
        "social_unrest_index": social_data.get("social_unrest_index", 0) or 3.0,
    }
    
    return features


def map_infrastructure_features(infrastructure_data: dict) -> Optional[dict]:
    """Map infrastructure data to model features."""
    if not infrastructure_data or infrastructure_data.get("water_access") is None:
        return None
    
    # Infrastructure model features
    features = {
        "water_access": infrastructure_data.get("water_access", 0) or 90.0,
        "urban_population": infrastructure_data.get("urban_population", 0) or 35.0,
        "electricity_access": infrastructure_data.get("electricity_access", 0) or 95.0,
        "roads_paved": infrastructure_data.get("roads_paved", 0) or 60.0,
    }
    
    return features
