"""
🏗️ Infrastructure Data Fetcher

Fetches live infrastructure and urban development data.

Required Features (from model):
- water_access
- urban_population
- total_revenue
- municipal_revenue
- avg_revenue

APIs:
- World Bank API (FREE) - IMPLEMENTED ✅
- India: Census, NITI Aayog, MoHUA
- OECD Regional Statistics

Status: IMPLEMENTED (World Bank API)
"""

import requests
from datetime import datetime


def fetch_infrastructure(country_code: str = "IN") -> dict:
    """
    Fetch live infrastructure data from World Bank API.
    
    Args:
        country_code: ISO country code (default: "IN" for India)
    
    Returns:
        Dictionary with infrastructure indicators
    """
    result = {
        "water_access": None,         # % with drinking water
        "urban_population": None,     # Urban population %
        "total_revenue": None,        # Municipal revenue (not available in World Bank)
        "municipal_revenue": None,    # Municipal revenue (not available in World Bank)
        "avg_revenue": None,          # Average revenue (not available in World Bank)
        "electricity_access": None,   # % with electricity access
        "roads_paved": None,          # % roads paved
        "timestamp": None             # fetch timestamp
    }
    
    # World Bank indicators
    indicators = {
        "water_access": "SH.H2O.BASW.ZS",        # People using at least basic drinking water services
        "urban_population": "SP.URB.TOTL.IN.ZS", # Urban population (% of total)
        "electricity_access": "EG.ELC.ACCS.ZS",  # Access to electricity (% of population)
        "roads_paved": "IS.ROD.PAVE.ZS"          # Roads paved (% of total roads)
    }
    
    # Fetch each indicator
    for key, indicator_code in indicators.items():
        try:
            value = _fetch_world_bank_indicator(country_code, indicator_code)
            if value is not None:
                result[key] = value
        except Exception as e:
            print(f"⚠️  Error fetching {key}: {e}")
    
    result["timestamp"] = datetime.now().isoformat()
    result["country"] = country_code
    result["note"] = "Revenue data requires India-specific sources (Census/MoHUA)"
    
    return result


def _fetch_world_bank_indicator(country_code: str, indicator_code: str) -> float:
    """
    Fetch latest value for a World Bank indicator.
    
    Args:
        country_code: ISO country code
        indicator_code: World Bank indicator code
    
    Returns:
        Latest indicator value or None
    """
    try:
        url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
        params = {
            "date": "2020:2024",
            "format": "json",
            "per_page": 10
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # World Bank returns [metadata, data_list]
        if len(data) > 1 and data[1]:
            # Get the most recent non-null value
            for item in data[1]:
                if item.get("value") is not None:
                    return float(item["value"])
        
        return None
        
    except Exception as e:
        print(f"⚠️  Error fetching {indicator_code}: {e}")
        return None
