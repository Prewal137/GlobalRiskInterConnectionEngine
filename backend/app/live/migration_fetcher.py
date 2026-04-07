"""
🌍 Migration Data Fetcher

Fetches live migration and demographic data.

Required Features (from model):
- Unemployment rate
- Net migration rate
- Population growth
- Urban population growth

APIs:
- World Bank API (FREE, already in use) - IMPLEMENTED ✅
- UN DESA Migration (FREE)
- IOM (International Organization for Migration)

Status: IMPLEMENTED (World Bank API)
"""

import requests
from datetime import datetime


def fetch_migration(country_code: str = "IN") -> dict:
    """
    Fetch live migration data from World Bank API.
    
    Args:
        country_code: ISO country code (default: "IN" for India)
    
    Returns:
        Dictionary with migration indicators
    """
    result = {
        "unemployment": None,          # Unemployment rate %
        "net_migration": None,         # Net migration rate
        "population_growth": None,     # Population growth %
        "urban_growth": None,          # Urban population growth %
        "timestamp": None              # fetch timestamp
    }
    
    # World Bank indicators
    indicators = {
        "unemployment": "SL.UEM.TOTL.ZS",
        "net_migration": "SM.POP.NETM",
        "population_growth": "SP.POP.GROW",
        "urban_growth": "SP.URB.GROW"
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
