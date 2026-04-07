"""
🌧️ Climate Data Fetcher

Fetches live climate data from weather APIs.

Required Features (from model):
- rainfall
- temperature
- humidity
- groundwater level
- reservoir levels

APIs:
- Open-Meteo (FREE, no key) - IMPLEMENTED ✅
- WeatherAPI (freemium)
- India: IMD, CWC, CGWB

Status: IMPLEMENTED (Open-Meteo API)
"""

import requests
from datetime import datetime, timedelta


def fetch_climate(location: str = "delhi", days: int = 30) -> dict:
    """
    Fetch live climate data from Open-Meteo API.
    
    Args:
        location: Location name (default: "delhi")
        days: Number of historical days to fetch (default: 30)
    
    Returns:
        Dictionary with climate indicators
    """
    # Location coordinates (default: Delhi, India)
    locations = {
        "delhi": (28.6139, 77.2090),
        "mumbai": (19.0760, 72.8777),
        "chennai": (13.0827, 80.2707),
        "bangalore": (12.9716, 77.5946),
        "kolkata": (22.5726, 88.3639),
    }
    
    lat, lon = locations.get(location.lower(), locations["delhi"])
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        # Open-Meteo Historical Weather API (FREE, no key required)
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min,relative_humidity_2m_mean",
            "timezone": "auto"
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if "daily" in data:
            daily_data = data["daily"]
            
            # Calculate averages from historical data
            rainfall_values = daily_data.get("precipitation_sum", [])
            temp_max_values = daily_data.get("temperature_2m_max", [])
            temp_min_values = daily_data.get("temperature_2m_min", [])
            humidity_values = daily_data.get("relative_humidity_2m_mean", [])
            
            # Filter out None values
            rainfall_values = [v for v in rainfall_values if v is not None]
            temp_max_values = [v for v in temp_max_values if v is not None]
            temp_min_values = [v for v in temp_min_values if v is not None]
            humidity_values = [v for v in humidity_values if v is not None]
            
            result = {
                "rainfall": sum(rainfall_values) / len(rainfall_values) if rainfall_values else None,  # Average daily mm
                "temperature": (sum(temp_max_values) / len(temp_max_values) + sum(temp_min_values) / len(temp_min_values)) / 2 if temp_max_values and temp_min_values else None,  # Average °C
                "humidity": sum(humidity_values) / len(humidity_values) if humidity_values else None,  # Average %
                "groundwater": None,     # Requires India WRIS API (manual data)
                "reservoir": None,       # Requires CWC API (manual data)
                "timestamp": datetime.now().isoformat(),
                "location": location,
                "days_fetched": days,
                "data_points": len(rainfall_values)
            }
            
            return result
        else:
            print(f"⚠️  No climate data returned from Open-Meteo")
            return {
                "rainfall": None,
                "temperature": None,
                "humidity": None,
                "groundwater": None,
                "reservoir": None,
                "timestamp": datetime.now().isoformat(),
                "error": "No data from API"
            }
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching climate data: {e}")
        return {
            "rainfall": None,
            "temperature": None,
            "humidity": None,
            "groundwater": None,
            "reservoir": None,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }
