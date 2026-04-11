"""
⚔️ Geopolitics Data Fetcher (ACLED API Integration)

Fetches live conflict and geopolitical risk data from ACLED.

Required Features (from model):
- conflict_count
- fatalities_sum
- deaths_total
- conflict_intensity
- policy_uncertainty
- global_uncertainty

APIs:
- ACLED (OAuth authentication, FREE for research)
  → Conflict events
  → Fatalities data
  → Event types

Status: ✅ IMPLEMENTED (ACLED OAuth integration active)
"""

import requests
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from backend.app.core.config import ACLED_EMAIL, ACLED_PASSWORD


def get_acled_token() -> str:
    """
    Get OAuth token from ACLED API.
    
    Returns:
        Access token string or None if failed
    """
    if not ACLED_EMAIL or not ACLED_PASSWORD:
        print("⚠️  ACLED credentials not configured in .env")
        return None
    
    url = "https://acleddata.com/oauth/token"
    
    # OAuth token request with form data (not JSON)
    payload = {
        "username": ACLED_EMAIL,
        "password": ACLED_PASSWORD,
        "grant_type": "password",
        "client_id": "acled"
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(url, data=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        token = data.get("access_token")
        if token:
            print("✅ ACLED OAuth token obtained successfully")
            print(f"   Token expires in: {data.get('expires_in', 'unknown')} seconds")
        
        return token
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ACLED token error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error getting ACLED token: {e}")
        return None


def fetch_geopolitics() -> dict:
    """
    Fetch live geopolitical risk data from ACLED API.
    
    Uses OAuth 2-step authentication:
    1. Get access token
    2. Fetch conflict events for India
    
    Returns:
        Dictionary with geopolitical indicators
    """
    # Step 1: Get OAuth token
    token = get_acled_token()
    
    if not token:
        print("⚠️  Could not get ACLED token, returning defaults")
        return {
            "conflict_count": 0,
            "fatalities": 0,
            "deaths_total": 0,
            "conflict_intensity": 0,
            "policy_uncertainty": 0,
            "global_uncertainty": 0,
            "timestamp": None
        }
    
    # Step 2: Fetch conflict data using OAuth token
    url = "https://acleddata.com/api/acled/read"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    params = {
        "country": "India",
        "limit": 50
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        events = data.get("data", [])
        
        if not events:
            print("⚠️  No ACLED events returned")
            return {
                "conflict_count": 0,
                "fatalities": 0,
                "deaths_total": 0,
                "conflict_intensity": 0,
                "policy_uncertainty": 0,
                "global_uncertainty": 0,
                "timestamp": None
            }
        
        # Calculate indicators
        conflict_count = len(events)
        
        fatalities = sum(
            int(event.get("fatalities", 0) or 0) for event in events
        )
        
        deaths_total = fatalities  # Same metric for ACLED
        
        # Calculate conflict intensity (fatalities per event)
        conflict_intensity = fatalities / conflict_count if conflict_count > 0 else 0
        
        # Event type analysis
        event_types = {}
        for event in events:
            event_type = event.get("event_type", "Unknown")
            event_types[event_type] = event_types.get(event_type, 0) + 1
        
        # Policy uncertainty proxy (protests + riots)
        policy_events = event_types.get("Protests", 0) + event_types.get("Riots", 0)
        policy_uncertainty = policy_events / conflict_count if conflict_count > 0 else 0
        
        # Global uncertainty proxy (battles + violence)
        violence_events = event_types.get("Battles", 0) + event_types.get("Violence against civilians", 0)
        global_uncertainty = violence_events / conflict_count if conflict_count > 0 else 0
        
        result = {
            "conflict_count": conflict_count,
            "fatalities": fatalities,
            "deaths_total": deaths_total,
            "conflict_intensity": conflict_intensity,
            "policy_uncertainty": policy_uncertainty,
            "global_uncertainty": global_uncertainty,
            "event_types": event_types,
            "timestamp": data.get("timestamp", None)
        }
        
        print(f"✅ ACLED data fetched: {conflict_count} events")
        print(f"   Fatalities: {fatalities}")
        print(f"   Conflict intensity: {conflict_intensity:.2f}")
        print(f"   Event types: {event_types}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching ACLED data: {e}")
        return {
            "conflict_count": 0,
            "fatalities": 0,
            "deaths_total": 0,
            "conflict_intensity": 0,
            "policy_uncertainty": 0,
            "global_uncertainty": 0,
            "timestamp": None,
            "error": str(e)
        }
    except Exception as e:
        print(f"❌ Unexpected error in ACLED fetcher: {e}")
        return {
            "conflict_count": 0,
            "fatalities": 0,
            "deaths_total": 0,
            "conflict_intensity": 0,
            "policy_uncertainty": 0,
            "global_uncertainty": 0,
            "timestamp": None,
            "error": str(e)
        }
