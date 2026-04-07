"""
📈 Economy Data Fetcher

Fetches live economic indicators from financial APIs.

Required Features (from model):
- Inflation
- InterestRate
- ExchangeRate
- NIFTY50
- VIX

APIs:
- World Bank API (FREE) - IMPLEMENTED ✅
- yfinance (FREE, for NIFTY/VIX) - IMPLEMENTED ✅
- FRED (FREE)
- Alpha Vantage (freemium)

Status: IMPLEMENTED (yfinance + World Bank)
"""

import yfinance as yf
import requests
from datetime import datetime


def fetch_economy() -> dict:
    """
    Fetch live economic indicators.
    
    Returns:
        Dictionary with economic indicators
    """
    result = {
        "nifty": None,           # NIFTY 50 index
        "vix": None,             # India VIX
        "inflation": None,       # CPI inflation %
        "interest_rate": None,   # Policy rate %
        "exchange_rate": None,   # USD/INR
        "timestamp": None        # fetch timestamp
    }
    
    # 1. Fetch NIFTY 50 and India VIX from Yahoo Finance
    try:
        # NIFTY 50 (last 30 days)
        nifty = yf.download("^NSEI", period="1mo", progress=False)
        if not nifty.empty:
            result["nifty"] = float(nifty["Close"].iloc[-1].item())
        
        # India VIX (last 30 days)
        vix = yf.download("^INDIAVIX", period="1mo", progress=False)
        if not vix.empty:
            result["vix"] = float(vix["Close"].iloc[-1].item())
            
    except Exception as e:
        print(f"⚠️  Error fetching stock data: {e}")
    
    # 2. Fetch economic indicators from World Bank API
    try:
        # Inflation (CPI) for India
        inflation_data = _fetch_world_bank_indicator("IN", "FP.CPI.TOTL.ZG")
        if inflation_data:
            result["inflation"] = inflation_data
        
        # Interest rate for India
        interest_data = _fetch_world_bank_indicator("IN", "FR.INR.RINR")
        if interest_data:
            result["interest_rate"] = interest_data
            
    except Exception as e:
        print(f"⚠️  Error fetching World Bank data: {e}")
    
    # 3. Fetch USD/INR exchange rate from Yahoo Finance
    try:
        usd_inr = yf.download("USDINR=X", period="1mo", progress=False)
        if not usd_inr.empty:
            result["exchange_rate"] = float(usd_inr["Close"].iloc[-1].item())
    except Exception as e:
        print(f"⚠️  Error fetching exchange rate: {e}")
    
    result["timestamp"] = datetime.now().isoformat()
    
    return result


def _fetch_world_bank_indicator(country_code: str, indicator_code: str) -> float:
    """
    Fetch latest value for a World Bank indicator.
    
    Args:
        country_code: ISO country code (e.g., "IN" for India)
        indicator_code: World Bank indicator code
    
    Returns:
        Latest indicator value or None
    """
    try:
        url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
        params = {
            "date": "2020:2024",  # Get recent data
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
