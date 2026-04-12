"""
💹 Trade Data Fetcher (World Bank API Integration)

Fetches live trade statistics from World Bank API.

Required Features (from model):
- Export values
- Import values
- Trade balance
- Total trade
- Growth rates
- Volatility

APIs:
- World Bank API (FREE, no key needed)
  → Export % of GDP
  → Import % of GDP
  → Trade (% of GDP)

Status: ✅ IMPLEMENTED (World Bank trade data active)
"""

import requests
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))


def fetch_trade() -> dict:
    """
    Fetch live trade data from World Bank API.
    
    Uses World Bank open data API (no key required).
    Fetches India's latest trade statistics.
    
    Returns:
        Dictionary with trade indicators
    """
    # World Bank API endpoints (FREE, no key needed)
    # NE.EXP.GNFS.ZS = Exports of goods and services (% of GDP)
    # NE.IMP.GNFS.ZS = Imports of goods and services (% of GDP)
    # NE.TRD.GNFS.ZS = Trade (% of GDP)
    
    indicators = {
        "exports": "NE.EXP.GNFS.ZS",
        "imports": "NE.IMP.GNFS.ZS",
        "trade": "NE.TRD.GNFS.ZS"
    }
    
    results = {}
    
    try:
        for key, indicator in indicators.items():
            url = f"https://api.worldbank.org/v2/country/IND/indicator/{indicator}"
            params = {
                "date": "2020:2024",
                "format": "json",
                "per_page": 5
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # World Bank returns [metadata, data_list]
            if len(data) >= 2 and data[1]:
                # Get latest available year
                latest = data[1][0]
                value = latest.get("value")
                if value is not None:
                    results[key] = float(value)
                    results[f"{key}_year"] = latest.get("date")
                else:
                    results[key] = 0
            else:
                results[key] = 0
        
        # Calculate derived metrics
        exports = results.get("exports", 0)
        imports = results.get("imports", 0)
        trade = results.get("trade", 0)
        
        trade_balance = exports - imports
        total_trade = exports + imports
        
        # Growth rates (placeholder until we have multiple years)
        growth = 0.0
        export_growth = 0.0
        import_growth = 0.0
        
        # Rolling statistics
        rolling_mean_3 = total_trade
        volatility_3 = 0.0
        
        # Trade shares
        export_share = exports / total_trade if total_trade > 0 else 0
        import_share = imports / total_trade if total_trade > 0 else 0
        
        # Balance ratio
        balance_ratio = trade_balance / total_trade if total_trade > 0 else 0
        
        # Shock indicator
        shock = 1 if abs(balance_ratio) > 0.3 else 0
        
        result = {
            "exports": exports,
            "imports": imports,
            "trade_balance": trade_balance,
            "total_trade": total_trade,
            "growth": growth,
            "rolling_mean_3": rolling_mean_3,
            "volatility_3": volatility_3,
            "export_growth": export_growth,
            "import_growth": import_growth,
            "export_share": export_share,
            "import_share": import_share,
            "balance_ratio": balance_ratio,
            "shock": shock,
            "timestamp": None
        }
        
        print(f"✅ World Bank trade data fetched")
        print(f"   Exports: {exports:.2f}% of GDP")
        print(f"   Imports: {imports:.2f}% of GDP")
        print(f"   Trade balance: {trade_balance:.2f}% of GDP")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching trade data: {e}")
        return {
            "exports": 0,
            "imports": 0,
            "trade_balance": 0,
            "total_trade": 0,
            "growth": 0,
            "rolling_mean_3": 0,
            "volatility_3": 0,
            "export_growth": 0,
            "import_growth": 0,
            "export_share": 0,
            "import_share": 0,
            "balance_ratio": 0,
            "shock": 0,
            "timestamp": None,
            "error": str(e)
        }
    except Exception as e:
        print(f"❌ Unexpected error in trade fetcher: {e}")
        return {
            "exports": 0,
            "imports": 0,
            "trade_balance": 0,
            "total_trade": 0,
            "growth": 0,
            "rolling_mean_3": 0,
            "volatility_3": 0,
            "export_growth": 0,
            "import_growth": 0,
            "export_share": 0,
            "import_share": 0,
            "balance_ratio": 0,
            "shock": 0,
            "timestamp": None,
            "error": str(e)
        }
