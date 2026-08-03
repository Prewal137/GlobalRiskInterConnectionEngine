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

import requests
import numpy as np

def fetch_trade() -> dict:
    """
    Fetch live trade data from World Bank API (India).
    Uses multiple years to calculate meaningful metrics.
    """

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
                "per_page": 10
            }

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if len(data) >= 2 and data[1]:
                # Extract valid values (ignore None)
                values = [d["value"] for d in data[1] if d["value"] is not None]

                if len(values) > 0:
                    results[key] = float(values[0])  # latest value

                    # Store full series for calculations
                    results[f"{key}_series"] = values
                else:
                    results[key] = 0
                    results[f"{key}_series"] = []
            else:
                results[key] = 0
                results[f"{key}_series"] = []

        # -------------------------
        # BASIC VALUES
        # -------------------------
        exports = results.get("exports", 0)
        imports = results.get("imports", 0)

        export_series = results.get("exports_series", [])
        import_series = results.get("imports_series", [])

        trade_balance = exports - imports
        total_trade = exports + imports

        # -------------------------
        # GROWTH CALCULATION
        # -------------------------
        def calc_growth(series):
            if len(series) >= 2 and series[-1] != 0:
                return (series[0] - series[-1]) / abs(series[-1])
            return 0

        growth = calc_growth(export_series) + calc_growth(import_series)
        export_growth = calc_growth(export_series)
        import_growth = calc_growth(import_series)

        # -------------------------
        # VOLATILITY (STD DEV)
        # -------------------------
        def calc_volatility(series):
            if len(series) >= 3:
                return float(np.std(series[:3]))
            return 0

        volatility_3 = calc_volatility(export_series) + calc_volatility(import_series)

        # -------------------------
        # ROLLING MEAN
        # -------------------------
        rolling_mean_3 = (
            np.mean(export_series[:3]) + np.mean(import_series[:3])
            if len(export_series) >= 3 and len(import_series) >= 3
            else total_trade
        )

        # -------------------------
        # RATIOS
        # -------------------------
        export_share = exports / total_trade if total_trade > 0 else 0
        import_share = imports / total_trade if total_trade > 0 else 0
        balance_ratio = trade_balance / total_trade if total_trade > 0 else 0

        # -------------------------
        # SHOCK DETECTION
        # -------------------------
        shock = 1 if abs(balance_ratio) > 0.3 else 0

        # -------------------------
        # FINAL RESULT
        # -------------------------
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

        print("✅ Trade data fetched (fixed)")
        print(f"Exports: {exports:.2f}, Imports: {imports:.2f}")
        print(f"Growth: {growth:.4f}, Volatility: {volatility_3:.4f}")

        return result

    except Exception as e:
        print("❌ Error fetching trade data:", str(e))
        return {}
        
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
