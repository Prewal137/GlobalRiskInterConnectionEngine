"""
🚀 Quick Start Script: Fetch Live Data for All Sectors

This script demonstrates how to fetch data from recommended APIs
for each of the 7 sectors in your Global Risk Platform.

Usage:
    python quick_api_examples.py

Prerequisites:
    pip install requests yfinance pandas
"""

import requests
import pandas as pd
from datetime import datetime


def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)


# ================================================================
# 1️⃣ CLIMATE DATA (Open-Meteo - FREE, no API key)
# ================================================================

def fetch_climate_data():
    """Fetch climate data from Open-Meteo API"""
    print_section("1️⃣ CLIMATE DATA - Open-Meteo (FREE)")
    
    # Example: Get historical rainfall for Delhi, India
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 28.6139,  # Delhi
        "longitude": 77.2090,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "daily": "precipitation_sum",
        "timezone": "Asia/Kolkata"
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        if "daily" in data:
            df = pd.DataFrame({
                "date": data["daily"]["time"],
                "rainfall_mm": data["daily"]["precipitation_sum"]
            })
            
            print(f"✅ Fetched {len(df)} days of rainfall data")
            print(f"📊 Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"💧 Average rainfall: {df['rainfall_mm'].mean():.2f} mm/day")
            print(f"\n📄 Sample data:")
            print(df.head(10))
            
            return df
        else:
            print("❌ No data returned")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


# ================================================================
# 2️⃣ ECONOMY DATA (World Bank - FREE, no API key)
# ================================================================

def fetch_economy_data():
    """Fetch economic indicators from World Bank API"""
    print_section("2️⃣ ECONOMY DATA - World Bank (FREE)")
    
    indicators = {
        "Inflation": "FP.CPI.TOTL.ZG",
        "GDP Growth": "NY.GDP.MKTP.KD.ZG",
        "Unemployment": "SL.UEM.TOTL.ZS"
    }
    
    country = "IN"  # India
    start_year = 2020
    end_year = 2024
    
    all_data = {}
    
    for name, indicator_code in indicators.items():
        try:
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
            params = {
                "date": f"{start_year}:{end_year}",
                "format": "json",
                "per_page": 100
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if len(data) > 1 and data[1]:
                df = pd.DataFrame(data[1])
                df["date"] = pd.to_datetime(df["date"])
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                
                all_data[name] = df[["date", "value"]]
                
                print(f"✅ {name}: {len(df)} years of data")
                print(f"   Latest value: {df.iloc[0]['value']:.2f} ({df.iloc[0]['date'].year})")
            else:
                print(f"⚠️  No data for {name}")
                
        except Exception as e:
            print(f"❌ Error fetching {name}: {e}")
    
    return all_data


# ================================================================
# 3️⃣ ECONOMY DATA (Yahoo Finance - FREE, for NIFTY/VIX)
# ================================================================

def fetch_stock_data():
    """Fetch NIFTY 50 and India VIX from Yahoo Finance"""
    print_section("3️⃣ STOCK MARKET DATA - Yahoo Finance (FREE)")
    
    try:
        import yfinance as yf
        
        # NIFTY 50
        print("📈 Fetching NIFTY 50...")
        nifty = yf.download("^NSEI", start="2024-01-01", end="2024-12-31")
        
        if not nifty.empty:
            print(f"✅ NIFTY 50: {len(nifty)} days of data")
            print(f"   Latest close: {nifty['Close'].iloc[-1]:.2f}")
            print(f"   Year high: {nifty['High'].max():.2f}")
            print(f"   Year low: {nifty['Low'].min():.2f}")
        
        # India VIX
        print("\n📊 Fetching India VIX...")
        vix = yf.download("^INDIAVIX", start="2024-01-01", end="2024-12-31")
        
        if not vix.empty:
            print(f"✅ India VIX: {len(vix)} days of data")
            print(f"   Latest VIX: {vix['Close'].iloc[-1]:.2f}")
            print(f"   Average VIX: {vix['Close'].mean():.2f}")
        
        return nifty, vix
        
    except ImportError:
        print("❌ yfinance not installed. Run: pip install yfinance")
        return None, None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None


# ================================================================
# 4️⃣ GEOPOLITICS DATA (ACLED - FREE for research)
# ================================================================

def fetch_geopolitics_data():
    """Fetch conflict data from ACLED API"""
    print_section("4️⃣ GEOPOLITICS DATA - ACLED (FREE for research)")
    
    print("⚠️  ACLED requires API key registration")
    print("📝 Register at: https://acleddata.com/register/")
    print("\nExample code (once you have API key):")
    print("""
    import requests
    
    url = "https://api.acleddata.com/acled/read.json"
    params = {
        'key': 'YOUR_API_KEY',
        'event_type': 'Battles',
        'country': 'India',
        'year': '2024',
        'limit': 1000
    }
    
    response = requests.get(url, params=params)
    events = response.json()
    
    # Aggregate by month
    # This gives you: conflict_count, fatalities_sum, etc.
    """)
    
    return None


# ================================================================
# 5️⃣ TRADE DATA (UN Comtrade - FREE registration)
# ================================================================

def fetch_trade_data():
    """Fetch trade data from UN Comtrade API"""
    print_section("5️⃣ TRADE DATA - UN Comtrade (FREE)")
    
    print("📊 Example: Fetch India's trade data")
    print("\nCode example:")
    print("""
    import requests
    
    url = "https://comtradeapi.un.org/data/v1/get"
    params = {
        'type': 'C',
        'freq': 'M',
        'ps': '2024',
        'r': '356',  # India
        'p': 'all',
        'rg': '1,2',  # Export + Import
        'cc': 'TOTAL'
    }
    
    response = requests.get(url, params=params)
    trade_data = response.json()
    """)
    
    return None


# ================================================================
# 6️⃣ MIGRATION DATA (World Bank - Already using!)
# ================================================================

def fetch_migration_data():
    """Fetch migration data from World Bank API"""
    print_section("6️⃣ MIGRATION DATA - World Bank (FREE)")
    
    indicators = {
        "Net Migration": "SM.POP.NETM",
        "Population Growth": "SP.POP.GROW",
        "Urban Population Growth": "SP.URB.GROW"
    }
    
    for name, code in indicators.items():
        try:
            url = f"https://api.worldbank.org/v2/country/IN/indicator/{code}"
            params = {
                "date": "2020:2024",
                "format": "json",
                "per_page": 100
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if len(data) > 1 and data[1]:
                latest = data[1][0]
                print(f"✅ {name}: {latest['value']:.2f} ({latest['date']})")
            else:
                print(f"⚠️  No data for {name}")
                
        except Exception as e:
            print(f"❌ Error: {e}")


# ================================================================
# 7️⃣ SOCIAL DATA (GDELT - FREE via BigQuery)
# ================================================================

def fetch_social_data():
    """Fetch social unrest data from GDELT Project"""
    print_section("7️⃣ SOCIAL DATA - GDELT Project (FREE)")
    
    print("📊 GDELT requires Google BigQuery setup")
    print("📝 Setup: https://cloud.google.com/bigquery (free tier: 1TB/month)")
    print("\nExample SQL query:")
    print("""
    SELECT
      DATE(ActionGeo_CountryCode) as date,
      COUNT(*) as event_count
    FROM `gdelt-bq.gdeltv2.events`
    WHERE
      ActionGeo_CountryCode = 'IN'
      AND EventType IN ('14', '15', '16')  -- Protests, Riots
    GROUP BY date
    ORDER BY date
    """)
    
    return None


# ================================================================
# MAIN EXECUTION
# ================================================================

def main():
    """Run all API examples"""
    print("\n" + "🚀"*35)
    print("  GLOBAL RISK PLATFORM - LIVE DATA API EXAMPLES")
    print("🚀"*35)
    print(f"\n  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Testing free APIs for all 7 sectors")
    
    # Run examples
    fetch_climate_data()
    fetch_economy_data()
    fetch_stock_data()
    fetch_geopolitics_data()
    fetch_trade_data()
    fetch_migration_data()
    fetch_social_data()
    
    # Summary
    print_section("SUMMARY")
    print("""
✅ WORKING NOW (no registration):
  1. Open-Meteo (Climate)
  2. World Bank API (Economy, Migration, Infrastructure)
  3. Yahoo Finance (NIFTY, VIX)

⚠️  NEEDS REGISTRATION:
  4. ACLED (Geopolitics, Social) - FREE for research
  5. UN Comtrade (Trade) - FREE
  6. GDELT/BigQuery (Social) - FREE tier

📝 NEXT STEPS:
  1. Register for ACLED API key
  2. Set up Google BigQuery for GDELT
  3. Create automated data fetchers in pipeline/data_fetch/
  4. Schedule daily/weekly updates
    """)
    
    print("\n" + "="*70)
    print("✅ API testing complete!")
    print("="*70)


if __name__ == "__main__":
    main()
