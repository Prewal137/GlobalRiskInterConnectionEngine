# 🌐 Live Data API Requirements for Global Risk Platform

**Analysis Date:** 2026-04-06  
**Based on:** Actual model features from pipeline/processing/  
**Sectors:** 7 (Climate, Economy, Trade, Geopolitics, Migration, Social, Infrastructure)

---

## 📊 SUMMARY TABLE

| Sector | Primary Data Type | Update Frequency | Best Free API | Best Paid API |
|--------|------------------|------------------|---------------|---------------|
| **Climate** | Weather & Water | Daily | Open-Meteo | WeatherAPI |
| **Economy** | Macroeconomic | Monthly | World Bank | FRED (St. Louis Fed) |
| **Trade** | Import/Export | Annual/Monthly | UN Comtrade | World Bank WITS |
| **Geopolitics** | Conflict Events | Real-time | ACLED (free for research) | GDELT Project |
| **Migration** | Population Movement | Annual | World Bank API | UN DESA |
| **Social** | Protest/Violence Events | Daily | GDELT Project | ACLED |
| **Infrastructure** | Urban Development | Annual | World Bank | National Statistics |

---

## 1️⃣ CLIMATE RISK

### Required Features (from model):
```yaml
Core inputs:
  - rainfall (monthly/daily)
  - groundwater level
  - reservoir levels
  - deviation (from average)

Derived features:
  - rainfall_lag_1, lag_2, lag_3
  - groundwater_lag_1, lag_2
  - reservoir_lag_1
  - rainfall_roll_3 (3-month rolling mean)
  - rainfall_trend
  - rainfall_std_3 (volatility)
  - month_sin, month_cos (seasonality)
```

### Suggested APIs:

#### ✅ **Open-Meteo** (FREE - RECOMMENDED FOR RESEARCH)
- **Website:** https://open-meteo.com/
- **Data Types:** Historical weather, precipitation, temperature
- **Update Frequency:** Daily
- **Coverage:** Global
- **Endpoints:**
  ```
  https://api.open-meteo.com/v1/forecast?
    latitude={lat}&longitude={lon}
    &daily=precipitation_sum
    &start_date=2020-01-01&end_date=2024-12-31
  ```
- **Pros:** 
  - ✅ Completely free for research
  - ✅ No API key required
  - ✅ Historical data available
  - ✅ Global coverage
- **Cons:** 
  - ❌ No groundwater/reservoir data
  - ❌ Need to aggregate daily → monthly

#### ⭐ **WeatherAPI** (FREEMIUM)
- **Website:** https://www.weatherapi.com/
- **Plan:** Free (1M calls/month), Paid ($4/month+)
- **Data Types:** Current, historical, forecast weather
- **Endpoints:**
  ```
  http://api.weatherapi.com/v1/history.json?
    key={API_KEY}&q={location}&dt={date}
  ```
- **Pros:** Easy to use, reliable
- **Cons:** Limited historical data on free tier

#### 💧 **USGS Water Data** (FREE - For Groundwater)
- **Website:** https://waterservices.usgs.gov/
- **Data Types:** Groundwater levels, streamflow
- **Coverage:** USA only (limitation)
- **For India:** Use India WRIS (https://wris.gov.in/)

#### 🇮🇳 **India-Specific:**
- **IMD (India Meteorological Department):** https://mausam.imd.gov.in/
- **CWC (Central Water Commission):** https://cwc.gov.in/ (reservoir data)
- **CGWB (Central Ground Water Board):** https://cgwb.gov.in/

### ⚡ Implementation Strategy:
```python
# For research paper: Use Open-Meteo (free, no key)
# For production: Combine WeatherAPI + India government APIs
# For groundwater: Manual data collection from government portals
```

---

## 2️⃣ ECONOMY RISK

### Required Features (from model):
```yaml
Core inputs:
  - Inflation (monthly)
  - InterestRate (monthly)
  - ExchangeRate (monthly)
  - NIFTY50 (stock index, monthly)
  - VIX (volatility index, monthly)

Derived features:
  - inflation_lag_1, lag_2
  - interest_lag_1, exchange_lag_1
  - nifty_lag_1, vix_lag_1
  - inflation_roll_3, nifty_roll_3, vix_roll_3
  - inflation_std_3
  - inflation_change, nifty_return, vix_change
  - stagflation_index (Inflation × InterestRate)
  - market_stress (VIX / NIFTY50)
```

### Suggested APIs:

#### ✅ **World Bank API** (FREE - RECOMMENDED FOR RESEARCH)
- **Website:** https://data.worldbank.org/
- **Data Types:** Inflation, GDP, unemployment, indicators
- **Update Frequency:** Monthly/Quarterly
- **Coverage:** Global (190+ countries)
- **Endpoints:**
  ```
  https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?
    date={start_year}:{end_year}&format=json
  
  Example (Inflation for India):
  https://api.worldbank.org/v2/country/IN/indicator/FP.CPI.TOTL.ZG?
    date=2020:2024&format=json
  ```
- **Key Indicators:**
  - `FP.CPI.TOTL.ZG` - Inflation (CPI)
  - `FR.INR.RINR` - Interest rate
  - `PA.NUS.FCRF` - Exchange rate
- **Pros:** 
  - ✅ Free, no API key
  - ✅ Reliable, official data
  - ✅ Historical data (1960+)
- **Cons:** 
  - ❌ Delayed (1-3 months lag)
  - ❌ No stock market data (NIFTY/VIX)

#### ⭐ **FRED (Federal Reserve Economic Data)** (FREE)
- **Website:** https://fred.stlouisfed.org/
- **Data Types:** Economic indicators, interest rates, exchange rates
- **API Key:** Free registration required
- **Endpoints:**
  ```
  https://api.stlouisfed.org/fred/series/observations?
    series_id={SERIES_ID}&api_key={API_KEY}&file_type=json
  
  Example:
  - India Exchange Rate: INDNAIR
  - VIX Index: VIXCLS
  ```
- **Pros:** 
  - ✅ High-quality data
  - ✅ Very comprehensive
  - ✅ Free API key
- **Cons:** 
  - ❌ US-focused (limited international)

#### 📈 **Yahoo Finance API** (FREE - For NIFTY/VIX)
- **Library:** `yfinance` (Python)
- **Data Types:** Stock prices, indices, VIX
- **Update Frequency:** Real-time/Daily
- **Code Example:**
  ```python
  import yfinance as yf
  
  # NIFTY 50
  nifty = yf.download("^NSEI", start="2020-01-01", end="2024-12-31")
  
  # India VIX
  india_vix = yf.download("^INDIAVIX", start="2020-01-01")
  ```
- **Pros:** 
  - ✅ Free, no API key
  - ✅ Real-time data
  - ✅ Easy Python integration
- **Cons:** 
  - ❌ Unofficial (may break)
  - ❌ Rate limits

#### 💳 **Alpha Vantage** (FREEMIUM)
- **Website:** https://www.alphavantage.co/
- **Plan:** Free (25 calls/day), Paid ($49.99/month)
- **Data Types:** Forex, stock indices, economic indicators
- **Endpoints:**
  ```
  https://www.alphavantage.co/query?
    function=FX_DAILY&from_symbol=USD&to_symbol=INR&apikey={API_KEY}
  ```

### ⚡ Implementation Strategy:
```python
# RECOMMENDED STACK FOR RESEARCH:
# 1. World Bank API → Inflation, Interest Rate, GDP
# 2. yfinance → NIFTY50, VIX
# 3. Combine & aggregate to monthly

# Example workflow:
import yfinance as yf
import requests

# Get inflation from World Bank
inflation_data = requests.get(
    "https://api.worldbank.org/v2/country/IN/indicator/FP.CPI.TOTL.ZG?date=2020:2024&format=json"
).json()

# Get NIFTY from Yahoo Finance
nifty = yf.download("^NSEI", start="2020-01-01")
```

---

## 3️⃣ TRADE RISK

### Required Features (from model):
```yaml
Core inputs:
  - Export values (annual/monthly)
  - Import values (annual/monthly)

Derived features:
  - Trade_Balance (Export - Import)
  - Total_Trade (Export + Import)
  - Growth (pct_change of Total_Trade)
  - Rolling_Mean_3
  - Volatility_3 (rolling std)
  - Export_Growth, Import_Growth
  - Export_Share, Import_Share
  - Balance_Ratio
  - Shock (Growth < -30%)
```

### Suggested APIs:

#### ✅ **UN Comtrade API** (FREE - RECOMMENDED FOR RESEARCH)
- **Website:** https://comtradeplus.un.org/
- **Data Types:** Import/Export by country, partner, product
- **Update Frequency:** Monthly (delayed 2-3 months)
- **Coverage:** Global (200+ countries)
- **Endpoints:**
  ```
  https://comtradeapi.un.org/data/v1/get/calls/all
  
  Example:
  https://comtradeapi.un.org/data/v1/get?
    type=C&freq=M&ps=202401&r=356&p=all&rg=1,2&cc=TOTAL
    (r=356 is India, rg=1,2 is exports+imports)
  ```
- **Pros:** 
  - ✅ Official UN data
  - ✅ Very detailed (by product, partner)
  - ✅ Free registration
- **Cons:** 
  - ❌ 2-3 month delay
  - ❌ Complex API structure
  - ❌ Rate limits (100 calls/hour free)

#### ✅ **World Bank WITS** (FREE)
- **Website:** https://wits.worldbank.org/
- **Data Types:** Trade statistics, tariffs
- **Coverage:** Global
- **Pros:** 
  - ✅ Free
  - ✅ Easy to use
  - ✅ Good for research
- **Cons:** 
  - ❌ Annual data only
  - ❌ Limited API (mostly web interface)

#### ⭐ **IMF Direction of Trade** (FREE)
- **Website:** https://data.imf.org/
- **Data Types:** Bilateral trade flows
- **Update Frequency:** Monthly
- **Endpoints:**
  ```
  Access via IMF Data Portal or SDMX API
  ```
- **Pros:** 
  - ✅ Official IMF data
  - ✅ Monthly frequency
- **Cons:** 
  - ❌ Complex API (SDMX format)

#### 💳 **Trading Economics** (FREEMIUM)
- **Website:** https://tradingeconomics.com/
- **Plan:** Free (limited), Paid ($49/month)
- **Data Types:** Trade balance, imports, exports
- **Endpoints:**
  ```
  https://api.tradingeconomics.com/country/
    balance-of-trade?c=india&f=json&api_key={KEY}
  ```

### ⚡ Implementation Strategy:
```python
# For research paper: World Bank WITS (web download → CSV)
# For automated pipeline: UN Comtrade API
# Note: Trade data is typically annual/quarterly, not real-time

# UN Comtrade Python example:
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
```

---

## 4️⃣ GEOPOLITICS RISK

### Required Features (from model):
```yaml
Core inputs:
  - conflict_count (monthly)
  - fatalities_sum (monthly)
  - deaths_total (monthly)
  - conflict_intensity (monthly)
  - policy_uncertainty (monthly)
  - global_uncertainty (monthly)

Derived features:
  - conflict_lag_1, lag_3, lag_6
  - fatalities_lag_1
  - policy_lag_1, uncertainty_lag_1
  - conflict_roll_3, conflict_roll_6
  - fatalities_roll_3
  - conflict_std_3 (volatility)
  - conflict_change, fatalities_change, uncertainty_change
  - conflict_present (event flag)
  - shock (sudden escalation)
  - instability_score (weighted composite)
  - conflict_uncertainty (interaction)
  - fatality_intensity (interaction)
```

### Suggested APIs:

#### ✅ **ACLED (Armed Conflict Location & Event Data)** (FREE FOR RESEARCH - HIGHLY RECOMMENDED)
- **Website:** https://acleddata.com/
- **Data Types:** Conflict events, fatalities, protests, violence
- **Update Frequency:** Real-time (weekly updates)
- **Coverage:** Global (190+ countries)
- **Endpoints:**
  ```
  https://api.acleddata.com/acled/read.json?
    key={API_KEY}&event_type=Battles&country=India&year=2024
  ```
- **Pros:** 
  - ✅ **FREE for academic/research use**
  - ✅ Very detailed event-level data
  - ✅ Real-time updates
  - ✅ Includes fatalities, event types
  - ✅ Perfect match for your features
- **Cons:** 
  - ❌ Requires registration
  - ❌ Commercial use requires paid license
- **⚡ PERFECT MATCH:** This API directly provides conflict_count, fatalities_sum, deaths_total!

#### ✅ **GDELT Project** (COMPLETELY FREE)
- **Website:** https://www.gdeltproject.org/
- **Data Types:** Global events, news sentiment, conflict
- **Update Frequency:** Real-time (15-minute intervals)
- **Coverage:** Global
- **Access:** 
  - Google BigQuery (free tier: 1TB/month)
  - Direct downloads
- **Endpoints:**
  ```sql
  -- BigQuery example
  SELECT EventCode, ActionGeo_CountryCode, NumArticles
  FROM `gdelt-bq.gdeltv2.events`
  WHERE ActionGeo_CountryCode = 'IN'
  ```
- **Pros:** 
  - ✅ Completely free
  - ✅ Massive dataset
  - ✅ Real-time
  - ✅ Includes policy uncertainty signals
- **Cons:** 
  - ❌ Requires BigQuery setup
  - ❌ Complex data structure
  - ❌ Needs significant processing

#### ⭐ **Policy Uncertainty Index** (FREE)
- **Website:** https://www.policyuncertainty.com/
- **Data Types:** Economic policy uncertainty index
- **Coverage:** 20+ countries (including India, USA, China)
- **Update Frequency:** Monthly
- **Access:** Direct CSV downloads
- **Pros:** 
  - ✅ Directly matches your policy_uncertainty feature
  - ✅ Academic research standard
  - ✅ Free
- **Cons:** 
  - ❌ Manual download (no API)
  - ❌ Monthly only

### ⚡ Implementation Strategy:
```python
# RECOMMENDED STACK FOR RESEARCH:
# 1. ACLED API → conflict events, fatalities (FREE for research)
# 2. GDELT Project → global uncertainty, news sentiment
# 3. PolicyUncertainty.com → policy uncertainty index (CSV download)

# ACLED Python example:
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

# Aggregate to monthly counts
# This directly gives you conflict_count, fatalities_sum, etc.
```

---

## 5️⃣ MIGRATION RISK

### Required Features (from model):
```yaml
Core inputs (from World Bank API files):
  - Unemployment rate
  - Net migration rate
  - Population growth
  - Urban population growth

Note: Your pipeline already uses World Bank API files!
```

### Suggested APIs:

#### ✅ **World Bank API** (FREE - ALREADY USING!)
- **Website:** https://data.worldbank.org/
- **Data Types:** Migration, unemployment, population
- **Update Frequency:** Annual
- **Endpoints:**
  ```
  # Net migration
  https://api.worldbank.org/v2/country/IN/indicator/SM.POP.NETM
  
  # Unemployment
  https://api.worldbank.org/v2/country/IN/indicator/SL.UEM.TOTL.ZS
  
  # Population growth
  https://api.worldbank.org/v2/country/IN/indicator/SP.POP.GROW
  ```
- **Pros:** 
  - ✅ Already in your pipeline!
  - ✅ Free, no API key
  - ✅ Reliable
- **Cons:** 
  - ❌ Annual only
  - ❌ 1-2 year delay

#### ⭐ **UN DESA Migration** (FREE)
- **Website:** https://www.un.org/development/desa/pd/data/migrant-stock
- **Data Types:** International migrant stock, flows
- **Update Frequency:** Annual
- **Access:** CSV downloads
- **Pros:** 
  - ✅ Official UN data
  - ✅ Detailed migration flows
- **Cons:** 
  - ❌ No API (manual download)
  - ❌ Annual only

### ⚡ Implementation Strategy:
```python
# Your pipeline already uses World Bank API files!
# Just automate the download process:

import requests
import pandas as pd

def get_world_bank_indicator(country_code, indicator_code, start_year, end_year):
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
    params = {
        'date': f'{start_year}:{end_year}',
        'format': 'json',
        'per_page': 100
    }
    response = requests.get(url, params=params).json()
    
    # Parse response (World Bank returns [metadata, data])
    data = response[1]
    df = pd.DataFrame(data)
    return df

# Example:
migration = get_world_bank_indicator('IN', 'SM.POP.NETM', 2020, 2024)
```

---

## 6️⃣ SOCIAL RISK

### Required Features (from model):
```yaml
Core inputs:
  - protest_count (monthly)
  - violence_count (monthly)
  - conflict_events (monthly)
  - fatalities (monthly)

Derived features:
  - protest_count_scaled (normalized)
  - violence_count_scaled (normalized)
  - protest_lag_1, lag_2, lag_3
  - violence_lag_1
  - protest_roll_3, protest_roll_6
  - violence_roll_3
  - protest_std_3, violence_std_3
  - protest_change, violence_change
```

### Suggested APIs:

#### ✅ **GDELT Project** (FREE - HIGHLY RECOMMENDED)
- **Website:** https://www.gdeltproject.org/
- **Data Types:** Protests, riots, violence, social unrest
- **Update Frequency:** Real-time (15-minute intervals)
- **Coverage:** Global
- **Event Types:**
  - `14` - Demonstrations/Protests
  - `15` - Riots
  - `16` - Violent protests
  - `17` - Clashes
- **Access:** Google BigQuery
- **Pros:** 
  - ✅ Completely free
  - ✅ Real-time
  - ✅ Event-level detail
  - ✅ Perfect for protest_count, violence_count
- **Cons:** 
  - ❌ Requires BigQuery setup
  - ❌ Complex SQL queries

#### ✅ **ACLED** (FREE FOR RESEARCH)
- **Website:** https://acleddata.com/
- **Data Types:** Protests, violence, political events
- **Update Frequency:** Weekly
- **Event Types:**
  - `Protests` - Peaceful protests
  - `Riots` - Violent riots
  - `Strategic developments` - Other social events
- **Endpoints:**
  ```
  https://api.acleddata.com/acled/read.json?
    key={API_KEY}&event_type=Protests&country=India&year=2024
  ```
- **Pros:** 
  - ✅ **FREE for research**
  - ✅ Detailed event data
  - ✅ Includes fatalities
- **Cons:** 
  - ❌ Registration required

#### ⭐ **ICEWS (Integrated Crisis Early Warning System)** (FREE)
- **Website:** https://dataverse.harvard.edu/dataverse/icews
- **Data Types:** Political events, social unrest
- **Coverage:** Global
- **Access:** Harvard Dataverse (CSV downloads)
- **Pros:** 
  - ✅ Free academic data
  - ✅ Event-level detail
- **Cons:** 
  - ❌ No API (manual download)
  - ❌ Complex data structure

### ⚡ Implementation Strategy:
```python
# RECOMMENDED STACK:
# 1. GDELT (BigQuery) → Real-time protest/violence events
# 2. ACLED → Detailed conflict event data (free for research)

# GDELT BigQuery SQL Example:
"""
SELECT
  DATE(ActionGeo_CountryCode) as date,
  COUNT(*) as event_count,
  SUM(NumArticles) as media_coverage
FROM `gdelt-bq.gdeltv2.events`
WHERE
  ActionGeo_CountryCode = 'IN'
  AND EventType IN ('14', '15', '16', '17')  -- Protests, Riots, Violence
GROUP BY date
ORDER BY date
"""

# ACLED Example (same as geopolitics):
import requests

params = {
    'key': 'YOUR_KEY',
    'event_type': 'Protests',
    'country': 'India',
    'year': '2024'
}
response = requests.get('https://api.acleddata.com/acled/read.json', params=params)
```

---

## 7️⃣ INFRASTRUCTURE RISK

### Required Features (from model):
```yaml
Core inputs:
  - water_access (% households with drinking water)
  - urban_population (annual)
  - total_revenue (municipal revenue)
  - municipal_revenue
  - avg_revenue
  - household_metric

Derived features:
  - water_access_lag_1, urban_population_lag_1, total_revenue_lag_1
  - water_access_growth, urban_population_growth, total_revenue_growth
  - water_access_roll_3, urban_population_roll_3
  - infra_pressure (urban_population / water_access)
  - infrastructure_risk (composite score)
```

### Suggested APIs:

#### ✅ **World Bank API** (FREE - RECOMMENDED)
- **Website:** https://data.worldbank.org/
- **Data Types:** Water access, urbanization, infrastructure
- **Update Frequency:** Annual
- **Indicators:**
  - `SH.H2O.BASW.ZS` - Access to drinking water (%)
  - `SP.URB.TOTL.IN.ZS` - Urban population (% of total)
  - `EG.ELC.ACCS.ZS` - Access to electricity (%)
  - `IS.ROD.PAVE.ZS` - Roads paved (% of total)
- **Endpoints:**
  ```
  https://api.worldbank.org/v2/country/IN/indicator/SH.H2O.BASW.ZS?
    date=2020:2024&format=json
  ```
- **Pros:** 
  - ✅ Free, no API key
  - ✅ Global coverage
  - ✅ Historical data
- **Cons:** 
  - ❌ Annual only
  - ❌ 1-2 year delay

#### ⭐ **India-Specific Sources:**
- **Census India:** https://censusindia.gov.in/ (water, housing data)
- **NITI Aayog:** https://www.niti.gov.in/ (infrastructure indices)
- **Ministry of Housing & Urban Affairs:** https://mohua.gov.in/
- **Smart Cities Mission:** https://smartcities.gov.in/

#### 💳 **OECD Regional Statistics** (FREEMIUM)
- **Website:** https://stats.oecd.org/
- **Data Types:** Regional infrastructure, urban development
- **Coverage:** OECD countries (limited for India)

### ⚡ Implementation Strategy:
```python
# For India-specific infrastructure data:
# 1. World Bank API → National level (annual)
# 2. Census India → State level (decennial, but some annual surveys)
# 3. NITI Aayog → Infrastructure indices (annual)

# Note: Infrastructure data is typically ANNUAL, not real-time
# This matches your model's annual frequency

import requests

def get_infrastructure_data(indicator, country='IN', start_year=2020, end_year=2024):
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    params = {
        'date': f'{start_year}:{end_year}',
        'format': 'json',
        'per_page': 100
    }
    response = requests.get(url, params=params).json()
    return pd.DataFrame(response[1])

# Examples:
water = get_infrastructure_data('SH.H2O.BASW.ZS')
urban = get_infrastructure_data('SP.URB.TOTL.IN.ZS')
```

---

## 🎯 RECOMMENDED STACK FOR RESEARCH PAPER

### Tier 1: Completely Free, No API Keys
1. **World Bank API** → Economy, Migration, Infrastructure, Trade
2. **Open-Meteo** → Climate (weather data)
3. **yfinance** → Economy (NIFTY, VIX)
4. **GDELT Project** → Geopolitics, Social (via BigQuery)

### Tier 2: Free for Research (Registration Required)
5. **ACLED** → Geopolitics, Social (conflict events, protests)
6. **Policy Uncertainty Index** → Geopolitics (manual CSV download)

### Tier 3: India-Specific (Government Portals)
7. **IMD** → Climate (India weather)
8. **Census India** → Infrastructure
9. **CWC/CGWB** → Climate (water data)

---

## 📊 DATA FREQUENCY SUMMARY

| Sector | Best Frequency | Typical Delay | Real-time? |
|--------|---------------|---------------|------------|
| Climate | Daily | 1-2 days | ✅ Yes (weather) |
| Economy | Monthly | 1-3 months | ❌ No (NIFTY/VIX are daily) |
| Trade | Monthly/Annual | 2-3 months | ❌ No |
| Geopolitics | Daily/Weekly | 1-7 days | ✅ Yes (ACLED, GDELT) |
| Migration | Annual | 1-2 years | ❌ No |
| Social | Daily | 1-2 days | ✅ Yes (GDELT, ACLED) |
| Infrastructure | Annual | 1-2 years | ❌ No |

---

## 🚀 IMPLEMENTATION ROADMAP

### Phase 1: Quick Start (Week 1-2)
```python
# Start with these 3 APIs (easiest, no keys):
1. World Bank API → Economy, Migration, Infrastructure
2. Open-Meteo → Climate
3. yfinance → Economy (NIFTY, VIX)
```

### Phase 2: Enhanced Data (Week 3-4)
```python
# Add these (registration required):
4. ACLED → Geopolitics, Social
5. UN Comtrade → Trade
```

### Phase 3: Real-time (Week 5-6)
```python
# Add advanced sources:
6. GDELT Project → Geopolitics, Social (real-time)
7. India Government APIs → Climate, Infrastructure
```

---

## 💡 KEY INSIGHTS FOR RESEARCH PAPER

### ✅ Best Choices for Academic Research:
1. **ACLED** - Free for research, gold standard for conflict data
2. **World Bank** - Free, official, widely cited in papers
3. **GDELT** - Free, massive dataset, increasingly popular in research
4. **Open-Meteo** - Free, no attribution required

### ⚠️ Important Notes:
- **Most economic data is DELAYED** (1-3 months) - not real-time
- **Trade/Migration/Infrastructure are ANNUAL** - cannot get monthly
- **Climate/Geopolitics/Social can be REAL-TIME** - daily updates
- **Always cite data sources** in your research paper

### 📝 Data Citation Format:
```
Climate: Open-Meteo (2024), Historical Weather API
Economy: World Bank (2024), World Development Indicators
Trade: UN Comtrade (2024), International Trade Statistics
Geopolitics: ACLED (2024), Armed Conflict Location & Event Data
Migration: World Bank (2024), Migration and Remittances Data
Social: GDELT Project (2024), Global Database of Events
Infrastructure: World Bank (2024), Infrastructure Indicators
```

---

## 📁 NEXT STEPS

1. **Register for API keys:**
   - ACLED: https://acleddata.com/register/
   - GDELT BigQuery: https://cloud.google.com/bigquery (free tier)
   
2. **Start with World Bank + Open-Meteo** (no registration needed)

3. **Create data fetcher scripts** in `pipeline/data_fetch/`

4. **Automate data updates** with scheduled tasks

---

**Last Updated:** 2026-04-06  
**Status:** ✅ Ready for Implementation  
**Confidence Level:** HIGH (based on actual model features)
