"""
🌐 Economy Sector API Routes

FastAPI endpoints for serving economic risk predictions.

Endpoints:
    GET /economy/risk/{country}/{year}/{month}  - Get risk by country + date
    GET /economy/latest/{country}               - Get latest risk data
    GET /economy/trend/{country}                - Get 12-month trend
    GET /economy/high-risk/{country}            - Get all HIGH risk months

Input: data/processed/economy/economic_risk_final.csv
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
import pandas as pd
import os
from datetime import datetime
from functools import lru_cache

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_FILE = os.path.join(BASE_PATH, "data", "processed", "economy", "economic_risk_final.csv")

# ================================================================
# 🔧 DATA LOADING (ON STARTUP)
# ================================================================

def load_economy_data():
    """
    Load economy risk data from CSV file.
    Called once on module import for efficiency.
    
    Returns:
        pd.DataFrame: Loaded dataset
        
    Raises:
        FileNotFoundError: If data file doesn't exist
        Exception: If loading fails
    """
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Economy data file not found: {DATA_FILE}")
    
    try:
        df = pd.read_csv(DATA_FILE)
        
        # Ensure proper sorting
        df = df.sort_values(['Country', 'Year', 'Month']).reset_index(drop=True)
        
        print(f"✅ Economy data loaded: {len(df)} rows")
        print(f"   Countries: {df['Country'].unique()}")
        print(f"   Date range: {df['Year'].min()}/{df['Month'].min()} - {df['Year'].max()}/{df['Month'].max()}")
        
        return df
    
    except Exception as e:
        raise Exception(f"Failed to load economy data: {str(e)}")

# Load data once on startup
try:
    economy_df = load_economy_data()
except Exception as e:
    print(f"⚠️  Warning: {e}")
    economy_df = pd.DataFrame()  # Empty DataFrame as fallback

# ================================================================
# 🎯 ROUTER SETUP
# ================================================================

router = APIRouter(
    prefix="/economy",
    tags=["Economy Risk"],
    responses={404: {"description": "Not found"}},
)

# ================================================================
# 📊 HELPER FUNCTIONS WITH CACHING
# ================================================================

@lru_cache(maxsize=128)
def validate_country_cached(country: str) -> bool:
    """
    Validate that country exists in dataset (CACHED).
    
    Args:
        country: Country code (uppercase)
        
    Returns:
        bool: True if country exists
    """
    if economy_df.empty:
        return False
    return country in economy_df['Country'].unique()

def validate_country(country: str) -> bool:
    """Validate that country exists in dataset."""
    return validate_country_cached(country.upper())

@lru_cache(maxsize=64)
def get_country_data_cached(country: str):
    """
    Get filtered data for a specific country (CACHED).
    
    Returns tuple of arrays for cache compatibility.
    
    Args:
        country: Country code (uppercase)
        
    Returns:
        tuple: Cached country data as tuple of arrays
    """
    if economy_df.empty:
        return None
    
    country_data = economy_df[economy_df['Country'] == country]
    
    if country_data.empty:
        return None
    
    # Convert DataFrame to tuple of tuples for caching
    # Each row becomes a tuple, entire result is tuple of tuples
    return tuple(country_data.itertuples(index=False, name=None))

def get_country_data(country: str) -> pd.DataFrame:
    """
    Get filtered data for a specific country.
    Converts cached tuple back to DataFrame.
    
    Args:
        country: Country code
        
    Returns:
        pd.DataFrame: Filtered country data
    """
    if economy_df.empty:
        return pd.DataFrame()
    
    country_upper = country.upper()
    cached_data = get_country_data_cached(country_upper)
    
    if cached_data is None:
        return pd.DataFrame()
    
    # Convert tuple back to DataFrame
    columns = economy_df.columns.tolist()
    return pd.DataFrame(cached_data, columns=columns)

@lru_cache(maxsize=256)
def get_risk_by_date_cached(country: str, year: int, month: int):
    """
    Get risk data for specific country and date (CACHED).
    
    This is the most frequently called query, so we cache it aggressively.
    
    Args:
        country: Country code (uppercase)
        year: Year
        month: Month
        
    Returns:
        dict or None: Risk data as dictionary, or None if not found
    """
    country_data = get_country_data(country)
    
    if country_data.empty:
        return None
    
    matching_rows = country_data[
        (country_data['Year'] == year) & 
        (country_data['Month'] == month)
    ]
    
    if matching_rows.empty:
        return None
    
    # Extract data as dictionary (cacheable)
    row = matching_rows.iloc[0]
    return {
        "country": row['Country'],
        "year": int(row['Year']),
        "month": int(row['Month']),
        "predicted_risk": float(row['predicted_risk']),
        "smoothed_risk": float(row['smoothed_risk']),
        "risk_level": row['risk_level'],
        "risk_direction": row['risk_direction']
    }

@lru_cache(maxsize=64)
def get_latest_risk_cached(country: str):
    """
    Get latest risk data for a country (CACHED).
    
    Args:
        country: Country code (uppercase)
        
    Returns:
        dict or None: Latest risk data
    """
    country_data = get_country_data(country)
    
    if country_data.empty:
        return None
    
    # Get latest entry (last row after sorting)
    latest_row = country_data.iloc[-1]
    
    return {
        "country": latest_row['Country'],
        "year": int(latest_row['Year']),
        "month": int(latest_row['Month']),
        "predicted_risk": float(latest_row['predicted_risk']),
        "smoothed_risk": float(latest_row['smoothed_risk']),
        "risk_level": latest_row['risk_level'],
        "risk_direction": latest_row['risk_direction']
    }

@lru_cache(maxsize=32)
def get_trend_data_cached(country: str, months: int):
    """
    Get trend data for last N months (CACHED).
    
    Args:
        country: Country code (uppercase)
        months: Number of months
        
    Returns:
        list or None: List of monthly risk data
    """
    country_data = get_country_data(country)
    
    if country_data.empty:
        return None
    
    # Get last N months
    trend_data = country_data.tail(months)
    
    # Build trend list
    trend_list = []
    for _, row in trend_data.iterrows():
        trend_list.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "predicted_risk": float(row['predicted_risk']),
            "smoothed_risk": float(row['smoothed_risk']),
            "risk_level": row['risk_level'],
            "risk_direction": row['risk_direction']
        })
    
    return tuple(trend_list)  # Convert to tuple for caching

@lru_cache(maxsize=64)
def get_high_risk_cached(country: str):
    """
    Get all HIGH risk months for a country (CACHED).
    
    Args:
        country: Country code (uppercase)
        
    Returns:
        tuple: Tuple of HIGH risk month dictionaries
    """
    country_data = get_country_data(country)
    
    if country_data.empty:
        return ()
    
    # Filter HIGH risk months
    high_risk_data = country_data[country_data['risk_level'] == 'HIGH'].copy()
    
    if high_risk_data.empty:
        return ()
    
    # Build high risk list
    high_risk_list = []
    for _, row in high_risk_data.iterrows():
        high_risk_list.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "predicted_risk": float(row['predicted_risk']),
            "smoothed_risk": float(row['smoothed_risk']),
            "risk_direction": row['risk_direction']
        })
    
    # Sort by date (most recent first)
    high_risk_list.sort(key=lambda x: (x['year'], x['month']), reverse=True)
    
    return tuple(high_risk_list)

@lru_cache(maxsize=64)
def get_country_summary_cached(country: str):
    """
    Get comprehensive summary statistics for a country (CACHED).
    
    Args:
        country: Country code (uppercase)
        
    Returns:
        dict: Summary statistics
    """
    country_data = get_country_data(country)
    
    if country_data.empty:
        return None
    
    # Risk level distribution
    risk_dist = country_data['risk_level'].value_counts().to_dict()
    
    # Risk direction distribution
    direction_dist = country_data['risk_direction'].value_counts().to_dict()
    
    # Basic statistics
    stats = {
        "total_months": len(country_data),
        "date_range": {
            "start": f"{int(country_data['Year'].min())}/{int(country_data['Month'].min())}",
            "end": f"{int(country_data['Year'].max())}/{int(country_data['Month'].max())}"
        },
        "risk_statistics": {
            "mean_predicted_risk": round(float(country_data['predicted_risk'].mean()), 4),
            "std_predicted_risk": round(float(country_data['predicted_risk'].std()), 4),
            "min_predicted_risk": round(float(country_data['predicted_risk'].min()), 4),
            "max_predicted_risk": round(float(country_data['predicted_risk'].max()), 4)
        },
        "risk_level_distribution": {k: int(v) for k, v in risk_dist.items()},
        "risk_direction_distribution": {k: int(v) for k, v in direction_dist.items()},
        "latest_month": {
            "year": int(country_data.iloc[-1]['Year']),
            "month": int(country_data.iloc[-1]['Month']),
            "risk_level": country_data.iloc[-1]['risk_level'],
            "risk_direction": country_data.iloc[-1]['risk_direction']
        }
    }
    
    return stats

# ================================================================
# 🔧 ENDPOINT 1: GET RISK BY COUNTRY + DATE
# ================================================================

@router.get("/risk/{country}/{year}/{month}")
async def get_risk_by_date(
    country: str,
    year: int,
    month: int
):
    """
    Get economic risk prediction for a specific country and date.
    
    Args:
        country: Country code (e.g., 'IND')
        year: Year (e.g., 2024)
        month: Month (1-12)
    
    Returns:
        dict: Risk data including predicted_risk, smoothed_risk, risk_level, risk_direction
    
    Raises:
        HTTPException: If country not found or date out of range
    """
    # Validate inputs
    if not validate_country(country):
        available_countries = economy_df['Country'].unique().tolist() if not economy_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"Country '{country}' not found. Available countries: {available_countries}"
        )
    
    if month < 1 or month > 12:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month: {month}. Must be between 1 and 12."
        )
    
    # Use CACHED function for instant response! ⚡
    result = get_risk_by_date_cached(country.upper(), year, month)
    
    if result is None:
        country_data = get_country_data(country)
        if not country_data.empty:
            min_year = int(country_data['Year'].min())
            max_year = int(country_data['Year'].max())
            raise HTTPException(
                status_code=404,
                detail=f"No data for {country} in {year}/{month}. Available range: {min_year}-{max_year}"
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No data available for country '{country}'"
            )
    
    # Add timestamp
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result

# ================================================================
# 🔧 ENDPOINT 2: GET LATEST RISK
# ================================================================

@router.get("/latest/{country}")
async def get_latest_risk(country: str):
    """
    Get the most recent economic risk data for a country.
    
    Args:
        country: Country code (e.g., 'IND')
    
    Returns:
        dict: Latest risk data
    
    Raises:
        HTTPException: If country not found or no data available
    """
    if not validate_country(country):
        available_countries = economy_df['Country'].unique().tolist() if not economy_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"Country '{country}' not found. Available countries: {available_countries}"
        )
    
    # Use CACHED function for instant response! ⚡
    result = get_latest_risk_cached(country.upper())
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for country '{country}'"
        )
    
    # Add metadata
    result["is_latest"] = True
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result

# ================================================================
# 🔧 ENDPOINT 3: GET TREND (LAST 12 MONTHS)
# ================================================================

@router.get("/trend/{country}")
async def get_risk_trend(country: str, months: Optional[int] = 12):
    """
    Get economic risk trend for the last N months.
    
    Args:
        country: Country code (e.g., 'IND')
        months: Number of months to retrieve (default: 12)
    
    Returns:
        dict: Trend data with list of monthly records
    
    Raises:
        HTTPException: If country not found
    """
    if not validate_country(country):
        available_countries = economy_df['Country'].unique().tolist() if not economy_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"Country '{country}' not found. Available countries: {available_countries}"
        )
    
    if months < 1 or months > 120:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid months parameter: {months}. Must be between 1 and 120."
        )
    
    # Use CACHED function for instant response! ⚡
    cached_trend = get_trend_data_cached(country.upper(), months)
    
    if cached_trend is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for country '{country}'"
        )
    
    # Convert tuple back to list
    trend_list = list(cached_trend)
    
    # Calculate summary statistics from cached data
    if trend_list:
        risks = [item['predicted_risk'] for item in trend_list]
        avg_risk = sum(risks) / len(risks)
        min_risk = min(risks)
        max_risk = max(risks)
        
        # Determine overall trend direction
        if len(trend_list) >= 2:
            first_risk = trend_list[0]['predicted_risk']
            last_risk = trend_list[-1]['predicted_risk']
            if last_risk > first_risk:
                overall_trend = "INCREASING"
            elif last_risk < first_risk:
                overall_trend = "DECREASING"
            else:
                overall_trend = "STABLE"
        else:
            overall_trend = "INSUFFICIENT_DATA"
    else:
        avg_risk = 0
        min_risk = 0
        max_risk = 0
        overall_trend = "NO_DATA"
    
    response = {
        "country": country.upper(),
        "period_months": len(trend_list),
        "overall_trend": overall_trend,
        "statistics": {
            "average_risk": round(avg_risk, 4),
            "min_risk": round(min_risk, 4),
            "max_risk": round(max_risk, 4)
        },
        "data": trend_list,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return response

# ================================================================
# 🔧 ENDPOINT 4: GET HIGH RISK MONTHS
# ================================================================

@router.get("/high-risk/{country}")
async def get_high_risk_months(country: str):
    """
    Get all months classified as HIGH risk for a country.
    
    Args:
        country: Country code (e.g., 'IND')
    
    Returns:
        dict: List of HIGH risk months with details
    
    Raises:
        HTTPException: If country not found
    """
    if not validate_country(country):
        available_countries = economy_df['Country'].unique().tolist() if not economy_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"Country '{country}' not found. Available countries: {available_countries}"
        )
    
    # Use CACHED function for instant response! ⚡
    cached_high_risk = get_high_risk_cached(country.upper())
    
    if len(cached_high_risk) == 0:
        country_data = get_country_data(country)
        total_months = len(country_data) if not country_data.empty else 0
        
        return {
            "country": country.upper(),
            "total_high_risk_months": 0,
            "percentage_high_risk": 0.0,
            "message": "No HIGH risk months found for this country",
            "data": [],
            "timestamp": datetime.utcnow().isoformat()
        }
    
    # Convert tuple back to list
    high_risk_list = list(cached_high_risk)
    
    # Calculate percentage
    country_data = get_country_data(country)
    total_months = len(country_data) if not country_data.empty else 1
    percentage = round(len(high_risk_list) / total_months * 100, 2)
    
    response = {
        "country": country.upper(),
        "total_high_risk_months": len(high_risk_list),
        "percentage_high_risk": percentage,
        "data": high_risk_list,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return response

# ================================================================
# 🔧 BONUS ENDPOINT: GET ALL COUNTRIES
# ================================================================

@router.get("/countries")
async def get_available_countries():
    """
    Get list of all available countries in the dataset.
    
    Returns:
        dict: List of country codes
    """
    if economy_df.empty:
        return {
            "countries": [],
            "total": 0,
            "message": "No data available"
        }
    
    countries = economy_df['Country'].unique().tolist()
    
    return {
        "countries": countries,
        "total": len(countries),
        "timestamp": datetime.utcnow().isoformat()
    }

# ================================================================
# 🔧 BONUS ENDPOINT: GET SUMMARY STATISTICS
# ================================================================

@router.get("/summary/{country}")
async def get_country_summary(country: str):
    """
    Get comprehensive summary statistics for a country.
    
    Args:
        country: Country code (e.g., 'IND')
    
    Returns:
        dict: Summary statistics including risk distribution
    """
    if not validate_country(country):
        available_countries = economy_df['Country'].unique().tolist() if not economy_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"Country '{country}' not found. Available countries: {available_countries}"
        )
    
    # Use CACHED function for instant response! ⚡
    cached_summary = get_country_summary_cached(country.upper())
    
    if cached_summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for country '{country}'"
        )
    
    return {
        "country": country.upper(),
        "summary": cached_summary,
        "timestamp": datetime.utcnow().isoformat()
    }
