"""
🌐 Social Sector API Routes

FastAPI endpoints for serving social risk predictions.

Endpoints:
    GET /social/risk/{state}/{year}/{month}  - Get risk by state + date
    GET /social/latest/{state}               - Get latest risk data
    GET /social/trend/{state}                - Get 12-month trend
    GET /social/high-risk/{state}            - Get all HIGH risk months

Input: data/processed/social/social_risk_output.csv
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
DATA_FILE = os.path.join(BASE_PATH, "data", "processed", "social", "social_risk_output.csv")

# ================================================================
# 🔧 DATA LOADING (ON STARTUP)
# ================================================================

def load_social_data():
    """
    Load social risk data from CSV file.
    Called once on module import for efficiency.
    
    Returns:
        pd.DataFrame: Loaded dataset
        
    Raises:
        FileNotFoundError: If data file doesn't exist
        Exception: If loading fails
    """
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Social data file not found: {DATA_FILE}")
    
    try:
        df = pd.read_csv(DATA_FILE)
        
        # Ensure proper sorting
        df = df.sort_values(['State', 'Year', 'Month']).reset_index(drop=True)
        
        print(f"✅ Social data loaded: {len(df)} rows")
        print(f"   States: {df['State'].nunique()}")
        print(f"   Date range: {df['Year'].min()}/{df['Month'].min()} - {df['Year'].max()}/{df['Month'].max()}")
        
        return df
    
    except Exception as e:
        raise Exception(f"Failed to load social data: {str(e)}")

# Load data once on startup
try:
    social_df = load_social_data()
except Exception as e:
    print(f"⚠️  Warning: {e}")
    social_df = pd.DataFrame()  # Empty DataFrame as fallback

# ================================================================
# 🎯 ROUTER SETUP
# ================================================================

router = APIRouter(
    prefix="/social",
    tags=["Social Risk"],
    responses={404: {"description": "Not found"}},
)

# ================================================================
# 📊 HELPER FUNCTIONS WITH CACHING
# ================================================================

@lru_cache(maxsize=128)
def validate_state_cached(state: str) -> bool:
    """
    Validate that state exists in dataset (CACHED).
    
    Args:
        state: State name
        
    Returns:
        bool: True if state exists
    """
    if social_df.empty:
        return False
    return state in social_df['State'].unique()

def validate_state(state: str) -> bool:
    """Validate that state exists in dataset."""
    return validate_state_cached(state)

@lru_cache(maxsize=64)
def get_state_data_cached(state: str):
    """
    Get filtered data for a specific state (CACHED).
    
    Returns tuple of arrays for cache compatibility.
    
    Args:
        state: State name
        
    Returns:
        tuple: Cached state data as tuple of arrays
    """
    if social_df.empty:
        return None
    
    state_data = social_df[social_df['State'] == state]
    
    if state_data.empty:
        return None
    
    # Convert DataFrame to tuple of tuples for caching
    return tuple(state_data.itertuples(index=False, name=None))

def get_state_data(state: str) -> pd.DataFrame:
    """
    Get filtered data for a specific state.
    Converts cached tuple back to DataFrame.
    
    Args:
        state: State name
        
    Returns:
        pd.DataFrame: Filtered state data
    """
    if social_df.empty:
        return pd.DataFrame()
    
    cached_data = get_state_data_cached(state)
    
    if cached_data is None:
        return pd.DataFrame()
    
    # Convert tuple back to DataFrame
    columns = social_df.columns.tolist()
    return pd.DataFrame(cached_data, columns=columns)

@lru_cache(maxsize=256)
def get_risk_by_date_cached(state: str, year: int, month: int):
    """
    Get risk data for specific state and date (CACHED).
    
    This is the most frequently called query, so we cache it aggressively.
    
    Args:
        state: State name
        year: Year
        month: Month
        
    Returns:
        dict or None: Risk data as dictionary, or None if not found
    """
    state_data = get_state_data(state)
    
    if state_data.empty:
        return None
    
    matching_rows = state_data[
        (state_data['Year'] == year) & 
        (state_data['Month'] == month)
    ]
    
    if matching_rows.empty:
        return None
    
    # Extract data as dictionary (cacheable)
    row = matching_rows.iloc[0]
    return {
        "state": row['State'],
        "year": int(row['Year']),
        "month": int(row['Month']),
        "predicted_risk": float(row['predicted_risk']),
        "actual_risk": float(row['actual_risk'])
    }

@lru_cache(maxsize=64)
def get_latest_risk_cached(state: str):
    """
    Get latest risk data for a state (CACHED).
    
    Args:
        state: State name
        
    Returns:
        dict or None: Latest risk data
    """
    state_data = get_state_data(state)
    
    if state_data.empty:
        return None
    
    # Get latest entry (last row after sorting)
    latest_row = state_data.iloc[-1]
    
    return {
        "state": latest_row['State'],
        "year": int(latest_row['Year']),
        "month": int(latest_row['Month']),
        "predicted_risk": float(latest_row['predicted_risk']),
        "actual_risk": float(latest_row['actual_risk'])
    }

@lru_cache(maxsize=32)
def get_trend_data_cached(state: str, months: int):
    """
    Get trend data for last N months (CACHED).
    
    Args:
        state: State name
        months: Number of months
        
    Returns:
        list or None: List of monthly risk data
    """
    state_data = get_state_data(state)
    
    if state_data.empty:
        return None
    
    # Get last N months
    trend_data = state_data.tail(months)
    
    # Build trend list
    trend_list = []
    for _, row in trend_data.iterrows():
        trend_list.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "predicted_risk": float(row['predicted_risk']),
            "actual_risk": float(row['actual_risk'])
        })
    
    return tuple(trend_list)  # Convert to tuple for caching

@lru_cache(maxsize=64)
def get_high_risk_cached(state: str, threshold: float = 0.5):
    """
    Get all HIGH risk months for a state (CACHED).
    
    Args:
        state: State name
        threshold: Risk score threshold for "HIGH" classification
        
    Returns:
        tuple: Tuple of HIGH risk month dictionaries
    """
    state_data = get_state_data(state)
    
    if state_data.empty:
        return ()
    
    # Filter HIGH risk months (above threshold)
    high_risk_data = state_data[state_data['predicted_risk'] >= threshold].copy()
    
    if high_risk_data.empty:
        return ()
    
    # Build high risk list
    high_risk_list = []
    for _, row in high_risk_data.iterrows():
        high_risk_list.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "predicted_risk": float(row['predicted_risk']),
            "actual_risk": float(row['actual_risk'])
        })
    
    # Sort by date (most recent first)
    high_risk_list.sort(key=lambda x: (x['year'], x['month']), reverse=True)
    
    return tuple(high_risk_list)

@lru_cache(maxsize=64)
def get_state_summary_cached(state: str):
    """
    Get comprehensive summary statistics for a state (CACHED).
    
    Args:
        state: State name
        
    Returns:
        dict: Summary statistics
    """
    state_data = get_state_data(state)
    
    if state_data.empty:
        return None
    
    # Basic statistics
    stats = {
        "total_months": len(state_data),
        "date_range": {
            "start": f"{int(state_data['Year'].min())}/{int(state_data['Month'].min())}",
            "end": f"{int(state_data['Year'].max())}/{int(state_data['Month'].max())}"
        },
        "risk_statistics": {
            "mean_predicted_risk": round(float(state_data['predicted_risk'].mean()), 4),
            "std_predicted_risk": round(float(state_data['predicted_risk'].std()), 4),
            "min_predicted_risk": round(float(state_data['predicted_risk'].min()), 4),
            "max_predicted_risk": round(float(state_data['predicted_risk'].max()), 4)
        },
        "latest_month": {
            "year": int(state_data.iloc[-1]['Year']),
            "month": int(state_data.iloc[-1]['Month']),
            "predicted_risk": round(float(state_data.iloc[-1]['predicted_risk']), 4)
        }
    }
    
    return stats

# ================================================================
# 🔧 ENDPOINT 1: GET RISK BY STATE + DATE
# ================================================================

@router.get("/risk/{state}/{year}/{month}")
async def get_risk_by_date(
    state: str,
    year: int,
    month: int
):
    """
    Get social risk prediction for a specific state and date.
    
    Args:
        state: State name (e.g., 'Bihar')
        year: Year (e.g., 2024)
        month: Month (1-12)
    
    Returns:
        dict: Risk data including predicted_risk and actual_risk
    
    Raises:
        HTTPException: If state not found or date out of range
    """
    # Validate inputs
    if not validate_state(state):
        available_states = social_df['State'].unique().tolist() if not social_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."  # Show first 10
        )
    
    if month < 1 or month > 12:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid month: {month}. Must be between 1 and 12."
        )
    
    # Use CACHED function for instant response! ⚡
    result = get_risk_by_date_cached(state, year, month)
    
    if result is None:
        state_data = get_state_data(state)
        if not state_data.empty:
            min_year = int(state_data['Year'].min())
            max_year = int(state_data['Year'].max())
            raise HTTPException(
                status_code=404,
                detail=f"No data for {state} in {year}/{month}. Available range: {min_year}-{max_year}"
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No data available for state '{state}'"
            )
    
    # Add timestamp
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result

# ================================================================
# 🔧 ENDPOINT 2: GET LATEST RISK
# ================================================================

@router.get("/latest/{state}")
async def get_latest_risk(state: str):
    """
    Get the most recent social risk data for a state.
    
    Args:
        state: State name (e.g., 'Bihar')
    
    Returns:
        dict: Latest risk data
    
    Raises:
        HTTPException: If state not found or no data available
    """
    if not validate_state(state):
        available_states = social_df['State'].unique().tolist() if not social_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    # Use CACHED function for instant response! ⚡
    result = get_latest_risk_cached(state)
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for state '{state}'"
        )
    
    # Add metadata
    result["is_latest"] = True
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result

# ================================================================
# 🔧 ENDPOINT 3: GET TREND (LAST 12 MONTHS)
# ================================================================

@router.get("/trend/{state}")
async def get_risk_trend(state: str, months: Optional[int] = 12):
    """
    Get social risk trend for the last N months.
    
    Args:
        state: State name (e.g., 'Bihar')
        months: Number of months to retrieve (default: 12)
    
    Returns:
        dict: Trend data with list of monthly records
    
    Raises:
        HTTPException: If state not found
    """
    if not validate_state(state):
        available_states = social_df['State'].unique().tolist() if not social_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    if months < 1 or months > 120:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid months parameter: {months}. Must be between 1 and 120."
        )
    
    # Use CACHED function for instant response! ⚡
    cached_trend = get_trend_data_cached(state, months)
    
    if cached_trend is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for state '{state}'"
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
        "state": state,
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

@router.get("/high-risk/{state}")
async def get_high_risk_months(state: str, threshold: Optional[float] = 0.5):
    """
    Get all months classified as HIGH risk for a state.
    
    Args:
        state: State name (e.g., 'Bihar')
        threshold: Risk score threshold for HIGH classification (default: 0.5)
    
    Returns:
        dict: List of HIGH risk months with details
    
    Raises:
        HTTPException: If state not found
    """
    if not validate_state(state):
        available_states = social_df['State'].unique().tolist() if not social_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    if threshold < 0 or threshold > 1:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid threshold: {threshold}. Must be between 0 and 1."
        )
    
    # Use CACHED function for instant response! ⚡
    cached_high_risk = get_high_risk_cached(state, threshold)
    
    if len(cached_high_risk) == 0:
        state_data = get_state_data(state)
        total_months = len(state_data) if not state_data.empty else 0
        
        return {
            "state": state,
            "threshold": threshold,
            "total_high_risk_months": 0,
            "percentage_high_risk": 0.0,
            "message": f"No HIGH risk months found (threshold >= {threshold})",
            "data": [],
            "timestamp": datetime.utcnow().isoformat()
        }
    
    # Convert tuple back to list
    high_risk_list = list(cached_high_risk)
    
    # Calculate percentage
    state_data = get_state_data(state)
    total_months = len(state_data) if not state_data.empty else 1
    percentage = round(len(high_risk_list) / total_months * 100, 2)
    
    response = {
        "state": state,
        "threshold": threshold,
        "total_high_risk_months": len(high_risk_list),
        "percentage_high_risk": percentage,
        "data": high_risk_list,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return response

# ================================================================
# 🔧 BONUS ENDPOINT: GET ALL STATES
# ================================================================

@router.get("/states")
async def get_available_states():
    """
    Get list of all available states in the dataset.
    
    Returns:
        dict: List of state names
    """
    if social_df.empty:
        return {
            "states": [],
            "total": 0,
            "message": "No data available"
        }
    
    states = social_df['State'].unique().tolist()
    
    return {
        "states": states,
        "total": len(states),
        "timestamp": datetime.utcnow().isoformat()
    }

# ================================================================
# 🔧 BONUS ENDPOINT: GET SUMMARY STATISTICS
# ================================================================

@router.get("/summary/{state}")
async def get_state_summary(state: str):
    """
    Get comprehensive summary statistics for a state.
    
    Args:
        state: State name (e.g., 'Bihar')
    
    Returns:
        dict: Summary statistics including risk distribution
    """
    if not validate_state(state):
        available_states = social_df['State'].unique().tolist() if not social_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    # Use CACHED function for instant response! ⚡
    cached_summary = get_state_summary_cached(state)
    
    if cached_summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for state '{state}'"
        )
    
    return {
        "state": state,
        "summary": cached_summary,
        "timestamp": datetime.utcnow().isoformat()
    }
