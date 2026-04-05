"""
🌐 Infrastructure Sector API Routes

Endpoints:
    GET /infrastructure-risk/state/{state}/{year}  - Get risk by state + year
    GET /infrastructure-risk/latest/{state}        - Get latest risk data
    GET /infrastructure-risk/trend/{state}         - Get risk trend over time
    GET /infrastructure-risk/top-states            - Get top 10 risky states
    GET /infrastructure-risk/states                - Get all available states
"""

from fastapi import APIRouter, HTTPException
import pandas as pd
import joblib
import os
from datetime import datetime
from functools import lru_cache

# ================================================================
# 📂 PATH CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_FILE = os.path.join(BASE_PATH, "data", "processed", "infrastructure", "infrastructure_risk_output.csv")
MODEL_FILE = os.path.join(BASE_PATH, "models", "trained", "infrastructure_model.pkl")
SCALER_FILE = os.path.join(BASE_PATH, "models", "trained", "infrastructure_scaler.pkl")

print("="*70)
print("🌐 Loading Infrastructure Risk API Data")
print("="*70)

# ================================================================
# 💾 LOAD DATA AND MODEL
# ================================================================

def load_infrastructure_data():
    """Load infrastructure risk predictions."""
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Infrastructure data file not found: {DATA_FILE}")
    
    df = pd.read_csv(DATA_FILE)
    df = df.sort_values(['state', 'year']).reset_index(drop=True)
    
    print(f"✅ Infrastructure data loaded: {len(df)} rows")
    print(f"   States: {df['state'].nunique()}")
    print(f"   Year range: {df['year'].min()} - {df['year'].max()}")
    
    return df


def load_model_and_scaler():
    """Load trained model and scaler."""
    if not os.path.exists(MODEL_FILE):
        raise FileNotFoundError(f"Model file not found: {MODEL_FILE}")
    
    if not os.path.exists(SCALER_FILE):
        raise FileNotFoundError(f"Scaler file not found: {SCALER_FILE}")
    
    model = joblib.load(MODEL_FILE)
    scaler = joblib.load(SCALER_FILE)
    
    print(f"✅ Model loaded: {MODEL_FILE}")
    print(f"✅ Scaler loaded: {SCALER_FILE}")
    
    return model, scaler


# Load on startup
try:
    infrastructure_df = load_infrastructure_data()
    infrastructure_model, infrastructure_scaler = load_model_and_scaler()
except Exception as e:
    print(f"⚠️  Warning: Could not load infrastructure data/model: {e}")
    infrastructure_df = pd.DataFrame()
    infrastructure_model = None
    infrastructure_scaler = None

# ================================================================
# 🔧 HELPER FUNCTIONS
# ================================================================

@lru_cache(maxsize=128)
def validate_state_cached(state: str) -> bool:
    """Check if state exists in dataset (CACHED)."""
    if infrastructure_df.empty:
        return False
    return state in infrastructure_df['state'].values


def get_state_data(state: str) -> pd.DataFrame:
    """Get all data for a specific state."""
    if infrastructure_df.empty:
        return pd.DataFrame()
    return infrastructure_df[infrastructure_df['state'] == state]


@lru_cache(maxsize=256)
def get_risk_by_year_cached(state: str, year: int):
    """Get risk data for specific state and year (CACHED)."""
    state_data = get_state_data(state)
    if state_data.empty:
        return None
    
    matching_rows = state_data[state_data['year'] == year]
    
    if matching_rows.empty:
        return None
    
    row = matching_rows.iloc[0]
    return {
        "state": row['state'],
        "year": int(row['year']),
        "predicted_risk": float(row['predicted_risk']),
        "actual_risk": float(row['infrastructure_risk'])
    }


# ================================================================
# 🎯 API ROUTER
# ================================================================

router = APIRouter(
    prefix="/infrastructure-risk",
    tags=["Infrastructure Risk"],
    responses={404: {"description": "Not found"}},
)


# ----------------------------------------------------------
# Endpoint 1: Get risk by state + year
# ----------------------------------------------------------
@router.get("/state/{state}/{year}")
async def get_risk_by_state_and_year(state: str, year: int):
    """
    Get infrastructure risk prediction for a specific state and year.
    
    Args:
        state: State name (e.g., "Maharashtra")
        year: Year (e.g., 2020)
    
    Returns:
        dict: Risk prediction with metadata
    """
    # Validate state
    if not validate_state_cached(state):
        available_states = infrastructure_df['state'].unique().tolist() if not infrastructure_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    # Validate year range
    state_data = get_state_data(state)
    if not state_data.empty:
        min_year = int(state_data['year'].min())
        max_year = int(state_data['year'].max())
        
        if year < min_year or year > max_year:
            raise HTTPException(
                status_code=400,
                detail=f"Year {year} out of range for {state}. Available: {min_year}-{max_year}"
            )
    
    # Get cached result
    result = get_risk_by_year_cached(state, year)
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data for {state} in year {year}"
        )
    
    # Add timestamp
    result["timestamp"] = datetime.utcnow().isoformat()
    
    return result


# ----------------------------------------------------------
# Endpoint 2: Latest risk for state
# ----------------------------------------------------------
@router.get("/latest/{state}")
async def get_latest_risk(state: str):
    """
    Get the most recent infrastructure risk prediction for a state.
    
    Args:
        state: State name
    
    Returns:
        dict: Latest risk prediction
    """
    # Validate state
    if not validate_state_cached(state):
        available_states = infrastructure_df['state'].unique().tolist() if not infrastructure_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    # Get state data
    state_data = get_state_data(state)
    
    if state_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for state: {state}"
        )
    
    # Get latest year
    latest = state_data.sort_values('year', ascending=False).iloc[0]
    
    result = {
        "state": latest['state'],
        "year": int(latest['year']),
        "predicted_risk": float(latest['predicted_risk']),
        "actual_risk": float(latest['infrastructure_risk']),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return result


# ----------------------------------------------------------
# Endpoint 3: Trend for state
# ----------------------------------------------------------
@router.get("/trend/{state}")
async def get_risk_trend(state: str):
    """
    Get infrastructure risk trend over time for a state.
    
    Args:
        state: State name
    
    Returns:
        list: Time series of risk predictions
    """
    # Validate state
    if not validate_state_cached(state):
        available_states = infrastructure_df['state'].unique().tolist() if not infrastructure_df.empty else []
        raise HTTPException(
            status_code=404,
            detail=f"State '{state}' not found. Available states: {available_states[:10]}..."
        )
    
    # Get state data sorted by year
    state_data = get_state_data(state)
    
    if state_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for state: {state}"
        )
    
    state_data = state_data.sort_values('year')
    
    # Build trend data
    trend_data = []
    for _, row in state_data.iterrows():
        trend_data.append({
            "year": int(row['year']),
            "predicted_risk": float(row['predicted_risk']),
            "actual_risk": float(row['infrastructure_risk'])
        })
    
    return {
        "state": state,
        "trend": trend_data,
        "data_points": len(trend_data),
        "year_range": f"{int(state_data['year'].min())}-{int(state_data['year'].max())}"
    }


# ----------------------------------------------------------
# Endpoint 4: Top risky states
# ----------------------------------------------------------
@router.get("/top-states")
async def get_top_risky_states(limit: int = 10):
    """
    Get the top N states with highest infrastructure risk.
    
    Args:
        limit: Number of top states to return (default: 10)
    
    Returns:
        list: Top risky states with their risk scores
    """
    if infrastructure_df.empty:
        raise HTTPException(
            status_code=500,
            detail="Infrastructure data not loaded"
        )
    
    # Get latest year for each state
    latest = infrastructure_df.sort_values('year').groupby('state').tail(1)
    
    # Sort by risk (descending) and take top N
    top = latest.sort_values('predicted_risk', ascending=False).head(limit)
    
    result = []
    for _, row in top.iterrows():
        result.append({
            "rank": len(result) + 1,
            "state": row['state'],
            "year": int(row['year']),
            "predicted_risk": float(row['predicted_risk']),
            "risk_level": "HIGH" if row['predicted_risk'] > 0.6 else 
                         "MEDIUM" if row['predicted_risk'] > 0.3 else "LOW"
        })
    
    return {
        "top_states": result,
        "total_states": len(infrastructure_df['state'].unique()),
        "timestamp": datetime.utcnow().isoformat()
    }


# ----------------------------------------------------------
# Endpoint 5: All states list
# ----------------------------------------------------------
@router.get("/states")
async def get_available_states():
    """
    Get list of all available states in the dataset.
    
    Returns:
        list: Sorted list of state names
    """
    if infrastructure_df.empty:
        raise HTTPException(
            status_code=500,
            detail="Infrastructure data not loaded"
        )
    
    states = sorted(infrastructure_df['state'].unique().tolist())
    
    return {
        "states": states,
        "count": len(states),
        "timestamp": datetime.utcnow().isoformat()
    }


# ----------------------------------------------------------
# Endpoint 6: Summary statistics
# ----------------------------------------------------------
@router.get("/summary")
async def get_summary():
    """
    Get summary statistics for infrastructure risk.
    
    Returns:
        dict: Overall statistics and insights
    """
    if infrastructure_df.empty:
        raise HTTPException(
            status_code=500,
            detail="Infrastructure data not loaded"
        )
    
    # Overall statistics
    overall_stats = {
        "mean_risk": float(infrastructure_df['predicted_risk'].mean()),
        "std_risk": float(infrastructure_df['predicted_risk'].std()),
        "min_risk": float(infrastructure_df['predicted_risk'].min()),
        "max_risk": float(infrastructure_df['predicted_risk'].max()),
        "median_risk": float(infrastructure_df['predicted_risk'].median())
    }
    
    # Get latest year
    latest_year = int(infrastructure_df['year'].max())
    latest_data = infrastructure_df[infrastructure_df['year'] == latest_year]
    
    # High risk states count
    high_risk_count = len(latest_data[latest_data['predicted_risk'] > 0.6])
    medium_risk_count = len(latest_data[(latest_data['predicted_risk'] > 0.3) & 
                                        (latest_data['predicted_risk'] <= 0.6)])
    low_risk_count = len(latest_data[latest_data['predicted_risk'] <= 0.3])
    
    return {
        "dataset_info": {
            "total_records": len(infrastructure_df),
            "states": infrastructure_df['state'].nunique(),
            "year_range": f"{int(infrastructure_df['year'].min())}-{int(infrastructure_df['year'].max())}",
            "latest_year": latest_year
        },
        "overall_statistics": overall_stats,
        "risk_distribution_latest_year": {
            "high_risk_states": high_risk_count,
            "medium_risk_states": medium_risk_count,
            "low_risk_states": low_risk_count
        },
        "timestamp": datetime.utcnow().isoformat()
    }
