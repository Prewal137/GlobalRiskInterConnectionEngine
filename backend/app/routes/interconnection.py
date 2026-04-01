"""
Global Risk Interconnection API Routes
Serves unified multi-sector risk data combining climate and trade risks.
"""

from fastapi import APIRouter, HTTPException, Query
import pandas as pd
import os
from typing import Optional

# Create router
router = APIRouter(prefix="/global-risk", tags=["global-risk"])

# File paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
GLOBAL_RISK_FILE = os.path.join(BASE_DIR, "data/processed/interconnection/global_risk.csv")

# Cache for data (loaded once at startup)
_cached_data: Optional[pd.DataFrame] = None


def load_data() -> pd.DataFrame:
    """
    Load global risk data from CSV with caching.
    
    Returns:
        pd.DataFrame: Global risk dataset
    """
    global _cached_data
    
    if _cached_data is not None:
        return _cached_data
    
    if not os.path.exists(GLOBAL_RISK_FILE):
        raise FileNotFoundError(f"Global risk data file not found: {GLOBAL_RISK_FILE}")
    
    _cached_data = pd.read_csv(GLOBAL_RISK_FILE)
    return _cached_data


def round_floats(value):
    """Round float values to 4 decimal places."""
    if isinstance(value, float):
        return round(value, 4)
    return value


@router.get("/summary")
async def get_global_risk_summary():
    """
    Get summary statistics of global multi-sector risk data.
    
    Returns:
        Summary metrics including averages and highest/lowest risk states
    """
    try:
        # Load data
        df = load_data()
        
        # Calculate summary statistics
        total_states = len(df)
        average_climate_risk = float(df['climate_risk'].mean())
        average_trade_risk = float(df['Trade_Risk'].mean())
        average_final_risk = float(df['final_risk'].mean())
        
        # Find highest and lowest risk states
        idx_max = df['final_risk'].idxmax()
        idx_min = df['final_risk'].idxmin()
        
        highest_risk_state = df.loc[idx_max]
        lowest_risk_state = df.loc[idx_min]
        
        return {
            "success": True,
            "summary": {
                "total_states": total_states,
                "average_climate_risk": round(average_climate_risk, 4),
                "average_trade_risk": round(average_trade_risk, 4),
                "average_final_risk": round(average_final_risk, 4),
                "highest_risk": {
                    "state": highest_risk_state['Country'],
                    "final_risk": round(float(highest_risk_state['final_risk']), 4),
                    "risk_level": highest_risk_state['risk_level']
                },
                "lowest_risk": {
                    "state": lowest_risk_state['Country'],
                    "final_risk": round(float(lowest_risk_state['final_risk']), 4),
                    "risk_level": lowest_risk_state['risk_level']
                }
            }
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/top")
async def get_top_risk_states():
    """
    Get top 10 states with highest final risk scores.
    
    Returns:
        List of top 10 high-risk states sorted by final_risk descending
    """
    try:
        # Load data
        df = load_data()
        
        # Sort by final_risk descending and take top 10
        top_10 = df.nlargest(10, 'final_risk')
        
        # Convert to records and round floats
        records = top_10.to_dict('records')
        records = [{k: round_floats(v) for k, v in record.items()} for record in records]
        
        return {
            "success": True,
            "total_states": len(records),
            "data": records
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/state/{state}")
async def get_state_risk(state: str):
    """
    Get risk data for a specific state/country.
    
    Args:
        state: State/Country name (case-insensitive)
        
    Returns:
        State/Country risk data including climate, trade, and final risk scores
    """
    try:
        # Load data
        df = load_data()
        
        # Filter by state/country (case-insensitive)
        state_data = df[df['Country'].str.lower() == state.lower()]
        
        # Handle missing state
        if len(state_data) == 0:
            raise HTTPException(
                status_code=404, 
                detail=f"State/Country '{state}' not found in dataset"
            )
        
        # Extract first match and convert to dict
        record = state_data.iloc[0].to_dict()
        record = {k: round_floats(v) for k, v in record.items()}
        
        return {
            "success": True,
            "state": state,
            "data": record
        }
        
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/all")
async def get_all_states(limit: Optional[int] = Query(default=100, ge=1, le=1000)):
    """
    Get all states risk data (limited for performance).
    
    Args:
        limit: Maximum number of records to return (default: 100, max: 1000)
        
    Returns:
        List of state risk records
    """
    try:
        # Load data
        df = load_data()
        
        # Limit rows for performance
        df_limited = df.head(limit)
        
        # Convert to records and round floats
        records = df_limited.to_dict('records')
        records = [{k: round_floats(v) for k, v in record.items()} for record in records]
        
        return {
            "success": True,
            "total_records": len(records),
            "limit": limit,
            "total_available": len(df),
            "data": records
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
