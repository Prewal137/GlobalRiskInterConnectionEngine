"""
Trade Risk API Routes
Serves trade risk prediction data via REST API.
"""

from fastapi import APIRouter, HTTPException
import pandas as pd
import os

# Create router
router = APIRouter(prefix="/trade-risk", tags=["trade-risk"])

# File paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
OUTPUT_FILE = os.path.join(BASE_DIR, "data/processed/trade/trade_risk_output.csv")
COUNTRY_FILE = os.path.join(BASE_DIR, "data/processed/trade/trade_risk_country.csv")
TOP_FILE = os.path.join(BASE_DIR, "data/processed/trade/trade_risk_top.csv")


def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV data using pandas."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")
    return pd.read_csv(filepath)


@router.get("/country/{country}")
async def get_country_risk(country: str):
    """
    Get all years of trade risk data for a specific country.
    
    Args:
        country: Country name
        
    Returns:
        List of records with year and trade risk for the country
    """
    try:
        # Load data
        df = load_data(OUTPUT_FILE)
        
        # Filter by country (case-insensitive)
        country_data = df[df['Country'].str.lower() == country.lower()]
        
        # Handle missing country
        if len(country_data) == 0:
            raise HTTPException(
                status_code=404, 
                detail=f"Country '{country}' not found in dataset"
            )
        
        # Convert to JSON records
        records = country_data[['Country', 'Year', 'Trade_Risk']].to_dict('records')
        
        return {
            "success": True,
            "country": country,
            "total_records": len(records),
            "data": records
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/top")
async def get_top_risky_countries():
    """
    Get top 10 countries with highest trade risk.
    
    Returns:
        List of top 10 risky countries with their risk scores
    """
    try:
        # Load data
        df = load_data(TOP_FILE)
        
        # Convert to JSON records
        records = df.to_dict('records')
        
        return {
            "success": True,
            "total_countries": len(records),
            "data": records
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/all")
async def get_all_trade_risk(limit: int = 1000):
    """
    Get full trade risk dataset (limited for performance).
    
    Args:
        limit: Maximum number of records to return (default: 1000)
        
    Returns:
        List of trade risk records
    """
    try:
        # Load data
        df = load_data(OUTPUT_FILE)
        
        # Limit rows for performance
        df_limited = df.head(limit)
        
        # Convert to JSON records
        records = df_limited[['Country', 'Year', 'Trade_Risk']].to_dict('records')
        
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


@router.get("/summary")
async def get_trade_risk_summary():
    """
    Get summary statistics of trade risk data.
    
    Returns:
        Summary metrics including total countries, average risk, and max risk country
    """
    try:
        # Load data
        df = load_data(COUNTRY_FILE)
        
        # Calculate summary statistics
        total_countries = len(df)
        average_risk = float(df['Trade_Risk'].mean())
        max_risk_row = df.loc[df['Trade_Risk'].idxmax()]
        min_risk_row = df.loc[df['Trade_Risk'].idxmin()]
        
        return {
            "success": True,
            "summary": {
                "total_countries": total_countries,
                "average_risk": round(average_risk, 4),
                "max_risk": {
                    "country": max_risk_row['Country'],
                    "risk_score": round(float(max_risk_row['Trade_Risk']), 4)
                },
                "min_risk": {
                    "country": min_risk_row['Country'],
                    "risk_score": round(float(min_risk_row['Trade_Risk']), 4)
                }
            }
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
