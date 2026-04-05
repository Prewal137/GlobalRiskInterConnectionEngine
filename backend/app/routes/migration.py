"""
🌍 Migration Risk API Routes

Serves migration risk data calculated from demographic and migration dynamics.

Endpoints:
1. GET /migration/risk/{country}/{year} - Risk by year
2. GET /migration/latest/{country} - Latest risk snapshot
3. GET /migration/trend/{country} - 10-year trend analysis
4. GET /migration/high-risk/{country} - High-risk years (risk > 0.7)
5. GET /migration/summary/{country} - Country risk summary
"""

from fastapi import APIRouter, HTTPException
from functools import lru_cache
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any

# Create router
router = APIRouter(prefix="/migration", tags=["migration"])

# ================================================================
# 📂 DATA LOADING
# ================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
MIGRATION_RISK_FILE = os.path.join(BASE_DIR, "data", "processed", "migration", "migration_risk_output.csv")


def load_migration_data() -> pd.DataFrame:
    """
    Load migration risk data from CSV.
    
    Returns:
        pd.DataFrame: Migration risk dataset
    """
    if not os.path.exists(MIGRATION_RISK_FILE):
        raise FileNotFoundError(f"Migration risk file not found: {MIGRATION_RISK_FILE}")
    
    df = pd.read_csv(MIGRATION_RISK_FILE)
    return df


# ================================================================
# ⚡ CACHED HELPER FUNCTIONS (LRU Cache for Performance)
# ================================================================

@lru_cache(maxsize=64)
def get_country_data_cached(country: str) -> tuple:
    """
    Get all data for a specific country (CACHED).
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        tuple: DataFrame records as tuples (for cache compatibility) or None
    """
    df = load_migration_data()
    
    # Filter by country (case-insensitive)
    country_data = df[df['Country'].str.upper() == country.upper()]
    
    if country_data.empty:
        return None
    
    # Convert to list of tuples for caching
    records = []
    for _, row in country_data.iterrows():
        records.append(tuple(row.values))
    
    return tuple(records)


def reconstruct_dataframe(records: tuple, columns: List[str]) -> pd.DataFrame:
    """Reconstruct DataFrame from cached tuple records."""
    if records is None:
        return pd.DataFrame()
    
    df = pd.DataFrame([list(record) for record in records], columns=columns)
    return df


@lru_cache(maxsize=256)
def get_risk_by_year_cached(country: str, year: int):
    """Get risk data for specific country and year (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, ['Country', 'Year', 'migration_risk'])
    
    matching_rows = df[df['Year'] == year]
    
    if matching_rows.empty:
        return None
    
    row = matching_rows.iloc[0]
    return {
        "country": row['Country'],
        "year": int(row['Year']),
        "migration_risk": float(row['migration_risk'])
    }


@lru_cache(maxsize=64)
def get_latest_risk_cached(country: str):
    """Get latest risk data for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, ['Country', 'Year', 'migration_risk'])
    
    # Sort by Year descending, take first
    df_sorted = df.sort_values('Year', ascending=False)
    
    if df_sorted.empty:
        return None
    
    row = df_sorted.iloc[0]
    return {
        "country": row['Country'],
        "year": int(row['Year']),
        "migration_risk": float(row['migration_risk'])
    }


@lru_cache(maxsize=64)
def get_trend_data_cached(country: str) -> tuple:
    """Get trend data (last 10 years) for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, ['Country', 'Year', 'migration_risk'])
    
    # Sort by Year
    df_sorted = df.sort_values('Year', ascending=True)
    
    # Take last 10 years
    df_last_10 = df_sorted.tail(10)
    
    # Convert to list of dicts
    trend_data = []
    for _, row in df_last_10.iterrows():
        trend_data.append({
            "year": int(row['Year']),
            "migration_risk": round(float(row['migration_risk']), 4)
        })
    
    return tuple(trend_data)


@lru_cache(maxsize=64)
def get_high_risk_years_cached(country: str) -> tuple:
    """Get high-risk years (migration_risk > 0.7) for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, ['Country', 'Year', 'migration_risk'])
    
    # Filter high-risk years
    high_risk = df[df['migration_risk'] > 0.7].copy()
    
    if high_risk.empty:
        return tuple()
    
    # Sort by migration_risk descending
    high_risk = high_risk.sort_values('migration_risk', ascending=False)
    
    # Convert to list of dicts
    result = []
    for _, row in high_risk.iterrows():
        result.append({
            "year": int(row['Year']),
            "migration_risk": round(float(row['migration_risk']), 4)
        })
    
    return tuple(result)


@lru_cache(maxsize=64)
def get_summary_cached(country: str):
    """Get summary statistics for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, ['Country', 'Year', 'migration_risk'])
    
    # Calculate statistics
    avg_risk = float(df['migration_risk'].mean())
    max_risk = float(df['migration_risk'].max())
    min_risk = float(df['migration_risk'].min())
    
    # Determine trend direction
    df_sorted = df.sort_values('Year', ascending=True)
    
    if len(df_sorted) >= 2:
        first_half = df_sorted.head(len(df_sorted) // 2)['migration_risk'].mean()
        second_half = df_sorted.tail(len(df_sorted) // 2)['migration_risk'].mean()
        
        if second_half > first_half * 1.1:
            trend_direction = "INCREASING"
        elif second_half < first_half * 0.9:
            trend_direction = "DECREASING"
        else:
            trend_direction = "STABLE"
    else:
        trend_direction = "INSUFFICIENT_DATA"
    
    return {
        "country": country.upper(),
        "total_records": len(df),
        "year_range": {
            "start": int(df_sorted.iloc[0]['Year']),
            "end": int(df_sorted.iloc[-1]['Year'])
        },
        "avg_migration_risk": round(avg_risk, 4),
        "max_migration_risk": round(max_risk, 4),
        "min_migration_risk": round(min_risk, 4),
        "trend_direction": trend_direction
    }


# ================================================================
# 🎯 API ENDPOINTS
# ================================================================

@router.get("/risk/{country}/{year}")
async def get_risk_by_year(country: str, year: int):
    """
    Get migration risk for a specific country and year.
    
    Args:
        country: Country code (e.g., 'IND')
        year: Year (e.g., 2020)
        
    Returns:
        Migration risk for the specified year
    """
    try:
        # Use CACHED function for instant response! ⚡
        result = get_risk_by_year_cached(country.upper(), year)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for {country.upper()} in year {year}"
            )
        
        result["timestamp"] = datetime.utcnow().isoformat()
        
        return {
            "success": True,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/latest/{country}")
async def get_latest_risk(country: str):
    """
    Get latest available migration risk for a country.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        Most recent migration risk snapshot
    """
    try:
        # Use CACHED function for instant response! ⚡
        result = get_latest_risk_cached(country.upper())
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for country: {country.upper()}"
            )
        
        result["timestamp"] = datetime.utcnow().isoformat()
        
        return {
            "success": True,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/trend/{country}")
async def get_trend(country: str):
    """
    Get migration risk trend for the last 10 years.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        Time series of migration risk for last 10 years
    """
    try:
        # Use CACHED function for instant response! ⚡
        trend_data = get_trend_data_cached(country.upper())
        
        if trend_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for country: {country.upper()}"
            )
        
        trend_list = list(trend_data)
        
        if not trend_list:
            raise HTTPException(
                status_code=404,
                detail=f"No trend data available for {country.upper()}"
            )
        
        return {
            "success": True,
            "country": country.upper(),
            "data_points": len(trend_list),
            "trend": trend_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/high-risk/{country}")
async def get_high_risk_years(country: str):
    """
    Get years where migration risk exceeds 0.7 threshold.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        List of high-risk years sorted by severity
    """
    try:
        # Use CACHED function for instant response! ⚡
        high_risk_data = get_high_risk_years_cached(country.upper())
        
        if high_risk_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for country: {country.upper()}"
            )
        
        high_risk_list = list(high_risk_data)
        
        return {
            "success": True,
            "country": country.upper(),
            "threshold": 0.7,
            "high_risk_count": len(high_risk_list),
            "high_risk_years": high_risk_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/summary/{country}")
async def get_summary(country: str):
    """
    Get comprehensive migration risk summary for a country.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        Summary statistics including averages, extremes, and trend
    """
    try:
        # Use CACHED function for instant response! ⚡
        summary = get_summary_cached(country.upper())
        
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for country: {country.upper()}"
            )
        
        summary["timestamp"] = datetime.utcnow().isoformat()
        
        return {
            "success": True,
            "summary": summary
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
