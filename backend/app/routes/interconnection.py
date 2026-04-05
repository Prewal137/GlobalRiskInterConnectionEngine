"""
🔗 Interconnected Risk API Routes

Serves multi-sector cascading risk data combining climate, economy, trade,
geopolitics, migration, social, and infrastructure risks with calculated impact metrics.

Endpoints:
1. GET /interconnection/risk/{country}/{year}/{month} - Risk by date
2. GET /interconnection/latest/{country} - Latest risk snapshot
3. GET /interconnection/trend/{country} - 12-month trend analysis
4. GET /interconnection/high-risk/{country} - High-risk months (global_risk > 0.7)
5. GET /interconnection/summary/{country} - Country risk summary
"""

from fastapi import APIRouter, HTTPException
from functools import lru_cache
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any

# Create router
router = APIRouter(prefix="/interconnection", tags=["interconnection"])

# ================================================================
# 📂 DATA LOADING
# ================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
INTERCONNECTED_RISK_FILE = os.path.join(BASE_DIR, "data", "processed", "interconnection", "interconnected_risk.csv")


def load_interconnection_data() -> pd.DataFrame:
    """
    Load interconnected risk data from CSV.
    
    Returns:
        pd.DataFrame: Interconnected risk dataset
    """
    if not os.path.exists(INTERCONNECTED_RISK_FILE):
        raise FileNotFoundError(f"Interconnected risk file not found: {INTERCONNECTED_RISK_FILE}")
    
    df = pd.read_csv(INTERCONNECTED_RISK_FILE)
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
    df = load_interconnection_data()
    
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
def get_risk_by_date_cached(country: str, year: int, month: int):
    """Get risk data for specific country and date (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, [
        'Country', 'Year', 'Month', 'climate_risk', 'economic_risk',
        'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ])
    
    matching_rows = df[
        (df['Year'] == year) & 
        (df['Month'] == month)
    ]
    
    if matching_rows.empty:
        return None
    
    row = matching_rows.iloc[0]
    return {
        "country": row['Country'],
        "year": int(row['Year']),
        "month": int(row['Month']),
        "climate_risk": float(row['climate_risk']),
        "economic_risk": float(row['economic_risk']),
        "trade_risk": float(row['trade_risk']),
        "geopolitical_risk": float(row['geopolitical_risk']),
        "migration_risk": float(row['migration_risk']),
        "social_risk": float(row['social_risk']),
        "infrastructure_risk": float(row['infrastructure_risk']),
        "economic_impact": float(row['economic_impact']),
        "trade_impact": float(row['trade_impact']),
        "global_risk": float(row['global_risk'])
    }


@lru_cache(maxsize=64)
def get_latest_risk_cached(country: str):
    """Get latest risk data for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, [
        'Country', 'Year', 'Month', 'climate_risk', 'economic_risk',
        'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ])
    
    # Sort by Year and Month descending, take first
    df_sorted = df.sort_values(['Year', 'Month'], ascending=[False, False])
    
    if df_sorted.empty:
        return None
    
    row = df_sorted.iloc[0]
    return {
        "country": row['Country'],
        "year": int(row['Year']),
        "month": int(row['Month']),
        "climate_risk": float(row['climate_risk']),
        "economic_risk": float(row['economic_risk']),
        "trade_risk": float(row['trade_risk']),
        "geopolitical_risk": float(row['geopolitical_risk']),
        "migration_risk": float(row['migration_risk']),
        "social_risk": float(row['social_risk']),
        "infrastructure_risk": float(row['infrastructure_risk']),
        "economic_impact": float(row['economic_impact']),
        "trade_impact": float(row['trade_impact']),
        "global_risk": float(row['global_risk'])
    }


@lru_cache(maxsize=64)
def get_trend_data_cached(country: str) -> tuple:
    """Get trend data (last 12 months) for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, [
        'Country', 'Year', 'Month', 'climate_risk', 'economic_risk',
        'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ])
    
    # Sort by Year and Month
    df_sorted = df.sort_values(['Year', 'Month'], ascending=[True, True])
    
    # Take last 12 months
    df_last_12 = df_sorted.tail(12)
    
    # Convert to list of dicts
    trend_data = []
    for _, row in df_last_12.iterrows():
        trend_data.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "global_risk": round(float(row['global_risk']), 4),
            "social_risk": round(float(row['social_risk']), 4),
            "infrastructure_risk": round(float(row['infrastructure_risk']), 4),
            "migration_risk": round(float(row['migration_risk']), 4),
            "economic_risk": round(float(row['economic_risk']), 4)
        })
    
    return tuple(trend_data)


@lru_cache(maxsize=64)
def get_high_risk_months_cached(country: str) -> tuple:
    """Get high-risk months (global_risk > 0.7) for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, [
        'Country', 'Year', 'Month', 'climate_risk', 'economic_risk',
        'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ])
    
    # Filter high-risk months
    high_risk = df[df['global_risk'] > 0.7].copy()
    
    if high_risk.empty:
        return tuple()
    
    # Sort by global_risk descending
    high_risk = high_risk.sort_values('global_risk', ascending=False)
    
    # Convert to list of dicts
    result = []
    for _, row in high_risk.iterrows():
        result.append({
            "year": int(row['Year']),
            "month": int(row['Month']),
            "global_risk": round(float(row['global_risk']), 4),
            "climate_risk": round(float(row['climate_risk']), 4),
            "economic_risk": round(float(row['economic_risk']), 4),
            "trade_risk": round(float(row['trade_risk']), 4),
            "geopolitical_risk": round(float(row['geopolitical_risk']), 4),
            "migration_risk": round(float(row['migration_risk']), 4),
            "social_risk": round(float(row['social_risk']), 4),
            "infrastructure_risk": round(float(row['infrastructure_risk']), 4)
        })
    
    return tuple(result)


@lru_cache(maxsize=64)
def get_summary_cached(country: str):
    """Get summary statistics for a country (CACHED)."""
    records = get_country_data_cached(country.upper())
    
    if records is None:
        return None
    
    df = reconstruct_dataframe(records, [
        'Country', 'Year', 'Month', 'climate_risk', 'economic_risk',
        'trade_risk', 'geopolitical_risk', 'migration_risk', 'social_risk', 'infrastructure_risk',
        'economic_impact', 'trade_impact', 'global_risk'
    ])
    
    # Calculate statistics
    avg_global_risk = float(df['global_risk'].mean())
    max_global_risk = float(df['global_risk'].max())
    min_global_risk = float(df['global_risk'].min())
    
    # Determine trend direction
    df_sorted = df.sort_values(['Year', 'Month'], ascending=[True, True])
    
    if len(df_sorted) >= 2:
        first_half = df_sorted.head(len(df_sorted) // 2)['global_risk'].mean()
        second_half = df_sorted.tail(len(df_sorted) // 2)['global_risk'].mean()
        
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
        "date_range": {
            "start": f"{int(df_sorted.iloc[0]['Year'])}/{int(df_sorted.iloc[0]['Month'])}",
            "end": f"{int(df_sorted.iloc[-1]['Year'])}/{int(df_sorted.iloc[-1]['Month'])}"
        },
        "avg_global_risk": round(avg_global_risk, 4),
        "max_global_risk": round(max_global_risk, 4),
        "min_global_risk": round(min_global_risk, 4),
        "trend_direction": trend_direction,
        "sector_averages": {
            "climate_risk": round(float(df['climate_risk'].mean()), 4),
            "economic_risk": round(float(df['economic_risk'].mean()), 4),
            "trade_risk": round(float(df['trade_risk'].mean()), 4),
            "geopolitical_risk": round(float(df['geopolitical_risk'].mean()), 4),
            "migration_risk": round(float(df['migration_risk'].mean()), 4),
            "social_risk": round(float(df['social_risk'].mean()), 4),
            "infrastructure_risk": round(float(df['infrastructure_risk'].mean()), 4)
        }
    }


# ================================================================
# 🎯 API ENDPOINTS
# ================================================================

@router.get("/risk/{country}/{year}/{month}")
async def get_risk_by_date(country: str, year: int, month: int):
    """
    Get interconnected risk data for a specific country and date.
    
    Args:
        country: Country code (e.g., 'IND')
        year: Year (e.g., 2020)
        month: Month (1-12)
        
    Returns:
        All sector risks and cascading impacts for the specified date
    """
    try:
        # Use CACHED function for instant response! ⚡
        result = get_risk_by_date_cached(country.upper(), year, month)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for {country.upper()} in {year}/{month}"
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
    Get latest available risk data for a country.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        Most recent risk snapshot with all sectors and impacts
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
    Get risk trend for the last 12 months.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        Time series of global_risk, economic_impact, and trade_impact
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
async def get_high_risk_months(country: str):
    """
    Get months where global_risk exceeds 0.7 threshold.
    
    Args:
        country: Country code (e.g., 'IND')
        
    Returns:
        List of high-risk months sorted by severity
    """
    try:
        # Use CACHED function for instant response! ⚡
        high_risk_data = get_high_risk_months_cached(country.upper())
        
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
            "high_risk_months": high_risk_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/summary/{country}")
async def get_summary(country: str):
    """
    Get comprehensive risk summary for a country.
    
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
