"""
🌍 GEOPOLITICS RISK API ROUTES

RESTful API endpoints for geopolitical risk prediction.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict
import pandas as pd
import os

router = APIRouter(
    prefix="/geopolitics-risk",
    tags=["Geopolitics Risk"]
)

# ================================================================
# 🔧 CONFIGURATION
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
RISK_OUTPUT_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output.csv")
COUNTRY_SUMMARY_FILE = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output_country.csv")


# ================================================================
# 📊 HELPER FUNCTIONS
# ================================================================

def load_risk_data():
    """Load risk prediction data."""
    if not os.path.exists(RISK_OUTPUT_FILE):
        raise FileNotFoundError(f"Risk output file not found: {RISK_OUTPUT_FILE}")
    
    df = pd.read_csv(RISK_OUTPUT_FILE)
    return df


def load_country_summary():
    """Load country summary data."""
    if not os.path.exists(COUNTRY_SUMMARY_FILE):
        raise FileNotFoundError(f"Country summary file not found: {COUNTRY_SUMMARY_FILE}")
    
    df = pd.read_csv(COUNTRY_SUMMARY_FILE)
    return df


# ================================================================
# 🛣️ API ENDPOINTS
# ================================================================

@router.get("/country/{country_code}")
async def get_country_risk(
    country_code: str,
    year: Optional[int] = Query(None, description="Filter by specific year")
):
    """
    Get geopolitical risk score for a specific country.
    
    Args:
        country_code: ISO-3 country code (e.g., USA, IND, AFG)
        year: Optional year filter
    
    Returns:
        Country risk data with time series
    """
    df = load_risk_data()
    
    # Filter by country
    country_data = df[df['Country'] == country_code]
    
    if len(country_data) == 0:
        raise HTTPException(status_code=404, detail=f"Country '{country_code}' not found")
    
    # Filter by year if provided
    if year is not None:
        country_data = country_data[country_data['Year'] == year]
    
    # Format response
    response = {
        "country": country_code,
        "total_observations": len(country_data),
        "risk_statistics": {
            "current_risk": float(country_data.iloc[-1]['risk_score']) if len(country_data) > 0 else None,
            "average_risk": float(country_data['risk_score'].mean()),
            "min_risk": float(country_data['risk_score'].min()),
            "max_risk": float(country_data['risk_score'].max())
        },
        "time_series": [
            {
                "year": int(row['Year']),
                "month": int(row['Month']),
                "risk_score": float(row['risk_score']),
                "risk_raw": float(row['risk_raw'])
            }
            for _, row in country_data.iterrows()
        ]
    }
    
    return response


@router.get("/top-countries")
async def get_top_risk_countries(
    top_n: int = Query(10, ge=1, le=300, description="Number of top countries to return"),
    min_year: Optional[int] = Query(None, description="Filter by minimum year")
):
    """
    Get top N highest-risk countries.
    
    Args:
        top_n: Number of countries to return (1-50)
        min_year: Optional minimum year filter
    
    Returns:
        List of highest-risk countries
    """
    country_summary = load_country_summary()
    
    # Filter by year if provided
    if min_year is not None:
        # Need to recalculate from raw data
        df = load_risk_data()
        df_filtered = df[df['Year'] >= min_year]
        
        country_summary = df_filtered.groupby('Country').agg(
            avg_risk=('risk_score', 'mean'),
            latest_risk=('risk_score', 'last'),
            min_risk=('risk_score', 'min'),
            max_risk=('risk_score', 'max')
        ).reset_index()
        country_summary = country_summary.sort_values('latest_risk', ascending=False)
    
    # Get top N
    top_countries = country_summary.head(top_n)
    
    response = {
        "query": {
            "top_n": top_n,
            "min_year": min_year
        },
        "countries": [
            {
                "rank": i + 1,
                "country": row['Country'],
                "latest_risk": float(row['latest_risk']),
                "average_risk": float(row['avg_risk']),
                "min_risk": float(row['min_risk']),
                "max_risk": float(row['max_risk']),
                "risk_trend": float(row['risk_trend']) if 'risk_trend' in country_summary.columns else None
            }
            for i, row in top_countries.iterrows()
        ]
    }
    
    return response


@router.get("/global-summary")
async def get_global_risk_summary():
    """
    Get global geopolitical risk summary statistics.
    
    Returns:
        Global risk statistics and distribution
    """
    df = load_risk_data()
    
    # Calculate statistics
    total_obs = len(df)
    countries = df['Country'].nunique()
    year_range = f"{df['Year'].min()}-{df['Year'].max()}"
    
    avg_risk = float(df['risk_score'].mean())
    std_risk = float(df['risk_score'].std())
    min_risk = float(df['risk_score'].min())
    max_risk = float(df['risk_score'].max())
    
    # Risk distribution
    very_high = int((df['risk_score'] > 0.8).sum())
    high = int(((df['risk_score'] > 0.6) & (df['risk_score'] <= 0.8)).sum())
    medium = int(((df['risk_score'] > 0.4) & (df['risk_score'] <= 0.6)).sum())
    low = int((df['risk_score'] <= 0.4).sum())
    
    # Latest global risk (most recent year/month)
    latest_date = df.sort_values(['Year', 'Month'], ascending=False).iloc[0]
    latest_global_risk = float(latest_date['risk_score'])
    
    response = {
        "overview": {
            "total_observations": total_obs,
            "countries_analyzed": countries,
            "time_period": year_range
        },
        "statistics": {
            "average_risk": avg_risk,
            "std_deviation": std_risk,
            "min_risk": min_risk,
            "max_risk": max_risk,
            "latest_global_risk": latest_global_risk
        },
        "distribution": {
            "very_high_risk": {
                "count": very_high,
                "percentage": round(very_high / total_obs * 100, 2)
            },
            "high_risk": {
                "count": high,
                "percentage": round(high / total_obs * 100, 2)
            },
            "medium_risk": {
                "count": medium,
                "percentage": round(medium / total_obs * 100, 2)
            },
            "low_risk": {
                "count": low,
                "percentage": round(low / total_obs * 100, 2)
            }
        }
    }
    
    return response


@router.get("/compare")
async def compare_countries(
    countries: str = Query(..., description="Comma-separated list of country codes to compare (e.g., IND,USA,CHN)")
):
    """
    Compare geopolitical risk across multiple countries.
    
    Args:
        countries: Comma-separated list of country codes
    
    Returns:
        Comparative analysis of selected countries
    """
    df = load_risk_data()
    country_summary = load_country_summary()
    
    # Parse country list
    country_list = [c.strip().upper() for c in countries.split(',')]
    
    print(f"🔍 Comparing countries: {country_list}")
    
    # Get data for selected countries
    comparison_data = df[df['Country'].isin(country_list)]
    comparison_summary = country_summary[country_summary['Country'].isin(country_list)]
    
    if len(comparison_summary) == 0:
        raise HTTPException(status_code=404, detail=f"No matching countries found for: {countries}")
    
    # Get latest data points for each country
    latest_data = []
    for country in country_list:
        country_data = comparison_data[comparison_data['Country'] == country]
        if len(country_data) > 0:
            latest = country_data.sort_values(['Year', 'Month'], ascending=False).iloc[0]
            latest_data.append({
                "country": country,
                "current_risk": float(latest['risk_score']),
                "risk_category": get_risk_category(float(latest['risk_score'])),
                "year": int(latest['Year']),
                "month": int(latest['Month'])
            })
    
    # Sort by current risk (descending)
    latest_data = sorted(latest_data, key=lambda x: x['current_risk'], reverse=True)
    
    # Add rankings
    for i, item in enumerate(latest_data):
        item['rank'] = i + 1
    
    # Calculate comparative statistics
    avg_risk = sum(item['current_risk'] for item in latest_data) / len(latest_data)
    max_risk = max(item['current_risk'] for item in latest_data)
    min_risk = min(item['current_risk'] for item in latest_data)
    
    response = {
        "query": {
            "countries_requested": country_list,
            "countries_found": len(latest_data)
        },
        "summary": {
            "average_risk": round(avg_risk, 4),
            "highest_risk": round(max_risk, 4),
            "lowest_risk": round(min_risk, 4),
            "risk_spread": round(max_risk - min_risk, 4)
        },
        "rankings": latest_data,
        "detailed_stats": [
            {
                "country": row['Country'],
                "latest_risk": float(row['latest_risk']),
                "average_risk": float(row['avg_risk']),
                "min_risk": float(row['min_risk']),
                "max_risk": float(row['max_risk']),
                "risk_trend": float(row['risk_trend']) if 'risk_trend' in row else None,
                "category": get_risk_category(float(row['latest_risk']))
            }
            for _, row in comparison_summary.iterrows()
        ]
    }
    
    return response


def get_risk_category(risk_score: float) -> str:
    """Helper function to get risk category from score."""
    if risk_score > 0.8:
        return "Very High"
    elif risk_score > 0.6:
        return "High"
    elif risk_score > 0.4:
        return "Medium"
    elif risk_score > 0.2:
        return "Low"
    else:
        return "Very Low"


@router.get("/search/{query}")
async def search_countries(query: str):
    """
    Search countries by name/partial match.
    
    Args:
        query: Search query string
    
    Returns:
        Matching countries with current risk scores
    """
    country_summary = load_country_summary()
    
    # Case-insensitive partial match
    matches = country_summary[
        country_summary['Country'].str.contains(query, case=False, na=False)
    ]
    
    response = {
        "query": query,
        "matches_found": len(matches),
        "countries": [
            {
                "country": row['Country'],
                "latest_risk": float(row['latest_risk']),
                "average_risk": float(row['avg_risk'])
            }
            for _, row in matches.iterrows()
        ]
    }
    
    return response
