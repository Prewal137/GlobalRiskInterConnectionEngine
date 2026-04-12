"""
🔗 Interconnected Risk API Routes

Serves multi-sector cascading risk data combining climate, economy, trade,
geopolitics, migration, social, and infrastructure risks with calculated impact metrics.

EXISTING ENDPOINTS (Static Interconnection):
1. GET /interconnection/risk/{country}/{year}/{month} - Risk by date
2. GET /interconnection/latest/{country} - Latest risk snapshot
3. GET /interconnection/trend/{country} - 12-month trend analysis
4. GET /interconnection/high-risk/{country} - High-risk months (global_risk > 0.7)
5. GET /interconnection/summary/{country} - Country risk summary

DYNAMIC GRAPH ENDPOINTS:
6. GET  /interconnection/dynamic - Run learning-based graph cascade simulation
7. GET  /interconnection/shock/{sector}/{value} - Simulate sector shock
8. GET  /interconnection/compare - Compare static vs dynamic results
9. POST /interconnection/custom - Run custom risk scenario simulation
10. POST /interconnection/live - Live AI pipeline with real-time data

ADVANCED ENDPOINTS (NEW):
11. GET  /interconnection/history/{year} - Historical risk data by year
12. GET  /interconnection/state/{state} - State-level climate risk
13. GET  /interconnection/state-impact/{state} - Cascade impact simulation
14. POST /interconnection/what-if - Custom what-if scenario simulation
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


# ================================================================
# 🚀 DYNAMIC GRAPH INTERCONNECTION ENDPOINTS
# ================================================================

@router.get("/dynamic")
async def dynamic_risk():
    """
    Run the complete learning-based dynamic graph interconnection system.
    
    This endpoint:
    1. Loads risk time series data
    2. Learns edge weights from data (NO fixed weights!)
    3. Builds interconnection graph
    4. Runs cascade simulation
    5. Returns final risk and cascade history
    
    Returns:
        Dynamic graph computation results with cascade steps
    """
    try:
        from app.graph.dynamic_engine import run_dynamic_system
        
        result = run_dynamic_system()
        
        # Convert non-serializable objects to dict
        final_risk = result.get("final_risk", {})
        cascade_history = result.get("cascade_history", [])
        
        return {
            "type": "dynamic_graph",
            "message": "Multi-step cascading risk computed",
            "final_risk": final_risk,
            "steps": cascade_history,
            "weights_count": len(result.get("weights", {}))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/shock/{sector}/{value}")
async def shock_simulation(sector: str, value: float):
    """
    Simulate a shock to a specific sector and analyze cascading effects.
    
    Args:
        sector: Sector to shock (e.g., 'climate', 'economy', 'trade')
        value: Shock value between 0 and 1
        
    Returns:
        Impact analysis comparing baseline vs shocked scenario
    """
    try:
        if value < 0 or value > 1:
            raise HTTPException(
                status_code=400,
                detail="Shock value must be between 0 and 1"
            )
        
        from app.graph.dynamic_engine import run_dynamic_system
        from app.graph.shock_simulator import simulate_shock
        
        # First run the dynamic system to get graph and risk data
        system_result = run_dynamic_system()
        
        graph = system_result.get("graph")
        risk_dict = system_result.get("initial_risk", {})
        
        if graph is None or not risk_dict:
            raise HTTPException(
                status_code=500,
                detail="Failed to initialize graph system"
            )
        
        # Validate sector
        if sector not in risk_dict:
            available_sectors = list(risk_dict.keys())
            raise HTTPException(
                status_code=400,
                detail=f"Sector '{sector}' not found. Available sectors: {available_sectors}"
            )
        
        # Run shock simulation
        result = simulate_shock(graph, risk_dict, sector, value)
        
        return {
            "type": "shock_simulation",
            "sector": sector,
            "shock_value": value,
            "impact": result.get("impact_metrics", {}),
            "total_system_impact": result.get("total_system_impact", 0.0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/compare")
async def compare_static_dynamic():
    """
    Compare static interconnection results vs dynamic learned graph results.
    
    Returns:
        Comparison between latest static risk and dynamic cascade final risk
    """
    try:
        # Load static data
        static_df = pd.read_csv(INTERCONNECTED_RISK_FILE)
        static_latest = static_df.tail(1).to_dict(orient="records")[0] if not static_df.empty else {}
        
        # Run dynamic system
        from app.graph.dynamic_engine import run_dynamic_system
        dynamic_result = run_dynamic_system()
        dynamic_final = dynamic_result.get("final_risk", {})
        
        return {
            "static_latest": static_latest,
            "dynamic_final": dynamic_final,
            "comparison_note": "Static uses fixed weights, Dynamic uses learned weights from data"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/custom")
async def custom_input(data: dict):
    """
    Run cascade simulation with custom input risk values.
    
    This allows testing with unseen/custom risk scenarios.
    
    Example input:
    {
        "climate": 0.9,
        "economy": 0.3,
        "trade": 0.2,
        "geopolitics": 0.4,
        "migration": 0.3,
        "social": 0.2,
        "infrastructure": 0.3
    }
    
    Args:
        data: Dictionary with sector risk values (0-1 scale)
        
    Returns:
        Custom simulation results with cascade history
    """
    try:
        from app.graph.dynamic_engine import run_dynamic_system
        from app.graph.cascade_engine import run_cascade
        
        # Validate input data
        valid_sectors = ['climate', 'economy', 'trade', 'geopolitics', 
                        'migration', 'social', 'infrastructure']
        
        for sector, value in data.items():
            if not (0 <= value <= 1):
                raise HTTPException(
                    status_code=400,
                    detail=f"Risk value for '{sector}' must be between 0 and 1"
                )
        
        # Run dynamic system to get the learned graph
        system_result = run_dynamic_system()
        graph = system_result.get("graph")
        
        if graph is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to initialize graph system"
            )
        
        # Run cascade with custom input
        cascade_history = run_cascade(graph, data, steps=5)
        
        return {
            "type": "custom_simulation",
            "input": data,
            "final_risk": cascade_history[-1] if cascade_history else {},
            "steps": cascade_history,
            "total_steps": len(cascade_history)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ================================================================
# 🚀 LIVE AI PIPELINE ENDPOINT
# ================================================================

@router.post("/live")
async def live_simulation():
    """
    Complete live AI pipeline:
    1. Fetch live data from APIs
    2. Map to model features
    3. Predict risk using trained models
    4. Build interconnection graph
    5. Run cascade simulation
    6. Return final risk with history
    
    Returns:
        Complete live risk assessment with cascade effects
    """
    try:
        from app.live.live_processor import process_live_data
        from app.graph.graph_builder import build_graph
        from app.graph.cascade_engine import run_cascade
        
        # Step 1: Live data → Model predictions → Risk scores
        print("\n🔄 Running live AI pipeline...")
        risk_dict = process_live_data()
        
        # Step 2: Build interconnection graph from learned weights
        print("\n🕸️  Building graph...")
        from backend.app.graph.risk_loader import load_risk_timeseries
        from backend.app.graph.weight_learner import learn_weights
        
        # Load historical data and learn weights
        df = load_risk_timeseries()
        weights = learn_weights(df, method='regression')
        graph = build_graph(weights)
        
        # Step 3: Run cascade simulation
        print("\n🌊 Running cascade simulation...")
        final_risk, history = run_cascade(graph, risk_dict, steps=5, damping=0.8)
        
        # Convert history to serializable format
        serializable_history = []
        for step in history:
            serializable_history.append({k: float(v) for k, v in step.items()})
        
        return {
            "mode": "live_ai_pipeline",
            "message": "Live data processed through ML models and graph cascade",
            "initial_risk": {k: float(v) for k, v in risk_dict.items()},
            "final_risk": {k: float(v) for k, v in final_risk.items()},
            "steps": serializable_history,
            "total_steps": len(serializable_history)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ================================================================
# 🆕 ADVANCED APIs - Historical, State, What-If Simulations
# ================================================================

@router.get("/history/{year}")
async def get_historical_risk(year: int):
    """
    Get historical risk data for a specific year.
    
    Args:
        year: Year to query (e.g., 2023, 2024)
    
    Returns:
        Historical risk data for the specified year
    """
    try:
        df = pd.read_csv(INTERCONNECTED_RISK_FILE)
        
        if "Year" not in df.columns:
            return {"error": "Year column missing in dataset"}
        
        df_year = df[df["Year"] == year]
        
        if df_year.empty:
            return {"error": f"No data available for year {year}"}
        
        return {
            "mode": "historical",
            "year": year,
            "record_count": len(df_year),
            "data": df_year.to_dict(orient="records")
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historical data: {str(e)}")


@router.get("/state/{state}")
async def get_state_risk(state: str):
    """
    Get climate risk data for a specific Indian state.
    
    Args:
        state: State name (e.g., "Maharashtra", "Karnataka")
    
    Returns:
        Climate risk data for the specified state
    """
    try:
        climate_risk_file = os.path.join(BASE_DIR, "data", "processed", "climate", "climate_risk_state.csv")
        
        if not os.path.exists(climate_risk_file):
            raise HTTPException(status_code=404, detail="Climate risk state data not found")
        
        df = pd.read_csv(climate_risk_file)
        
        if "State" not in df.columns:
            return {"error": "State column missing"}
        
        state_data = df[df["State"].str.lower() == state.lower()]
        
        if state_data.empty:
            return {"error": f"No data for state {state}"}
        
        return {
            "mode": "state",
            "state": state,
            "data": state_data.to_dict(orient="records")
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching state data: {str(e)}")


@router.get("/state-impact/{state}")
async def state_impact(state: str):
    """
    Simulate cascading impact when a specific state/sector experiences high risk.
    
    Args:
        state: State or sector name to apply shock
    
    Returns:
        Cascade simulation showing risk propagation
    """
    try:
        from app.graph.graph_builder import build_graph
        from app.graph.cascade_engine import run_cascade
        from app.graph.risk_loader import load_risk_timeseries
        from app.graph.weight_learner import learn_weights
        
        # Build graph with learned weights
        df = load_risk_timeseries()
        weights = learn_weights(df, method='regression')
        graph = build_graph(weights)
        
        # Initialize all nodes with low baseline risk
        risk = {node: 0.3 for node in graph.nodes()}
        
        # Apply shock to selected state/sector
        if state not in risk:
            # Try case-insensitive match
            matched_node = None
            for node in graph.nodes():
                if node.lower() == state.lower():
                    matched_node = node
                    break
            
            if matched_node:
                risk[matched_node] = 0.9
            else:
                return {"error": f"{state} not found in graph. Available: {list(graph.nodes())}"}
        else:
            risk[state] = 0.9
        
        # Run cascade simulation
        final_risk, history = run_cascade(graph, risk, steps=5, damping=0.8)
        
        # Convert to serializable format
        serializable_history = []
        for step in history:
            serializable_history.append({k: round(float(v), 4) for k, v in step.items()})
        
        return {
            "mode": "state-impact",
            "state": state,
            "initial": {k: round(float(v), 4) for k, v in risk.items()},
            "final": {k: round(float(v), 4) for k, v in final_risk.items()},
            "steps": serializable_history,
            "total_steps": len(serializable_history)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running state impact simulation: {str(e)}")


@router.post("/what-if")
async def what_if_simulation(payload: dict):
    """
    Run what-if scenario simulation with custom risk inputs.
    
    Example input:
    {
        "climate": 0.9,
        "economy": 0.8
    }
    
    Args:
        payload: Dictionary of sector risks to override
    
    Returns:
        Cascade simulation with custom inputs
    """
    try:
        from app.graph.graph_builder import build_graph
        from app.graph.cascade_engine import run_cascade
        from app.graph.risk_loader import load_risk_timeseries
        from app.graph.weight_learner import learn_weights
        
        # Build graph with learned weights
        df = load_risk_timeseries()
        weights = learn_weights(df, method='regression')
        graph = build_graph(weights)
        
        # Default baseline risk for all sectors
        risk = {node: 0.3 for node in graph.nodes()}
        
        # Override with user inputs
        for key, value in payload.items():
            # Try exact match first
            if key in risk:
                risk[key] = float(value)
            else:
                # Try case-insensitive match
                for node in graph.nodes():
                    if node.lower() == key.lower():
                        risk[node] = float(value)
                        break
        
        # Run cascade simulation
        final_risk, history = run_cascade(graph, risk, steps=5, damping=0.8)
        
        # Convert to serializable format
        serializable_history = []
        for step in history:
            serializable_history.append({k: round(float(v), 4) for k, v in step.items()})
        
        return {
            "mode": "what-if",
            "input": payload,
            "initial": {k: round(float(v), 4) for k, v in risk.items()},
            "final": {k: round(float(v), 4) for k, v in final_risk.items()},
            "steps": serializable_history,
            "total_steps": len(serializable_history)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running what-if simulation: {str(e)}")
