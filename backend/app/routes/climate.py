from fastapi import APIRouter, HTTPException
import pandas as pd
import os
router = APIRouter(prefix="/climate-risk")


# ================================================================
# 📂 LOAD DATA (ON START)
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

state_path = os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_state.csv")
district_path = os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_district.csv")
interconnection_path = os.path.join(BASE_PATH, "data", "processed", "interconnection", "interconnected_risk.csv")

state_df = pd.read_csv(state_path)
district_df = pd.read_csv(district_path)

# Load interconnection data if available
try:
    interconnection_df = pd.read_csv(interconnection_path)
    print("✅ Interconnection data loaded!")
except FileNotFoundError:
    print("⚠️ Interconnection data not found. Run interconnection_engine.py first.")
    interconnection_df = None

print("✅ Climate risk data loaded successfully!")
print(f"   States: {len(state_df)}")
print(f"   Districts: {len(district_df)}")

# ================================================================
# 🔧 HELPER FUNCTION (RISK LEVEL)
# ================================================================

def get_risk_level(score):
    """
    Classify risk level based on normalized score (0-1).
    
    Args:
        score (float): Normalized climate risk score (0.0 to 1.0)
    
    Returns:
        str: Risk level category
    """
    if score < 0.05:
        return "VERY LOW"
    elif score < 0.08:
        return "LOW"
    elif score < 0.12:
        return "MEDIUM"
    elif score < 0.20:
        return "HIGH"
    else:
        return "VERY HIGH"

# ================================================================
# 🏛️ STATE ENDPOINT
# ================================================================

@router.get("/state/{state}")
def get_state_risk(state: str):
    """
    Get climate risk information for a specific state.
    
    Args:
        state (str): State name (case-insensitive)
    
    Returns:
        dict: State climate risk data with risk level
    
    Raises:
        HTTPException: 404 if state not found
    """
    result = state_df[state_df['State'].str.lower() == state.lower()]
    
    if result.empty:
        raise HTTPException(status_code=404, detail=f"State '{state}' not found")
    
    row = result.iloc[0]
    score = row['predicted_risk']
    
    return {
        "state": row['State'],
        "climate_risk_score": round(score, 4),
        "risk_level": get_risk_level(score)
    }

# ================================================================
# 🗺️ DISTRICT ENDPOINT
# ================================================================

@router.get("/district/{district}")
def get_district_risk(district: str):
    """
    Get climate risk information for a specific district.
    
    Args:
        district (str): District name (case-insensitive)
    
    Returns:
        dict: District climate risk data with state and risk level
    
    Raises:
        HTTPException: 404 if district not found
    """
    result = district_df[district_df['District'].str.lower() == district.lower()]
    
    if result.empty:
        raise HTTPException(status_code=404, detail=f"District '{district}' not found")
    
    row = result.iloc[0]
    score = row['predicted_risk']
    
    return {
        "state": row['State'],
        "district": row['District'],
        "climate_risk_score": round(score, 4),
        "risk_level": get_risk_level(score)
    }

# ================================================================
# 🏆 TOP STATES ENDPOINT
# ================================================================

@router.get("/top-states")
def get_top_states():
    """
    Get top 5 states with highest climate risk.
    
    Returns:
        list: List of top 5 high-risk states with scores
    """
    top = state_df.sort_values(by='predicted_risk', ascending=False).head(5)
    
    return top[['State', 'predicted_risk']].to_dict(orient="records")

# ================================================================
# 🏆 TOP DISTRICTS ENDPOINT
# ================================================================

@router.get("/top-districts")
def get_top_districts():
    """
    Get top 5 districts with highest climate risk.
    
    Returns:
        list: List of top 5 high-risk districts with state and scores
    """
    top = district_df.sort_values(by='predicted_risk', ascending=False).head(5)
    
    return top[['State', 'District', 'predicted_risk']].to_dict(orient="records")

# ================================================================
# 📊 ALL STATES ENDPOINT
# ================================================================

@router.get("/states")
def get_all_states():
    """
    Get climate risk data for all states.
    
    Returns:
        list: List of all states with risk scores and levels
    """
    results = []
    for _, row in state_df.iterrows():
        score = row['predicted_risk']
        results.append({
            "state": row['State'],
            "climate_risk_score": round(score, 4),
            "risk_level": get_risk_level(score)
        })
    
    return results

# ================================================================
# 📊 ALL DISTRICTS ENDPOINT
# ================================================================

@router.get("/districts")
def get_all_districts():
    """
    Get climate risk data for all districts.
    
    Returns:
        list: List of all districts with state, risk scores and levels
    """
    results = []
    for _, row in district_df.iterrows():
        score = row['predicted_risk']
        results.append({
            "state": row['State'],
            "district": row['District'],
            "climate_risk_score": round(score, 4),
            "risk_level": get_risk_level(score)
        })
    
    return results

# ================================================================
# 🌍 INTERCONNECTION ENDPOINTS
# ================================================================

@router.get("/interconnection/district/{district}")
def get_interconnected_risk(district: str):
    """
    Get interconnected risk scores for a specific district.
    
    Args:
        district (str): District name (case-insensitive)
    
    Returns:
        dict: All risk scores (climate, migration, economic, infrastructure, composite)
    
    Raises:
        HTTPException: 404 if district not found or interconnection data not available
    """
    if interconnection_df is None:
        raise HTTPException(
            status_code=503, 
            detail="Interconnection engine not run. Please run interconnection_engine.py first."
        )
    
    result = interconnection_df[interconnection_df['District'].str.lower() == district.lower()]
    
    if result.empty:
        raise HTTPException(status_code=404, detail=f"District '{district}' not found")
    
    row = result.iloc[0]
    
    return {
        "state": row['State'],
        "district": row['District'],
        "climate_risk": round(row['predicted_risk'], 4),
        "migration_risk": round(row['migration_risk'], 4),
        "economic_risk": round(row['economic_risk'], 4),
        "infrastructure_risk": round(row['infrastructure_risk'], 4),
        "composite_risk": round(row['composite_risk'], 4)
    }

@router.get("/interconnection/top-districts")
def get_top_interconnected_districts():
    """
    Get top 5 districts with highest composite interconnected risk.
    
    Returns:
        list: Top 5 districts with all risk scores
    
    Raises:
        HTTPException: 503 if interconnection data not available
    """
    if interconnection_df is None:
        raise HTTPException(
            status_code=503, 
            detail="Interconnection engine not run. Please run interconnection_engine.py first."
        )
    
    top = interconnection_df.nlargest(5, 'composite_risk')
    
    return top.to_dict(orient="records")

@router.get("/interconnection/summary")
def get_interconnection_summary():
    """
    Get summary statistics of interconnected risks.
    
    Returns:
        dict: Mean risk scores across all sectors
    
    Raises:
        HTTPException: 503 if interconnection data not available
    """
    if interconnection_df is None:
        raise HTTPException(
            status_code=503, 
            detail="Interconnection engine not run. Please run interconnection_engine.py first."
        )
    
    return {
        "total_districts": len(interconnection_df),
        "mean_climate_risk": round(interconnection_df['predicted_risk'].mean(), 4),
        "mean_migration_risk": round(interconnection_df['migration_risk'].mean(), 4),
        "mean_economic_risk": round(interconnection_df['economic_risk'].mean(), 4),
        "mean_infrastructure_risk": round(interconnection_df['infrastructure_risk'].mean(), 4),
        "mean_composite_risk": round(interconnection_df['composite_risk'].mean(), 4)
    }
