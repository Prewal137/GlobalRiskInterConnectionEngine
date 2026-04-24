from fastapi import APIRouter, HTTPException, Query
import pandas as pd
import os
from typing import Optional, List

router = APIRouter(prefix="/historical", tags=["Historical Risk"])

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# Map sectors to their data files and column mappings
SECTOR_CONFIG = {
    "climate": {
        "file": "data/processed/climate/climate_risk_output.csv",
        "risk_col": "predicted_risk"
    },
    "economy": {
        "file": "data/processed/economy/economic_risk_final.csv",
        "risk_col": "predicted_risk"
    },
    "social": {
        "file": "data/processed/social/social_risk_output.csv",
        "risk_col": "predicted_risk"
    },
    "migration": {
        "file": "data/processed/migration/migration_risk_output.csv",
        "risk_col": "migration_risk"
    },
    "infrastructure": {
        "file": "data/processed/infrastructure/infrastructure_risk_output.csv",
        "risk_col": "predicted_risk"
    },
    "trade": {
        "file": "data/processed/trade/trade_risk_output.csv",
        "risk_col": "Trade_Risk"
    },
    "geopolitics": {
        "file": "data/processed/geopolitics/geopolitics_risk_output.csv",
        "risk_col": "risk_score"
    },
    "global": {
        "file": "data/processed/interconnection/interconnected_risk.csv",
        "risk_col": "global_risk"
    }
}

@router.get("/{sector}")
def get_historical_data(
    sector: str,
    country: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    # Sector name mapping if needed (e.g., 'global' -> 'interconnection')
    if sector == "interconnection":
        sector = "global"
        
    if sector not in SECTOR_CONFIG:
        # Check if it matches any of the config keys case-insensitively
        found = False
        for k in SECTOR_CONFIG.keys():
            if k.lower() == sector.lower():
                sector = k
                found = True
                break
        if not found:
            raise HTTPException(status_code=404, detail=f"Sector '{sector}' not found")
    
    config = SECTOR_CONFIG[sector]
    file_path = os.path.join(BASE_PATH, config["file"])
    
    if not os.path.exists(file_path):
        return {"success": True, "sector": sector, "data": []}
    
    try:
        df = pd.read_csv(file_path)
        
        # Normalize column names (strip whitespace)
        df.columns = [c.strip() for c in df.columns]
        
        # Apply filters
        if country:
            country_col = next((c for c in df.columns if c.lower() == 'country'), None)
            if country_col:
                df = df[df[country_col].astype(str).str.upper() == country.upper()]
        
        if state:
            state_col = next((c for c in df.columns if c.lower() == 'state'), None)
            if state_col:
                df = df[df[state_col].astype(str).str.upper() == state.upper()]

        if year:
            year_col = next((c for c in df.columns if c.lower() == 'year'), None)
            if year_col:
                df = df[df[year_col].astype(int) == int(year)]
        
        if df.empty:
            return {"success": True, "sector": sector, "data": []}
        
        # Standardize risk score column
        risk_col = config["risk_col"]
        if risk_col in df.columns:
            df = df.rename(columns={risk_col: "risk_score"})
        else:
            # Fallback mappings as requested in Step 4
            mappings = {
                "predicted_risk": "risk_score",
                "global_risk": "risk_score",
                "climate_risk_score": "risk_score",
                "risk_index": "risk_score",
                "migration_risk": "risk_score",
                "Trade_Risk": "risk_score"
            }
            for old_col, new_col in mappings.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
                    break
        
        # Ensure year and month are lowercase as requested
        for col in df.columns:
            if col.lower() == 'year':
                df = df.rename(columns={col: "year"})
            if col.lower() == 'month':
                df = df.rename(columns={col: "month"})
        
        # Select relevant columns
        keep_cols = ["year", "risk_score"]
        if "month" in df.columns:
            keep_cols.append("month")
            
        # Drop duplicates and sort
        df = df[keep_cols].drop_duplicates()
        
        sort_cols = ["year"]
        if "month" in df.columns:
            sort_cols.append("month")
        
        df = df.sort_values(sort_cols)
        
        # Convert to records
        data = df.to_dict(orient="records")
        
        return {
            "success": True,
            "sector": sector,
            "data": data
        }
        
    except Exception as e:
        return {
            "success": False,
            "sector": sector,
            "data": [],
            "error": str(e)
        }
