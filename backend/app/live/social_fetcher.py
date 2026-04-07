"""
📢 Social Data Fetcher

Fetches live social unrest and protest data.

Required Features (from model):
- protest_count
- violence_count
- conflict_events
- fatalities

APIs:
- GDELT Project (FREE via BigQuery)
- ACLED (FREE for research)
- ICEWS (FREE academic)

Status: PLACEHOLDER (No real API calls yet)
"""


def fetch_social() -> dict:
    """
    Fetch live social unrest data.
    
    Returns:
        Dictionary with social indicators (placeholder values)
    """
    # TODO: Implement real API calls
    # 1. GDELT BigQuery → Protest/riot events
    # 2. ACLED → Social conflict events
    # 3. News API → Social sentiment
    
    return {
        "protest_count": None,     # Number of protests
        "violence_count": None,    # Number of violent events
        "conflict_events": None,   # Total conflict events
        "fatalities": None,        # Fatalities from social unrest
        "timestamp": None          # fetch timestamp
    }
