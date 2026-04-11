"""
📢 Social Data Fetcher (NewsAPI Integration)

Fetches live social unrest and protest data from news sources.

Required Features (from model):
- protest_count
- violence_count
- conflict_events
- fatalities

APIs:
- NewsAPI (FREE tier: 100 requests/day)
  → News sentiment analysis
  → Social instability signals

Status: ✅ IMPLEMENTED (NewsAPI integration active)
"""

import requests
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from backend.app.core.config import NEWS_API_KEY


def fetch_social() -> dict:
    """
    Fetch live social unrest data from NewsAPI.
    
    Uses news headlines to detect social instability signals:
    - Protest/riot keywords
    - Violence/conflict indicators
    - Strike/unrest mentions
    
    Returns:
        Dictionary with social indicators
    """
    try:
        # Check if API key is configured
        if not NEWS_API_KEY:
            print("⚠️  NEWS_API_KEY not configured in .env")
            return {
                "news_count": 0,
                "negative_news_ratio": 0,
                "protest_indicators": 0,
                "violence_indicators": 0,
                "timestamp": None
            }
        
        # Fetch news - use 'everything' endpoint for better global coverage
        # Free tier has limited India coverage, so we use keyword search
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "India OR Asia",
            "language": "en",
            "pageSize": 50,
            "sortBy": "publishedAt",
            "apiKey": NEWS_API_KEY
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # If everything endpoint fails, fallback to US headlines
        if data.get("status") != "ok" or not data.get("articles"):
            print("⚠️  Global news fetch failed, trying US headlines...")
            url = "https://newsapi.org/v2/top-headlines"
            params = {
                "country": "us",
                "pageSize": 50,
                "apiKey": NEWS_API_KEY
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        
        articles = data.get("articles", [])
        headline_count = len(articles)
        
        # Keyword-based sentiment scoring
        negative_words = [
            "protest", "violence", "riot", "conflict", "attack", 
            "strike", "unrest", "clash", "demonstration", "chaos"
        ]
        
        protest_words = [
            "protest", "strike", "demonstration", "march", "rally",
            "boycott", "sit-in", "walkout"
        ]
        
        violence_words = [
            "violence", "riot", "attack", "clash", "killed",
            "injured", "fatalities", "bloodshed", "assault"
        ]
        
        negative_count = 0
        protest_count = 0
        violence_count = 0
        
        for article in articles:
            title = article.get("title") or ""
            description = article.get("description") or ""
            title = title.lower()
            description = description.lower()
            text = f"{title} {description}"
            
            # Count negative news
            if any(word in text for word in negative_words):
                negative_count += 1
            
            # Count protest indicators
            if any(word in text for word in protest_words):
                protest_count += 1
            
            # Count violence indicators
            if any(word in text for word in violence_words):
                violence_count += 1
        
        # Calculate ratios
        negative_ratio = negative_count / headline_count if headline_count > 0 else 0
        protest_ratio = protest_count / headline_count if headline_count > 0 else 0
        violence_ratio = violence_count / headline_count if headline_count > 0 else 0
        
        result = {
            "news_count": headline_count,
            "negative_news_ratio": negative_ratio,
            "protest_indicators": protest_count,
            "violence_indicators": violence_count,
            "protest_ratio": protest_ratio,
            "violence_ratio": violence_ratio,
            "timestamp": data.get("publishedAt", None)
        }
        
        print(f"✅ Social news fetched: {headline_count} articles")
        print(f"   Negative: {negative_count} ({negative_ratio:.2%})")
        print(f"   Protest indicators: {protest_count}")
        print(f"   Violence indicators: {violence_count}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching social news data: {e}")
        return {
            "news_count": 0,
            "negative_news_ratio": 0,
            "protest_indicators": 0,
            "violence_indicators": 0,
            "protest_ratio": 0,
            "violence_ratio": 0,
            "timestamp": None,
            "error": str(e)
        }
    except Exception as e:
        print(f"❌ Unexpected error in social fetcher: {e}")
        return {
            "news_count": 0,
            "negative_news_ratio": 0,
            "protest_indicators": 0,
            "violence_indicators": 0,
            "protest_ratio": 0,
            "violence_ratio": 0,
            "timestamp": None,
            "error": str(e)
        }
