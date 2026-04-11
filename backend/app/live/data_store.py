"""
💾 Historical Data Store (SQLite)

Stores live data to build time-series history for:
- Lag features (t-1, t-2, t-3)
- Rolling statistics (3-month mean, std)
- Trend calculations

Database: backend/live_data.db
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, List


# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "live_data.db")


def init_db():
    """Initialize SQLite database with sector_data table."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sector_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        sector TEXT NOT NULL,
        data TEXT NOT NULL
    )
    """)
    
    # Create index for faster queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_sector_timestamp 
    ON sector_data(sector, timestamp)
    """)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Database initialized: {DB_PATH}")


def save_live_data(data_dict: Dict[str, Dict]):
    """
    Save current live data to database.
    
    Args:
        data_dict: {sector: {metric: value, ...}}
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    timestamp = datetime.utcnow().isoformat()
    
    for sector, values in data_dict.items():
        try:
            cursor.execute("""
            INSERT INTO sector_data (timestamp, sector, data)
            VALUES (?, ?, ?)
            """, (timestamp, sector, json.dumps(values)))
        except Exception as e:
            print(f"⚠️  Error saving {sector} data: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Saved live data for {len(data_dict)} sectors")


def load_recent_data(sector: str, limit: int = 10) -> List[Dict]:
    """
    Load recent historical data for a sector.
    
    Args:
        sector: Sector name (e.g., "economy", "climate")
        limit: Number of recent records to load
        
    Returns:
        List of data dictionaries (chronological order: oldest first)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT data FROM sector_data
    WHERE sector=?
    ORDER BY timestamp DESC
    LIMIT ?
    """, (sector, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    # Reverse to get chronological order (oldest first)
    return [json.loads(row[0]) for row in rows][::-1]


def get_history_count(sector: str) -> int:
    """
    Get count of historical records for a sector.
    
    Args:
        sector: Sector name
        
    Returns:
        Number of records
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT COUNT(*) FROM sector_data
    WHERE sector=?
    """, (sector,))
    
    count = cursor.fetchone()[0]
    conn.close()
    
    return count


def clear_old_data(days: int = 365):
    """
    Remove data older than specified days.
    
    Args:
        days: Keep data from last N days
    """
    from datetime import timedelta
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    
    cursor.execute("""
    DELETE FROM sector_data
    WHERE timestamp < ?
    """, (cutoff,))
    
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    
    if deleted > 0:
        print(f"🗑️  Cleared {deleted} old records")


def get_database_stats() -> Dict:
    """
    Get database statistics.
    
    Returns:
        Dictionary with stats
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total records
    cursor.execute("SELECT COUNT(*) FROM sector_data")
    total = cursor.fetchone()[0]
    
    # Records per sector
    cursor.execute("""
    SELECT sector, COUNT(*) as count 
    FROM sector_data 
    GROUP BY sector
    ORDER BY count DESC
    """)
    by_sector = dict(cursor.fetchall())
    
    # Date range
    cursor.execute("""
    SELECT MIN(timestamp), MAX(timestamp) 
    FROM sector_data
    """)
    date_range = cursor.fetchone()
    
    conn.close()
    
    return {
        "total_records": total,
        "by_sector": by_sector,
        "earliest": date_range[0],
        "latest": date_range[1]
    }


if __name__ == "__main__":
    # Test the database
    print("\n" + "="*70)
    print("💾 TESTING DATA STORE")
    print("="*70)
    
    # Initialize
    init_db()
    
    # Save test data
    test_data = {
        "economy": {
            "nifty": 22000,
            "vix": 15.0,
            "inflation": 5.0
        },
        "climate": {
            "rainfall": 50.0,
            "temperature": 30.0
        }
    }
    
    save_live_data(test_data)
    
    # Load history
    history = load_recent_data("economy", limit=5)
    print(f"\n📊 Economy history: {len(history)} records")
    
    # Stats
    stats = get_database_stats()
    print(f"\n📈 Database Stats:")
    print(f"   Total records: {stats['total_records']}")
    print(f"   By sector: {stats['by_sector']}")
    
    print("\n✅ Data store test complete!")
