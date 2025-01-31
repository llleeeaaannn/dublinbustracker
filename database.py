import sqlite3
from typing import Dict, Any

# Sets up SQLite database
def setup_database(db_name: str = "bus_monitoring.db"):
    """Set up the SQLite database and table."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bus_data (
            trip_id TEXT PRIMARY KEY,
            route TEXT,
            headsign TEXT,
            direction TEXT,
            first_seen_at TEXT,
            initial_due_in_seconds INTEGER,
            arrival_time TEXT,
            actual_duration_seconds INTEGER,
            prediction_difference_seconds INTEGER,
            prediction_difference_minutes REAL,
            day_of_week TEXT,
            is_weekend INTEGER,
            time_of_day TEXT,
            peak_hours INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# Saves data from the bus_data object to the SQLite database
def save_to_database(bus_data: Dict[str, Any], db_name: str = "bus_monitoring.db"):
    """Save bus data to the SQLite database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO bus_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        bus_data['trip_id'],
        bus_data['route'],
        bus_data['headsign'],
        bus_data['direction'],
        bus_data['first_seen_at'].strftime('%Y-%m-%d %H:%M:%S'),
        bus_data['initial_due_in_seconds'],
        bus_data['last_seen_at'].strftime('%Y-%m-%d %H:%M:%S'),
        bus_data['actual_duration_seconds'],
        bus_data['prediction_difference_seconds'],
        bus_data['prediction_difference_minutes'],
        bus_data['day_of_week'],
        bus_data['is_weekend'],
        bus_data['time_of_day'],
        bus_data['peak_hours']
    ))
    conn.commit()
    conn.close()
