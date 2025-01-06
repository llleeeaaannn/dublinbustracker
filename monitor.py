import csv
import datetime
import time
import json
import urllib.request
import os
from typing import Dict, Any, List, Optional

def get_live_data(stop_ids: Optional[List[str]] = None, max_retries: int = 3):
    """
    Fetches real-time bus data from the local GTFS server.

    Args:
        stop_ids (list, optional): List of stop IDs to monitor. If None, shows all stops.
        max_retries (int): Maximum number of retry attempts

    Returns:
        dict: JSON response containing real-time bus data
    """
    base_url = "http://127.0.0.1:6824/live.json"

    if stop_ids:
        # Construct URL with multiple stops if provided
        stop_params = "&".join(f"stop={stop}" for stop in stop_ids)
        url = f"{base_url}?{stop_params}"
    else:
        # Use base URL for all stops
        url = base_url

    for attempt in range(max_retries):
        try:
            response = urllib.request.urlopen(url)
            return json.loads(response.read())
        except (ConnectionResetError, urllib.error.URLError) as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"Failed to get data after {max_retries} attempts: {e}")
                raise
            else:
                wait_time = (attempt + 1) * 30  # Exponential backoff: 30s, 60s, 90s
                print(f"Connection error (attempt {attempt + 1}/{max_retries}). Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

def get_time_of_day(hour: int) -> str:
    """
    Determines the time of day based on hour.

    Args:
        hour (int): Hour in 24-hour format (0-23)

    Returns:
        str: 'Morning', 'Afternoon', 'Evening', or 'Night'
    """
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'

def is_peak_hour(hour: int, day_of_week: int) -> bool:
    """
    Determines if the current time is during peak hours on a weekday.

    Args:
        hour (int): Hour in 24-hour format (0-23)
        day_of_week (int): Day of week (0 = Monday, 6 = Sunday)

    Returns:
        bool: True if current time is during peak hours
    """
    is_morning_peak = (7 <= hour < 9)
    is_evening_peak = (16 <= hour < 18)
    is_weekday = day_of_week < 5
    return (is_morning_peak or is_evening_peak) and is_weekday

def monitor_bus(stop_ids: Optional[List[str]] = None, route_numbers: Optional[List[str]] = None):
    """
    Main monitoring function that tracks buses.

    Args:
        stop_ids (list, optional): List of stop IDs to monitor. If None, monitors all stops.
        route_numbers (list, optional): List of route numbers to track. If None, tracks all routes.
    """
    # Create monitoring_data directory if it doesn't exist
    data_dir = "monitoring_data"
    os.makedirs(data_dir, exist_ok=True)

    # Generate unique filename with timestamp
    filename = os.path.join(data_dir, f"bus_monitoring_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")

    # Initialize CSV file with headers
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'trip_id',                      # Unique identifier for specific bus journey
            'route',                        # Route number
            'stop_id',                      # ID of the bus stop
            'headsign',                     # Final destination of the bus
            'direction',                    # Direction of travel
            'first_seen_at',                # When bus first appeared in feed
            'initial_due_in_seconds',       # Original predicted arrival time
            'arrival_time',                 # When bus disappeared from feed
            'actual_duration_seconds',      # How long it actually took
            'prediction_difference_seconds', # Difference (positive = late, negative = early)
            'prediction_difference_minutes', # Same as above but in minutes
            'absolute_difference_seconds',   # Absolute difference (always positive)
            'percentage_difference',         # How far off as a percentage
            'day_of_week',                  # Monday, Tuesday, etc.
            'is_weekend',                   # True/False
            'time_of_day',                  # Morning/Afternoon/Evening/Night
            'peak_hours',                   # True/False (rush hour)
            'tracking_duration_seconds',     # How long we tracked this bus
            'last_seen_due_seconds'         # Final prediction before disappearing
        ])

    # Dictionary to keep track of buses we're currently monitoring
    tracked_buses: Dict[str, Dict[str, Any]] = {}

    stops_desc = "all stops" if stop_ids is None else f"stops {', '.join(stop_ids)}"
    routes_desc = "all routes" if route_numbers is None else f"routes {', '.join(route_numbers)}"
    print(f"Starting monitoring of {routes_desc} at {stops_desc}")
    print(f"Writing data to {filename}")
    print(f"Will start tracking buses when they are 10 minutes or less from arrival")

    while True:
        try:
            current_time = datetime.datetime.now()
            data = get_live_data(stop_ids)

            current_trip_ids = set()

            # Process each bus in the current live data
            for bus in data['live']:
                # Check if we should track this route
                if route_numbers is None or bus['route'] in route_numbers:
                    trip_id = bus['trip_id']
                    due_in_minutes = bus['dueInSeconds'] / 60
                    current_trip_ids.add(trip_id)

                    # Update last_seen_due_seconds for tracked buses
                    if trip_id in tracked_buses:
                        tracked_buses[trip_id]['last_seen_due_seconds'] = bus['dueInSeconds']

                    # Only start tracking when bus is 10 minutes or less away
                    if trip_id not in tracked_buses and due_in_minutes <= 10:
                        tracked_buses[trip_id] = {
                            'first_seen_at': current_time,
                            'initial_due_in_seconds': bus['dueInSeconds'],
                            'route': bus['route'],
                            'headsign': bus['headsign'],
                            'direction': bus['direction'],
                            'last_seen_due_seconds': bus['dueInSeconds']
                        }
                        print(f"New bus detected: Route {bus['route']}, Trip {trip_id}, Due in {round(due_in_minutes, 2)} minutes")

            # Process disappeared (arrived) buses
            disappeared_buses = set(tracked_buses.keys()) - current_trip_ids
            for trip_id in disappeared_buses:
                bus_data = tracked_buses[trip_id]

                # Calculate various timing metrics
                actual_duration = (current_time - bus_data['first_seen_at']).total_seconds()
                prediction_difference = actual_duration - bus_data['initial_due_in_seconds']

                # Get time-based contextual information
                day_of_week = bus_data['first_seen_at'].weekday()
                hour = bus_data['first_seen_at'].hour

                with open(filename, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        trip_id,
                        bus_data['route'],
                        stop_id,
                        bus_data['headsign'],
                        bus_data['direction'],
                        bus_data['first_seen_at'].strftime('%Y-%m-%d %H:%M:%S'),
                        bus_data['initial_due_in_seconds'],
                        current_time.strftime('%Y-%m-%d %H:%M:%S'),
                        actual_duration,
                        prediction_difference,
                        prediction_difference / 60,  # Convert to minutes
                        abs(prediction_difference),  # Absolute difference
                        (prediction_difference / bus_data['initial_due_in_seconds']) * 100,  # Percentage
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][day_of_week],
                        day_of_week >= 5,  # is_weekend
                        get_time_of_day(hour),
                        is_peak_hour(hour, day_of_week),
                        actual_duration,  # tracking_duration_seconds
                        bus_data['last_seen_due_seconds']
                    ])

                print(f"Bus completed: Route {bus_data['route']}, Trip {trip_id}")
                print(f"Prediction difference: {round(prediction_difference/60, 2)} minutes")
                del tracked_buses[trip_id]

            # Wait 20 seconds before next check
            time.sleep(20)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(20)

if __name__ == "__main__":
    # Example configurations:

    # Monitor specific stop and route
    monitor_bus(stop_ids=["8220DB000017"], route_numbers=["41"])

    # Monitor all stops and routes
    # monitor_bus()

    # Monitor specific stops, all routes
    # monitor_bus(stop_ids=["8220DB000017", "8220DB000018"])

    # Monitor all stops, specific routes
    # monitor_bus(route_numbers=["41", "42"])
