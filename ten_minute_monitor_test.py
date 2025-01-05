import csv
import datetime
import time
import json
import urllib.request
import os
from typing import Dict, Any

def get_live_data(stop_id):
    """
    Fetches real-time bus data from the local GTFS server for a specific stop.

    Args:
        stop_id (str): The ID of the bus stop to monitor (e.g., "8220DB000017")

    Returns:
        dict: JSON response containing real-time bus data
    """
    url = f"http://127.0.0.1:6824/live.json?stop={stop_id}"
    response = urllib.request.urlopen(url)
    return json.loads(response.read())

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

def monitor_bus(stop_id: str, route_number: str):
    """
    Main monitoring function that tracks buses of a specific route at a specific stop.
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

    print(f"Starting monitoring of route {route_number} at stop {stop_id}")
    print(f"Writing data to {filename}")
    print(f"Will start tracking buses when they are 10 minutes or less from arrival")

    while True:
        try:
            current_time = datetime.datetime.now()
            data = get_live_data(stop_id)

            current_trip_ids = set()

            # Process each bus in the current live data
            for bus in data['live']:
                if bus['route'] == route_number:
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

            time.sleep(10)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    STOP_ID = "8220DB000017"
    ROUTE_NUMBER = "41"

    monitor_bus(STOP_ID, ROUTE_NUMBER)
