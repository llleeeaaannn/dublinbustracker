import csv
import datetime
import time
import json
import urllib.request
import os
from typing import Dict, Any
from apilogger import ApiLogger
from database import setup_database, save_to_database

# Tries to get bus data from the API
# If successful, returns the data and logs it
# If it fails we simply return None
def get_live_data(stop_id: str, logger=None):
    """
    Fetches real-time bus data from the local GTFS server for a specific stop.
    """
    url = f"http://127.0.0.1:6824/live.json?stop={stop_id}"

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        if logger:
            logger.log_response(data, stop_id)
        return data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def get_time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'

# Function to check if current time is a peak time (Mon-Fri 7-9am or 4-6pm)
def is_peak_hour(hour: int, day_of_week: int) -> bool:
    is_morning_peak = (7 <= hour < 9)
    is_evening_peak = (16 <= hour < 18)
    is_weekday = day_of_week < 5
    return (is_morning_peak or is_evening_peak) and is_weekday

# Main monitoring function that tracks all buses at a specific stop.
def monitor_bus(stop_id: str):

    # Set up SQLite database
    setup_database()

    # Logging Data to JSON file
    logger = ApiLogger()
    print(f"Logging API responses to {logger.filepath}")

    # Create dictionary with key:value types of string:dict
    tracked_buses: Dict[str, Dict[str, Any]] = {}

    print(f"Starting monitoring of all buses at stop {stop_id}")
    print(f"Writing data to {filename}")
    print(f"Will start tracking buses when they are 10 minutes or less from arrival")

    while True:
        try:
            # Get current time
            current_time = datetime.datetime.now()

            # Call get live data and pass the stop to get the data and assign it to 'data' for alongside the logger
            data = get_live_data(stop_id, logger=logger)

            # Create empty set
            current_trip_ids = set()

            for bus in data['live']:
                trip_id = bus['trip_id']
                due_in_minutes = bus['dueInSeconds'] / 60
                current_trip_ids.add(trip_id)

                if trip_id in tracked_buses:
                    tracked_buses[trip_id]['last_seen_at'] = current_time
                    tracked_buses[trip_id]['last_seen_due_seconds'] = bus['dueInSeconds']

                if trip_id not in tracked_buses and due_in_minutes <= 10:
                    tracked_buses[trip_id] = {
                        'first_seen_at': current_time,
                        'last_seen_at': current_time,
                        'initial_due_in_seconds': bus['dueInSeconds'],
                        'route': bus['route'],
                        'headsign': bus['headsign'],
                        'direction': bus['direction'],
                        'last_seen_due_seconds': bus['dueInSeconds']
                    }
                    print(f"New bus detected: Route {bus['route']}, Trip {trip_id}, Due in {round(due_in_minutes, 2)} minutes")

            disappeared_buses = set(tracked_buses.keys()) - current_trip_ids

            for trip_id in disappeared_buses:

                # Check when we last saw this bus
                bus_last_seen = tracked_buses[trip_id]['last_seen_at']

                # Calculate how many seconds its been since we last saw said bus
                seconds_since_last_seen = (current_time - bus_last_seen).total_seconds()

                # If statement to only mark bus as arrived if it hasnt been seen in over 300 secs (to prevent busses that temporarily disappeared from tracking being marked as arrived)
                if seconds_since_last_seen > 300:

                    bus_data = tracked_buses[trip_id]

                    # Calculate derived values
                    actual_duration = (bus_last_seen - bus_data['first_seen_at']).total_seconds()
                    prediction_difference = actual_duration - bus_data['initial_due_in_seconds']
                    day_of_week = bus_data['first_seen_at'].weekday()
                    hour = bus_data['first_seen_at'].hour

                    # Adding derived values to bus_data object
                    bus_data['actual_duration_seconds'] = actual_duration
                    bus_data['prediction_difference_seconds'] = prediction_difference
                    bus_data['prediction_difference_minutes'] = prediction_difference / 60
                    bus_data['day_of_week'] = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][day_of_week]
                    bus_data['is_weekend'] = day_of_week >= 5
                    bus_data['time_of_day'] = get_time_of_day(hour)
                    bus_data['peak_hours'] = is_peak_hour(hour, day_of_week)

                    save_to_database(bus_data)

                    print(f"Bus completed: Route {bus_data['route']}, Trip {trip_id}")
                    print(f"Prediction difference for Route {bus_data['route']}, Trip {trip_id}: {round(prediction_difference/60, 2)} minutes")

                    # Remove bus from tracking
                    del tracked_buses[trip_id]

            time.sleep(20)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(20)

if __name__ == "__main__":
    STOP_ID = "8220DB000017"  # This is Drumcondra Rail Station's stop
    monitor_bus(STOP_ID)
