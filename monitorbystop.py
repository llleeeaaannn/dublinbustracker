import csv
import datetime
import time
import json
import urllib.request
import os
from typing import Dict, Any

def get_live_data(stop_id: str, max_retries: int = 3):
    """
    Fetches real-time bus data from the local GTFS server for a specific stop.
    """
    url = f"http://127.0.0.1:6824/live.json?stop={stop_id}"

    for attempt in range(max_retries):
        try:
            response = urllib.request.urlopen(url)
            return json.loads(response.read())
        except (ConnectionResetError, urllib.error.URLError) as e:
            if attempt == max_retries - 1:
                print(f"Failed to get data after {max_retries} attempts: {e}")
                raise
            else:
                wait_time = (attempt + 1) * 30
                print(f"Connection error (attempt {attempt + 1}/{max_retries}). Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

def get_time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'

def is_peak_hour(hour: int, day_of_week: int) -> bool:
    is_morning_peak = (7 <= hour < 9)
    is_evening_peak = (16 <= hour < 18)
    is_weekday = day_of_week < 5
    return (is_morning_peak or is_evening_peak) and is_weekday

def monitor_bus(stop_id: str):
    """
    Main monitoring function that tracks all buses at a specific stop.
    """
    data_dir = "monitoring_data"
    os.makedirs(data_dir, exist_ok=True)

    filename = os.path.join(data_dir, f"bus_monitoring_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'trip_id',
            'route',
            'headsign',
            'direction',
            'first_seen_at',
            'initial_due_in_seconds',
            'arrival_time',
            'actual_duration_seconds',
            'prediction_difference_seconds',
            'prediction_difference_minutes',
            'absolute_difference_seconds',
            'percentage_difference',
            'day_of_week',
            'is_weekend',
            'time_of_day',
            'peak_hours',
            'tracking_duration_seconds',
            'last_seen_due_seconds'
        ])

    tracked_buses: Dict[str, Dict[str, Any]] = {}

    print(f"Starting monitoring of all buses at stop {stop_id}")
    print(f"Writing data to {filename}")
    print(f"Will start tracking buses when they are 10 minutes or less from arrival")

    while True:
        try:
            current_time = datetime.datetime.now()
            data = get_live_data(stop_id)

            current_trip_ids = set()

            for bus in data['live']:
                trip_id = bus['trip_id']
                due_in_minutes = bus['dueInSeconds'] / 60
                current_trip_ids.add(trip_id)

                if trip_id in tracked_buses:
                    tracked_buses[trip_id]['last_seen_due_seconds'] = bus['dueInSeconds']

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

            disappeared_buses = set(tracked_buses.keys()) - current_trip_ids
            for trip_id in disappeared_buses:
                bus_data = tracked_buses[trip_id]

                actual_duration = (current_time - bus_data['first_seen_at']).total_seconds()
                prediction_difference = actual_duration - bus_data['initial_due_in_seconds']

                day_of_week = bus_data['first_seen_at'].weekday()
                hour = bus_data['first_seen_at'].hour

                with open(filename, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        trip_id,
                        bus_data['route'],
                        bus_data['headsign'],
                        bus_data['direction'],
                        bus_data['first_seen_at'].strftime('%Y-%m-%d %H:%M:%S'),
                        bus_data['initial_due_in_seconds'],
                        current_time.strftime('%Y-%m-%d %H:%M:%S'),
                        actual_duration,
                        prediction_difference,
                        prediction_difference / 60,
                        abs(prediction_difference),
                        (prediction_difference / bus_data['initial_due_in_seconds']) * 100,
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][day_of_week],
                        day_of_week >= 5,
                        get_time_of_day(hour),
                        is_peak_hour(hour, day_of_week),
                        actual_duration,
                        bus_data['last_seen_due_seconds']
                    ])

                print(f"Bus completed: Route {bus_data['route']}, Trip {trip_id}")
                print(f"Prediction difference: {round(prediction_difference/60, 2)} minutes")
                del tracked_buses[trip_id]

            time.sleep(20)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(20)

if __name__ == "__main__":
    STOP_ID = "8220DB000017"  # This is Drumcondra Rail Station's stop
    monitor_bus(STOP_ID)
