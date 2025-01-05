import csv
import datetime
import time
import json
import urllib.request

def get_live_data(stop_id):
    """
    Fetches real-time bus data from the local GTFS server for a specific stop.

    Args:
        stop_id (str): The ID of the bus stop to monitor (e.g., "8220DB000017")

    Returns:
        dict: JSON response containing real-time bus data including:
            - current_timestamp
            - live: list of buses with their trip_id, route, due time, etc.

    Raises:
        URLError: If connection to local GTFS server fails
        JSONDecodeError: If response isn't valid JSON
    """
    url = f"http://127.0.0.1:6824/live.json?stop={stop_id}"
    response = urllib.request.urlopen(url)
    return json.loads(response.read())

def monitor_bus(stop_id, route_number):
    """
    Main monitoring function that tracks buses of a specific route at a specific stop.
    The function runs indefinitely, checking every 10 seconds for updates.

    Key Features:
    - Only starts tracking buses when they are 10 minutes or less from arrival
    - Creates a CSV file with timestamp in filename for data logging
    - Tracks when each bus is first seen and when it disappears (assumed arrived)
    - Calculates actual journey duration vs predicted duration
    - Handles connection errors gracefully

    Data Collection Process:
    1. Every 10 seconds, fetches current live data
    2. For new buses within 10 minutes: records first appearance and predicted arrival
    3. For disappeared buses: records actual arrival time
    4. Writes completed journeys to CSV file

    Args:
        stop_id (str): The ID of the bus stop to monitor
        route_number (str): The route number to track (e.g., "41")
    """

    # Generate unique filename with timestamp
    filename = f"bus_monitoring_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Initialize CSV file with headers
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'trip_id',          # Unique identifier for specific bus journey
            'route',            # Route number
            'first_seen_at',    # Timestamp when bus first appeared in feed within 10 min threshold
            'initial_due_in_seconds',  # Original predicted arrival time in seconds
            'arrival_time',     # When bus disappeared from feed (assumed arrived)
            'actual_duration_seconds'  # Actual time taken from first seen to arrival
        ])

    # Dictionary to keep track of buses we're currently monitoring
    # Structure: {trip_id: {first_seen_at: datetime, initial_due_in_seconds: float, route: str}}
    tracked_buses = {}

    print(f"Starting monitoring of route {route_number} at stop {stop_id}")
    print(f"Writing data to {filename}")
    print(f"Will start tracking buses when they are 10 minutes or less from arrival")

    # Main monitoring loop
    while True:
        try:
            current_time = datetime.datetime.now()
            data = get_live_data(stop_id)

            # Keep track of which buses are currently in the feed
            # Used later to determine which buses have disappeared (arrived)
            current_trip_ids = set()

            # Process each bus in the current live data
            for bus in data['live']:
                if bus['route'] == route_number:
                    trip_id = bus['trip_id']
                    current_trip_ids.add(trip_id)

                    # Only start tracking when bus is 10 minutes or less away
                    if trip_id not in tracked_buses and bus['dueInSeconds'] <= 600:  # 600 seconds = 10 minutes
                        tracked_buses[trip_id] = {
                            'first_seen_at': current_time,
                            'initial_due_in_seconds': bus['dueInSeconds'],
                            'route': bus['route']
                        }
                        print(f"New bus detected: Route {bus['route']}, Trip {trip_id}, Due in {round(bus['dueInSeconds']/60, 2)} minutes")

            # Find buses that were being tracked but are no longer in the feed
            # These buses are assumed to have arrived at the stop
            disappeared_buses = set(tracked_buses.keys()) - current_trip_ids

            # Process each disappeared (arrived) bus
            for trip_id in disappeared_buses:
                bus_data = tracked_buses[trip_id]

                # Calculate actual duration and write journey data to CSV
                with open(filename, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        trip_id,  # Unique journey identifier
                        bus_data['route'],  # Route number
                        bus_data['first_seen_at'].strftime('%Y-%m-%d %H:%M:%S'),  # When first detected
                        bus_data['initial_due_in_seconds'],  # Initial prediction
                        current_time.strftime('%Y-%m-%d %H:%M:%S'),  # Arrival time
                        (current_time - bus_data['first_seen_at']).total_seconds()  # Actual duration
                    ])

                print(f"Bus completed: Route {bus_data['route']}, Trip {trip_id}")
                # Remove the arrived bus from our tracking dictionary
                del tracked_buses[trip_id]

            # Wait 10 seconds before next check
            # This helps prevent overwhelming the API and provides reasonable update frequency
            time.sleep(10)

        except Exception as e:
            # Handle any errors (network issues, API problems, etc.)
            print(f"Error: {e}")
            # Wait before retrying to prevent rapid error loops
            time.sleep(20)

if __name__ == "__main__":
    # Configuration constants
    STOP_ID = "8220DB000017"  # ID of the bus stop to monitor
    ROUTE_NUMBER = "41"        # Bus route to track

    # Start the monitoring process
    monitor_bus(STOP_ID, ROUTE_NUMBER)
