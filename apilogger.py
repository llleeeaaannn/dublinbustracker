import json
import os
from datetime import datetime

class ApiLogger:
    def __init__(self, base_dir="api_logs"):
        """
        Initialize the API logger.
        This creates one file for each monitoring session.
        """
        self.base_dir = base_dir
        self._setup_directory()
        self.filename = f"api_responses_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        self.filepath = os.path.join(self.base_dir, self.filename)
        self._initialize_file()

    def _setup_directory(self):
        """Create the log directory if it doesn't exist."""
        os.makedirs(self.base_dir, exist_ok=True)

    def _initialize_file(self):
        """Initialize the JSON file with an empty list."""
        with open(self.filepath, 'w') as f:
            json.dump([], f)

    def log_response(self, response_data, stop_id=None):
        """
        Append API response to the session's JSON file.
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "stop_id": stop_id,
            "response": response_data
        }

        # Read existing data
        with open(self.filepath, 'r') as f:
            data = json.load(f)

        # Append new entry
        data.append(log_entry)

        # Write back to file
        with open(self.filepath, 'w') as f:
            json.dump(data, f, indent=2)
